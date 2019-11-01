/** 
**  DTSD -- Entry point for the Data Transfer system.
**
**  Usage:
**	dtsd [options]
**
**  Options:
**	-h 		print help
**
** 	-c <cfg>	specify the config file to use
** 	-d		run as a daemon
**	-i 		run interactively
**	-m 		allow multiple DTSD on the same machine
**	-p <port>	specify server port
**      -t		trace
**	-r <path>	specify server root dir
**      -v		verbose
**
**	--copy		enable for CopyMode to $cwd
**	--loPort	set lower transport socket number
**	--hiPort	set upper transport socket number
**
**	-H <host>	specify the host name to simulate
**	-I       	initialize DTSD working directory
**	-P       	set queue pause time
**
**	+d		increment debug level
**	+v		increment verbose level
**
**  @file	dtsd.c
**  @author	Mike Fitzpatrick, NOAO
**  @date	July 2010
*/


#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>

#include "dts.h"
#include "dtsdb.h"
#include "dtsMethods.h"
#include "dtsClient.h"
#include "build.h"


#define  _DTS_SRC_

#define	DTSD_PAUSE	5		/* Pause between retries (sec)	*/
#define	DTSD_COPYLO	3005		/* CopyMode default low port	*/
#define	DTSD_COPYHI	3099		/* CopyMode default high port	*/

#define DTSD_SEMKEY	1214


DTS    *dts 		= (DTS *) NULL;	/* main DTS structure       	*/

int    	debug		= 0;		/* debug flag			*/
int    	trace		= 0;		/* trace flag			*/
int    	verbose		= 0;		/* verbose flag			*/
int    	daemonize	= 0;		/* run as daemon	    	*/
int    	interactive	= 0;		/* run interactively	    	*/
int    	init		= 0;		/* initialize server root    	*/
int    	dts_singleton	= 1;		/* allow only single server	*/

int    	dts_monitor	= -1;		/* DTS monitor handle		*/
int    	normal_exit	= 0;		/* did we exit normally?	*/
int	stat_semid	= 0;		/* status semaphore		*/

int    	copyMode	= 0;		/* CopyMode?			*/
int    	copyLoPort	= DTSD_COPYLO;	/* CopyMode low transfer port	*/
int    	copyHiPort	= DTSD_COPYHI;	/* CopyMode high transfer port	*/
char   *copyDir		= NULL;		/* CopyMode destination dir	*/
char   *copyCwd		= NULL;		/* CopyMode working dir		*/

int 	_queue_pause_time_    = 3;	/* queue sampling rate		*/


int     dts_initServerMethods (DTS *dts);
int     dts_initQueueManagers (DTS *dts);

void    dts_launchQueueManager (void *data);
void    dts_normalQueueManager (DTS *dts, dtsQueue *dtsq);
void    dts_scheduledQueueManager (DTS *dts, dtsQueue *dtsq);
void    dts_priorityQueueManager (DTS *dts, dtsQueue *dtsq);
int     dts_respawnQueueManager (DTS *dts, int index);
int     dts_queueRestart (DTS *dts, dtsQueue *dtsq);
int     dts_queueObjXfer (DTS *dts, dtsQueue *dtsq);
void    dts_threadCleanup (void *flag);
void    dts_statTimer (void *data);
void    dts_startTimerStats (DTS *dts);

void    dts_cmdLoop (DTS *dts);
void    dtsFree (DTS *dts);
void 	dtsOnExit (int stat, void *data); 	/*  Not Used */

time_t  dts_schedTime (dtsQueue *dtsq, time_t last);

static  void dtsd_Usage (void);
static  void dts_initSharedMem (void);

typedef struct {
    char      qname[SZ_FNAME];			/* queue name		*/
    int       threadNum;			/* thread number	*/
    dtsQueue  *dtsq;				/* dtsQueue struct	*/
} qmArg, *qmArgP;


xr_dtsstat mystat = { 0 , 0 };		// FIXME




/**
 *  DTS Daemon Entry point.  
 */
int 
main (int argc, char **argv)
{
    register int i, j, len, port = DTS_PORT;
    char     ch, *larg = NULL, *host = NULL, *root = NULL, *config = NULL;
    key_t    key = DTSD_SEMKEY;
    dtsQueue  *dtsq;				/* dtsQueue struct	*/



    /* Initialize.
    */
    host = dts_getLocalIP ();

    /* Process commandline arguments.
    */
    for (i=1; i < argc; i++) {
        if (argv[i][0] == '-' && !(isdigit(argv[i][1]))) {
            len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                ch = argv[i][j];

                switch (ch) {
		case 'h':   dtsd_Usage ();			    exit (0);
                case 'B':   use_builtin_config = 1; 		    break;

		case 'c':   config = argv[++i];     		    break;
		case 'd':   daemonize++; 	    		    break;
		case 'i':   interactive++; 	    		    break;
		case 'p':   port = atoi (argv[++i]); 		    break;
		case 't':   trace++; 				    break;
		case 'r':   root = argv[++i]; 			    break;
		case 'm':   dts_singleton = 0; 			    break;
		case 'v':
                    if (strcmp (&(argv[i][j+1]), "ersion") == 0) {
                        printf ("Build Version: %s\n", build_date);
                        return (0);
                    } else
                        verbose++;
                    break;

		case 'H':   host = argv[++i]; 	    		    break;
		case 'I':   init++; 				    break;
		case 'P':   _queue_pause_time_ = atoi (argv[++i]);  break;

		case '-':			/* CopyMode args	*/
                    larg = &argv[i][++j];

		    if (strncasecmp (larg, "copyDir", 7) == 0) {
			copyDir = argv[++i];
		    } else if (strncasecmp (larg, "copyPort", 8) == 0) {
			char *ip;

			for (ip=argv[++i]; isdigit(*ip); ip++)
			    ;
			*ip++ = '\0';
			copyLoPort = atoi (argv[i]);
			copyHiPort = atoi (ip);
			copyMode++;
		    } else if (strncasecmp (larg, "copy", 4) == 0) {
			copyMode++;
		    } else if (strncasecmp (larg, "loPort", 6) == 0) {
			copyLoPort = atoi (argv[++i]);
		    } else if (strncasecmp (larg, "hiPort", 6) == 0) {
			copyHiPort = atoi (argv[++i]);
		    } 
		    break;

                default:
                    fprintf (stderr, "Unknown option '%c'\n", ch);
                    break;
                }
                j = len;        /* skip remainder       */
            }

        } else if (argv[i][0] == '+') {
            len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                switch ((ch = argv[i][j])) {
		case 'd':   debug++; 			break;
		case 'v':   verbose++; 			break;
		}
                j = len;        /* skip remainder       */
	    }
	}
    }

    /*  Sanity argument checking.
    */
    if (interactive)
	daemonize = 0;

    /*  Initialize the status semaphores (an array of 3, which is why we can't
     *  use dts_semInit() here).
     */
    if ((stat_semid = semget (key, 3, 0600 | IPC_CREAT)) == -1) {
        printf ("Error in semget: '%s'\n", strerror(errno));
        exit (1);
    }

    /*  These two are for STAT THREAD.
     *  BINARY/MUTEX - initialize sem 0 to 1, and sem 1 to 1.
     */
    if (semctl (stat_semid, 0, SETVAL, 0) == -1) {
        printf ("error in semctl");
        exit (0);
    }
    if (semctl (stat_semid, 1, SETVAL, 0) == -1) {
        printf ("error in semctl");
        exit (0);
    }
    
    xr_semSet (stat_semid);



    /*  Setup for CopyMode.
     */
    if (copyMode) {
	if (!copyDir)
	    copyDir = strdup ( dts_localCwd() );  /* Note: lost memory	*/
	copyCwd = copyDir;
	if (debug)
            fprintf (stderr, "copyMode: dir='%s' lo=%d hi=%d\n", 
	        copyDir, copyLoPort, copyHiPort);
        dts = dtsInit ("copyMode", port, host, root, init);

    } else {
        /*  Initialize.  Passing in the port/host/svc let's us override the
        **  local setting or else pretend to be another server.
        */
        dts = dtsInit (config, port, host, root, init);
    }


    /* See the if there is a DTSD already running on this machine.
     */
    if (dts_singleton) {
	char  poke_host[SZ_FNAME];

	memset (poke_host, 0, SZ_FNAME);
	sprintf (poke_host, "localhost:%d", port);
        if (dts_hostPoke (poke_host) == OK) {
	    fprintf (stderr, "Error: DTSD is already running.\n");
	    exit (1);
        }
    }

    if (copyMode) {
	dts->copyMode++;
	strcpy (dts->self->clientCopy, copyDir);
	chdir (copyCwd);
    }

    /*  Connect to the logging monitor or else open a local logfile.
    */
    dts_connectToMonitor (dts);

    dts->debug      = debug;
    dts->verbose    = verbose;
    dts->daemonize  = daemonize;

    time (&dts->start_time);		 // Start time used to calculate uptime
    strcpy (dts->build_date,build_date); // Also added for version.

    dtsLog (dts, "Starting DTS server: %s", dts->serverUrl);
    dtsLog (dts, "DTS server root dir: %s", 
	(dts->serverRoot[0] ?  dts->serverRoot : " "));
	
    if (dts->self)
        dtsLog (dts, "DTS server copy dir: %s", 
	    (dts->self->clientCopy[0] ? dts->self->clientCopy : " "));
    else
        dtsLog (dts, "DTS server copy dir:  ");
    dtsLog (dts, "DTS build version: %s", build_date);

    if (dts->verbose > 1 && dts->debug > 2)
	dtsLog (dts, "DTS queues: 0x%lx 0x%lx", dts->queues[0], dts->queues[1]);


    /*  Initialize the DTS working directory.
    */
    if (!copyMode)
        dts_initServerRoot (dts);


    /*  Initialize the DTS shared memory information.
    */
    dts_initSharedMem ();

    /*  Initialize the DTS server methods.
    */
    dts_initServerMethods (dts);

    /*  Initialize the Queue Managers.  These are spawned on separate threads
     *  to manage queues independently.
     */
    if (!getenv ("DTS_NOQUEUE") && !copyMode) {
        dtsLog (dts, "DTS starting queues ....\n");
        dts_initQueueManagers (dts);
    }

    /* Establish signal handlers.
    */
    /*xr_setSigHandler (SIGCHLD, SIG_IGN);*/      /* ignore child            */
    xr_setSigHandler (SIGTSTP, SIG_IGN);          /* ignore tty signals      */
    xr_setSigHandler (SIGTTOU, SIG_IGN);
    xr_setSigHandler (SIGTTIN, SIG_IGN);
    xr_setSigHandler (SIGHUP,  dts_sigHandler);   /* catch hangup signal     */
    xr_setSigHandler (SIGTERM, dts_sigHandler);   /* catch kill signal       */
    xr_setSigHandler (SIGUSR1, dts_sigHandler);   /* clean shutdown request  */
    xr_setSigHandler (SIGUSR2, dts_sigHandler);   /* abort request           */


#ifdef USE_TIMER_STATS
    /*  Initialize the stats heartbeat timer thread.
     */
    dts_startTimerStats (dts);
#endif

    /*  Start the server running.  If interactive, run server on a separate
    **  thread and wait for input, otherwise we end in the server.
    */
    if (!interactive) {
	if (daemonize) {
            dtsLog (dts, "DTS running as daemon\n");
	    dtsDaemonize (dts);
	} else
            dtsLog (dts, "DTS server running\n");
        dtsLog (dts, "======================================================\n");

#ifdef ORIG_SERVER
	xr_startServer ();
#else
        while (xr_startServer () != 1) {
            //  Check queues to see if one needs to respawn.
            for (i=0; i < dts->nqueues; i++) {
		dtsq = dts->queues[i];
                //dtsLog(dts," Var# %d, %s %s\n",i,
                //dtsq->qstat->infile,dtsq->qstat->outfile);
                if (dts_semGetVal (dtsq->activeSem) == QUEUE_RESPAWN) {
		    dts_semSetVal (dtsq->activeSem, QUEUE_RESPAWNING);
                    dts_respawnQueueManager (dts, i);   // call spawn function
                }
            }
	    sleep(2);
        }
        xr_killServer();

#endif

    } else {
        xr_startServerThread ();
	dts_cmdLoop (dts);
    }


    /* Clean up.
    */
    dtsLog (dts, "^bExiting normally^r");
    normal_exit = 1;

    /* FIXME -- Need to wait for any pending asynch calls to complete....
    */
    xr_asyncWait ();
    dtsFree (dts);

    return (0);
}


/******************************************************************************
**  DTS_INITSHAREDMEM -- Initialize the shared memory segment.
*/
static void
dts_initSharedMem (void)
{
    int    i, shmid;
    key_t  key2;
    shmQueueStat *shmQstat, *qs;
    int    nqueues =  max (10, dts->nqueues);
    int    sz_qstat = ( nqueues * sizeof(shmQueueStat) );


    /*  We'll name our shared memory segment "5678".
     */
    key2 = 5678;

    /* Create the segment.     
     */
    if ((shmid = shmget (key2, sz_qstat, IPC_CREAT | 0666)) < 0) {
        perror ("dts_initSharedMem shmget");
//fprintf (stderr, "key2=%d sz=%d flags=%d\n", key2, sz_qstat, IPC_CREAT|0666);
        exit (1);
    }

    //FREE
    // NEED TO CALL THIS WHEN PROCESS CRASHES OR CLOSES.

    // shmctl(shmid, IPC_RMID, 0);
                
    // Use the shared memory to update some of our DTS and DTSQ structs.
    // This is due to the server executed methods being forked.

    if ((shmQstat = (shmQueueStat *) shmat (shmid, NULL, 0)) == (void *) -1) {
	perror ("shmat");
	exit (1);
    }
                                    
    dtsQueue   *dtsq = (dtsQueue *) NULL;       

    memset ((void *)shmQstat, 0, sz_qstat);
    for (i=0; i < max (10, dts->nqueues); i++) {
	qs = &shmQstat[i];
        strcpy(qs->infile, "none\n");
        strcpy(qs->outfile, "none\n");
                                        
        qs->numflushes = 0;
        qs->canceledxfers = 0;
                                        
        dtsq = dts->queues[i];
	if (dtsq)
            dtsq->qstat = qs;
    }
}


/******************************************************************************
**  DTS_CMDLOOP -- Interactive command loop.
*/
void
dts_cmdLoop (DTS *dts)
{
    char   cmd[SZ_CMD], method[SZ_CMD], what[SZ_CMD];
    char   arg1[SZ_CMD], arg2[SZ_CMD];
    char   host[SZ_FNAME], clientUrl[SZ_FNAME];
    char  *sres;

    int    port = DTS_PORT, client = -1, res = 0;


    memset (cmd, 0, SZ_CMD);			/* clear strings	*/
    memset (method, 0, SZ_CMD);
    memset (clientUrl, 0, SZ_FNAME);

    strcpy (host, "localhost");
    strcpy (clientUrl, "http://localhost:3000/RPC2");

    dtsLog (dts, "waiting for command input");

    fprintf (stdout, "dts> "); fflush (stdout);
    while (fgets(cmd, SZ_CMD, stdin)) {

        sscanf (cmd, "%s", method);

	if (cmd[0] == '\n') {
	    ;

	} else if (cmd[0] != '!')
            xr_initParam (client);


	if (cmd[0] == '!') { 				/* shell escape	*/
	    system (&cmd[1]);

	} else if (strcmp (method, "quit") == 0) {	/* quit loop	*/
	    break;

	} else if (strcmp (method, "add") == 0) {	/* add client	*/

	} else if (strcmp (method, "connect") == 0) {	/* connect	*/
connect:
	    xr_closeClient (client);
    	    client = xr_initClient (clientUrl, DTS_NAME, DTS_VERSION);

	} else if (strcmp (method, "host") == 0) {	/* set host	*/
	    memset (host, 0, SZ_FNAME);
            sscanf (cmd, "%s %s", method, host);
    	    sprintf (clientUrl, "http://%s:%d/RPC2", host, port);
	    goto connect;

	} else if (strcmp (method, "port") == 0) {	/* set port	*/
            sscanf (cmd, "%s %s", method, host);
    	    sprintf (clientUrl, "http://%s:%d/RPC2", host, port);
	    goto connect;


	} else if (strcmp (method, "list") == 0) {	/* set host	*/
            sscanf (cmd, "%s %s", method, what);
	    if (what[0] == 'd' || what[0] == 'a')
		dts_printDTS (dts, stdout);
	    if (what[0] == 'q' || what[0] == 'a')
		dts_printAllQueues (dts, stdout);


	/* Provide some basic commands as RPC calls to ourself.
	*/
	} else if (strcmp (method, "cwd") == 0) {
            sres = dts_hostCwd ("localhost");
            printf ("%s\n", (sres ? sres :""));
	    free (sres), sres = NULL;

	} else if (strcmp (method, "rm") == 0) {
	    int recurse = 0;

            sscanf (cmd, "%s %s %s", method, arg1, arg2);
	    recurse = (strncmp (arg1,"-r",2) == 0);

            res = dts_hostDel ("localhost", 
		(arg1[0] == '-' ? arg2 : arg1), "XYZZY", recurse);
            printf ("%s\n", ((res == OK) ? "OK" : "ERR"));

	} else if (strcmp (method, "ls") == 0) {
	    int  lsLong = 0;

            sscanf (cmd, "%s %s %s", method, arg1, arg2);
	    lsLong = (strncmp (arg1,"-l",2) == 0);

            sres = dts_hostDir ("localhost", 
		(arg1[0] == '-' ? arg2 : arg1), lsLong);
            printf ("%s\n", (sres ? sres :""));
	    free (sres), sres = NULL;


	} else if (method[0]) {
	    fprintf (stderr, "Unknown command '%s'\n", method);
	    if (dts->monitor >= 0)
		dtsLog (dts, "Unknown command '%s'", method);
	}

        bzero (cmd, SZ_CMD);
        bzero (method, SZ_CMD);
    
	fprintf (stdout, "dts> "); fflush (stdout);

        if (sres) { free (sres);  sres = NULL;  }
    }

    dtsLog (dts, "command loop terminating");
}



/******************************************************************************
**  DTS_INITSERVERMETHODS -- Initialize the server-side of the DTS.  Create
**  an XML-RPC server instance, define the commands and start the thread.
*/
int
dts_initServerMethods (DTS *dts)
{
    if (dts->trace)
	fprintf (stderr, "Initializing Server Methods:\n");

    /*  Create the server and launch.  Note we never return from this
    **  and make no calls to other servers.
    */
    xr_createServer ("/RPC2", dts->serverPort, NULL);


    /*  Define the server methods.
    */
		/****  DTS Management Methods  ****/
    xr_addServerMethod ("init",            dts_initDTS,          NULL);
    xr_addServerMethod ("shutdown",        dts_shutdownDTS,      NULL);
    xr_addServerMethod ("abort",           dts_shutdownDTS,      NULL);
    xr_addServerMethod ("cfg",             dts_Cfg,              NULL);
    xr_addServerMethod ("nodeStat",        dts_nodeStat,         NULL);


		/****  Queue Methods  ****/
    xr_addServerMethod ("startQueue",      dts_startQueue,       NULL);
    xr_addServerMethod ("pauseQueue",      dts_pauseQueue,       NULL);
    xr_addServerMethod ("pokeQueue",       dts_pokeQueue,        NULL);
    xr_addServerMethod ("stopQueue",       dts_stopQueue,        NULL);
    xr_addServerMethod ("flushQueue",      dts_flushQueue,       NULL);
    xr_addServerMethod ("restartQueue",    dts_restartQueue,     NULL);
    xr_addServerMethod ("addToQueue",      dts_addToQueue,       NULL);
    xr_addServerMethod ("removeFromQueue", dts_removeFromQueue,  NULL);
    xr_addServerMethod ("printQueueCfg",   dts_printQueueCfg,    NULL);
    xr_addServerMethod ("submitLogs",      dts_submitLogs,       NULL);
    xr_addServerMethod ("getQLog",         dts_getQLog,          NULL);
    xr_addServerMethod ("eraseQLog",       dts_eraseQLog,        NULL);


		/****  Utility Methods  ****/
    xr_addServerMethod ("monitor",         dts_monAttach,        NULL);
    xr_addServerMethod ("console",         dts_monConsole,       NULL);
    xr_addServerMethod ("detach",          dts_monDetach,        NULL);

    xr_addServerMethod ("dtsList",         dts_List,             NULL);
    xr_addServerMethod ("dtsSet",          dts_Set,              NULL);
    xr_addServerMethod ("dtsGet",          dts_Get,              NULL);


		/****  File Utility Methods  ****/
    xr_addServerMethod ("access",          dts_Access,           NULL);
    xr_addServerMethod ("cat",             dts_Cat,              NULL);
    xr_addServerMethod ("copy",            dts_Copy,             NULL);
    xr_addServerMethod ("checksum",        dts_Checksum,         NULL);
    xr_addServerMethod ("chmod",           dts_Chmod,            NULL);
    xr_addServerMethod ("cwd",             dts_Cwd,              NULL);
    xr_addServerMethod ("del",             dts_Delete,           NULL);
    xr_addServerMethod ("dir",             dts_Dir,              NULL);
    xr_addServerMethod ("ddir",            dts_DestDir,          NULL);
    xr_addServerMethod ("isDir",           dts_CheckDir,         NULL);
    xr_addServerMethod ("diskFree",        dts_DiskFree,         NULL);
    xr_addServerMethod ("diskUsed",        dts_DiskUsed,         NULL);
    xr_addServerMethod ("echo",            dts_Echo,             NULL);
    xr_addServerMethod ("fsize",           dts_FSize,            NULL);
    xr_addServerMethod ("fmode",           dts_FMode,            NULL);
    xr_addServerMethod ("ftime",           dts_FTime,            NULL);
    xr_addServerMethod ("mkdir",           dts_Mkdir,            NULL);
    xr_addServerMethod ("ping",            dts_Ping,             NULL);
    xr_addServerMethod ("pingstr",         dts_PingStr,          NULL);
    xr_addServerMethod ("pingarray",       dts_PingArray,        NULL);
    xr_addServerMethod ("pingsleep",       dts_PingSleep,        NULL);
    xr_addServerMethod ("remotePing",      dts_remotePing,       NULL);
    xr_addServerMethod ("rename",          dts_Rename,           NULL);
    xr_addServerMethod ("setRoot",         dts_SetRoot,          NULL);
    xr_addServerMethod ("setDbg",          dts_SetDbg,           NULL);
    xr_addServerMethod ("unsetDbg",        dts_UnsetDbg,         NULL);
    xr_addServerMethod ("touch",           dts_Touch,            NULL);

		/****  Low-Level I/O Methods  ****/
    xr_addServerMethod ("read",        	   dts_Read,        	 NULL);
    xr_addServerMethod ("write",           dts_Write,        	 NULL);
    xr_addServerMethod ("prealloc",        dts_Prealloc,         NULL);
    xr_addServerMethod ("stat",            dts_Stat,             NULL);

		/****  Transfer Methods  ****/
    xr_addServerMethod ("xferPushFile",    dts_xferPushFile,     NULL);
    xr_addServerMethod ("xferPullFile",    dts_xferPullFile,     NULL);
    xr_addServerMethod ("receiveFile",     dts_xferReceiveFile,  NULL);
    xr_addServerMethod ("sendFile",        dts_xferSendFile,     NULL);

    xr_addServerMethod ("initTransfer",    dts_initTransfer,     NULL);
    xr_addServerMethod ("doTransfer",      dts_doTransfer,       NULL);
    xr_addServerMethod ("endTransfer",     dts_endTransfer,      NULL);
    xr_addServerMethod ("cancelTransfer",  dts_cancelTransfer,   NULL);

		/****  Queue Methods  ****/
    xr_addServerMethod ("queueAccept",     dts_queueAccept,      NULL);
    xr_addServerMethod ("queueComplete",   dts_queueComplete,    NULL);
    xr_addServerMethod ("queueRelease",    dts_queueRelease,     NULL);
    xr_addServerMethod ("queueSetControl", dts_queueSetControl,  NULL);
    xr_addServerMethod ("queueDest",       dts_queueDest,        NULL);
    xr_addServerMethod ("queueSrc",        dts_queueSrc,         NULL);
    xr_addServerMethod ("updateStats",     dts_queueUpdateStats, NULL);

    xr_addServerMethod ("startQueue",      dts_startQueue,       NULL);
    xr_addServerMethod ("stopQueue",       dts_stopQueue,        NULL);
    xr_addServerMethod ("listQueue",       dts_listQueue,        NULL);
    xr_addServerMethod ("getQueueStat",    dts_getQueueStat,     NULL);
    xr_addServerMethod ("setQueueStat",    dts_setQueueStat,     NULL);
    xr_addServerMethod ("getQueueCount",   dts_getQueueCount,    NULL);
    xr_addServerMethod ("setQueueCount",   dts_setQueueCount,    NULL);
    xr_addServerMethod ("getQueueDir",     dts_getQueueDir,      NULL);
    xr_addServerMethod ("setQueueDir",     dts_setQueueDir,      NULL);
    xr_addServerMethod ("getQueueCmd",     dts_getQueueCmd,      NULL);
    xr_addServerMethod ("setQueueCmd",     dts_setQueueCmd,      NULL);
    xr_addServerMethod ("getCopyDir",      dts_getCopyDir,       NULL);
    xr_addServerMethod ("execCmd",         dts_execCmd,          NULL);
    /*
    xr_addServerMethod ("addToQueue",      dts_addToQueue,       NULL);
    xr_addServerMethod ("delFromQueue",    dts_delFromQueue,     NULL);
    */

    xr_addServerMethod ("testFault",       dts_testFault,        NULL);

    return (OK);
}


/******************************************************************************
**  DTS_INITQUEUEMANAGERS -- Initialize the queue managers, one thread for
**  each queue operating on this machine.
*/
int
dts_initQueueManagers (DTS *dts)
{
    register    int i, rc, status = OK;
    dtsQueue   *dtsq = (dtsQueue *) NULL;
    qmArg      *arg[MAX_QUEUES];
    pthread_t   qm_tid[MAX_QUEUES];  		/* QueueMgr thread ids	*/
    pthread_attr_t  qm_attr;


    /* Initialize the service processing thread attribute.
    */
    pthread_attr_init (&qm_attr);
    pthread_attr_setdetachstate (&qm_attr, PTHREAD_CREATE_JOINABLE);

    /* Loop over the config, finding the queues for this machine.
     */
    for (i=0; i < dts->nqueues; i++) {

	/* Set up the thread arguments.
	 */
	dtsq = dts->queues[i];
	arg[i] = (qmArg *) calloc (1, sizeof(qmArg));
	strcpy (arg[i]->qname, dts->queues[i]->name);
	arg[i]->threadNum = i;
	arg[i]->dtsq = dtsq;

        /*  Remove existing semaphores.  It's okay if these fail since the
         *  semaphore may not actually exist.	
         */
        dts_semRemove (dtsq->activeSem);
        dts_semRemove (dtsq->countSem);

        /*  Initialize the queue semaphores.  The 'activeSem' is used to
         *  indicate whether the queue is active for processing.  The
         *  'countSem' is used to indicate how many items are remaining
         *  in the queue to be processed.
         */
        dtsq->activeSem = dts_semInit (((int)getuid()+i), QUEUE_ACTIVE);
        dtsq->countSem = dts_semInit (((int)getuid()+dtsq->port), dts_queueRestart (dts, dtsq));

        if (dtsq->activeSem < 0) {
	    dtsLog (dts, "Cannot initialize '%s' active semaphore", dtsq->name);
	    status = ERR;
        }
        if (dtsq->countSem < 0) {
	    dtsLog (dts, "Cannot initialize '%s' count semaphore, port=%d", 
	        dtsq->name, dtsq->port);
	    status = ERR;
        }
        if (status == ERR)
	    continue;

        if ((rc = pthread_create (&qm_tid[i], &qm_attr, 
		(void *)dts_launchQueueManager, (void *)arg[i]))) {
                    fprintf (stderr,
                        "ERROR: QM pthread_create() fails, code: %d\n", rc);
                    exit (1);
        } else if (dts->verbose) {
	    dtsLog (dts, "Running QMgr[%d]: t='%s' c=%-3d n='%s'\n", 
		i, dts_cfgQTypeStr (dtsq->type), 
		dts_semGetVal (dtsq->countSem), dtsq->name);
        }

	dtsq->qm_tid = qm_tid[i];	/* save the thread ID		*/
    }

    return (OK);
}


/****************************************************************************
**  DTS_RESPAWNQUEUEMANAGER -- Re-spawn the specified queue.
*/
int 
dts_respawnQueueManager (DTS *dts, int index)
{
    int rc, status = OK;
    dtsQueue   *dtsq = (dtsQueue *) NULL;
    qmArg      *arg[MAX_QUEUES];
    pthread_t   qm_tid;                 /* QueueMgr thread ids  */
    pthread_attr_t  qm_attr;

    /* Initialize the service processing thread attribute.
    */
    pthread_attr_init (&qm_attr);
    pthread_attr_setdetachstate (&qm_attr, PTHREAD_CREATE_JOINABLE);

    /* Set up the thread arguments.
     */
    dtsq = dts->queues[index];
    arg[index] = (qmArg *) calloc (1, sizeof(qmArg));
    strcpy (arg[index]->qname, dts->queues[index]->name);
    arg[index]->threadNum = index;
    arg[index]->dtsq = dtsq;
    dtsq = dts->queues[index];
        

    /*  Remove existing semaphores.  It's okay if these fail since the
     *  semaphore may not actually exist.   
     */
    pthread_mutex_lock (&dtsq->mutex);
    dts_semRemove (dtsq->activeSem);
    dts_semRemove (dtsq->countSem);

    dtsq->activeSem = dts_semInit (((int)getuid()+50+index), QUEUE_ACTIVE);
    dtsq->countSem = dts_semInit (((int)getuid()+dtsq->port), dts_queueRestart (dts, dtsq));
       

    if (dtsq->activeSem < 0) {
        dtsLog (dts, "Cannot initialize '%s' active semaphore", dtsq->name);
        status = ERR;
    }
    if (dtsq->countSem < 0) {
        dtsLog (dts, "Cannot initialize '%s' count semaphore, port=%d", 
            dtsq->name, dtsq->port);
        status = ERR;
    }
    if (status == ERR) {
        fprintf (stderr, "ERROR: initializing semaphores\n");
        exit(1);
    }

    if ((rc = pthread_create (&qm_tid, &qm_attr, (void *)dts_launchQueueManager,
	(void *)arg[index]))) {
            fprintf (stderr, "ERROR: QM pthread_create() fails, code: %d\n",rc);
            exit (1);
    } else if (dts->verbose) {
        dtsLog (dts, "Running QMgr[%d]: t='%s' c=%-3d n='%s'\n", 
            index, dts_cfgQTypeStr (dtsq->type), 
        dts_semGetVal (dtsq->countSem), dtsq->name);
    }
    pthread_mutex_unlock (&dtsq->mutex);
    dtsq->qm_tid = qm_tid;  /* save the thread ID           */

    return (OK);
}


/****************************************************************************
**  DTS_LAUNCHQUEUEMANAGER -- Worker procedure to manage the transfer queues.
*/
#define CLEANUP_NOTCALLED 	0
#define CLEANUP_CALLED 		1

void
dts_launchQueueManager (void *data)
{
    qmArg *arg = data;
    dtsQueue *dtsq = (dtsQueue *) NULL;
    DTS *dts = (DTS *) NULL;
    int  threadNum = 0;
    static int err_ret = ERR;

    extern void xr_reallocClients (void);


    /* pthread_cleanup_push (dts_threadCleanup, (void*) CLEANUP_CALLED); */

    if (arg) {			/* make sure we have valid pointers	*/
	arg  = data;
	dtsq = arg->dtsq;
	dts  = dtsq->dts;
	threadNum = arg->threadNum;
    } else
	pthread_exit (&err_ret);

    free ((char *) arg);	/* free the argument pointer		*/

    xr_reallocClients ();

    /*  Start the appropriate queue manager.
     */
    switch (dtsq->type) {
    case QUEUE_NORMAL: 	   dts_normalQueueManager    (dts, dtsq);   break;
    case QUEUE_SCHEDULED:  dts_scheduledQueueManager (dts, dtsq);   break;
    case QUEUE_PRIORITY:   dts_priorityQueueManager  (dts, dtsq);   break;
    default:
	dtsLog (dts, "Invalid queue type '%d'", dtsq->name, dtsq->type);
	break;
    }

    /*  NEVER RETURNS ...... But be nice if we do.
     */
    dtsLog (dts, "ERROR: launchQueueManager exiting on '%s'....\n", dtsq->name);
    pthread_exit (&err_ret);
}


/****************************************************************************
**  DTS_QUEUENEXT -- Block until there is a next file in the queue to
**  process.
*/
#define	DBG_QUEUE (getenv("QUEUE_DBG")!=NULL||access("/tmp/QUEUE_DBG",F_OK)==0)

int
dts_queueNext (DTS *dts, dtsQueue *dtsq)
{
    char curfil[SZ_PATH], nextfil[SZ_PATH], lockfil[SZ_PATH], dir[SZ_PATH];
    int  i, count = 0, current = 0, next = 0;
    int activeVal = -1;


    /*  Choose greater than 50 because QUEUE_SHUTDOWN is 99, sometimes
     *  becomes 98. Overkill.
     */
    activeVal = dts_semGetVal (dtsq->activeSem);
    if (activeVal > 50)     // should be QUEUE_SHUTDOWN?
      return -99;

#ifdef USE_SEMS
    count = (dts_semDecr (dtsq->countSem) >= 0);

#else
    /*  Get the file-based current/next values of the queue.
     */
    memset (curfil, 0, SZ_PATH);
    sprintf (curfil, "%s/spool/%s/current", dts->serverRoot, dtsq->name);

    memset (nextfil, 0, SZ_PATH);
    sprintf (nextfil, "%s/spool/%s/next", dts->serverRoot, dtsq->name);

    while (1) {

        if ((activeVal = dts_semGetVal (dtsq->activeSem)) > 50)
	    return -99;

        next = dts_queueGetNext (nextfil);
        current = dts_queueGetCurrent (curfil);
        count = next - current;

    	memset (dir, 0, SZ_PATH);
    	sprintf (dir, "%s/spool/%s/%d/", dts->serverRoot, dtsq->name, current);

if (DBG_QUEUE)
  fprintf (stderr, "dir(%s)(%d): next=%d cur=%d cnt=%d\n", 
    dir, access (dir, F_OK), next, current, count);

	if (access (dir, F_OK) != 0) {
if (DBG_QUEUE)
  fprintf (stderr, "wait 1: next=%d cur=%d cnt=%d\n", next, current, count);
	    sleep (_queue_pause_time_);

	} else if (count == 0) {
if (DBG_QUEUE)
  fprintf (stderr, "wait 2: next=%d cur=%d cnt=%d\n", next, current, count);
	    sleep (_queue_pause_time_);

	} else {
if (DBG_QUEUE)
  fprintf (stderr, "breaking: next=%d cur=%d cnt=%d\n", next, current, count);
    	    memset (lockfil, 0, SZ_PATH);
    	    sprintf (lockfil, "%s/_lock", dir);

	    for (i=0; i < 5 && (access (lockfil, F_OK) == 0); i++) {
if (DBG_QUEUE)
  fprintf (stderr, "lockfile(%s): next=%d cur=%d cnt=%d\n", 
    lockfil, next, current, count);
	        sleep (_queue_pause_time_);
	    }
	    if (i < 5)
	        break;
	}
    }
#endif

sleep (1);

if (DBG_QUEUE)
  fprintf (stderr, "proc(%s)(%d): next=%d cur=%d cnt=%d\n", 
    dir, access (dir, F_OK), next, current, count);

    return (count);
}


/****************************************************************************
**  DTS_NORMALQUEUEMANAGER -- Manage normal transfer queues.
*/
void
dts_normalQueueManager (DTS *dts, dtsQueue *dtsq)
{
    char   curfil[SZ_PATH], ctrlpath[SZ_PATH], spath[SZ_PATH];
    char   rejpath[SZ_PATH], cpath[SZ_PATH], lpath[SZ_PATH], logpath[SZ_PATH];
    char  *qpath = (char *) NULL, msg[SZ_PATH], ppath[SZ_PATH], lfpath[SZ_PATH];
    char  *lp    = (char *) NULL, *dest = (char *) NULL;
    int    count=0, wait=0, current=0, done=0, stat=OK, key=0, nres=0, i;
    int    activeVal = QUEUE_RUNNING;
    Control *ctrl = (Control *) NULL;
    Entry   *entry = (Entry *) NULL, *e = (Entry *) NULL;
    xferStat xfs;


    /*  Initialize the queue.  It is as this point we do any error
     *  recovery from an earlier crash.
     */
    pthread_mutex_lock (&dtsq->mutex);
    dts_queueCleanup (dts, dtsq);
    pthread_mutex_unlock (&dtsq->mutex);

    /*  The logic here is simply to continue processing as long as there
     *  are objects in the queue, i.e. so long as the countSem semaphore
     *  is greater than zero.  Any attempt to decrement a zero semaphore
     *  will block, so we just wait until the value has been incremented
     *  indicating new data have arrived.  The increment is done in the
     *  endTransfer method following successful transfer of a new object
     *  into the queue.
     */
    dtsq->status = QUEUE_RUNNING;

    if (! (dest = dts_getAliasDest (dtsq->dest)))
	dest = dts_getLocalHost ();

#ifdef USE_SEM_COUNTERS
    while ((count = dts_semDecr (dtsq->countSem)) >= 0) {
#endif
    while ((count = dts_queueNext (dts, dtsq)) >= 0) {

	/* The startup initialized the activeSem to one.  Try to decrement
	 * the value; This will block if the queue has been paused externally
	 * until the value is eventually reset.  Save the state at this time
	 * so we can reset at the bottom of the loop.
	 */
        activeVal = dts_semGetVal (dtsq->activeSem);
        if (activeVal > 50) { 		// QUEUE_SHUTDOWN= 99) {
            count = -99;
            break;
        } else
            wait = dts_semDecr (dtsq->activeSem);

	if (! (dtsq->dest && dtsq->dest[0]))	/* endpoint		*/
	    continue;

	stat = OK;
	memset (lpath,    0, SZ_PATH);		/* local file path	*/
	memset (ppath,    0, SZ_PATH);		/* previous directory	*/
	memset (spath,    0, SZ_PATH);		/* spool directory	*/
	memset (cpath,    0, SZ_PATH);		/* current directory	*/
	memset (lfpath,   0, SZ_PATH);		/* lock file path	*/
	memset (curfil,   0, SZ_PATH);		/* current file		*/
	memset (ctrlpath, 0, SZ_PATH);		/* control file		*/
	memset (logpath,  0, SZ_PATH);		/* log file		*/

        /*  Initialize the queue status and validate our connection
         *  to the DTS before processing.
         */
	sprintf (spath, "%s/spool/", dts->serverRoot);
	sprintf (curfil, "%s/spool/%s/current", dts->serverRoot, dtsq->name);
	current = dts_queueGetCurrent (curfil);

	sprintf (cpath, "%sspool/%s/%d",		/* current path     */ 
	    dts->serverRoot, dtsq->name, current);
	sprintf (ppath, "%sspool/%s/%d",		/* previous path    */ 
	    dts->serverRoot, dtsq->name, (current-1));
        sprintf (ctrlpath, "%s/_control", cpath);
        sprintf (lfpath, "%s/_lock", cpath);
        strcpy (logpath, dts_getQueueLog (dts, dtsq->name, "log.out"));


	if (access (cpath, F_OK) != 0) {
	    dtsLog (dts, "No current path '%s'\n", cpath);
	    continue;
	}

	for (i=5; i ; i--) {
	    if (access (ctrlpath, R_OK) == 0) {
                ctrl = dts_loadControl (ctrlpath, &dtsq->ctrl);
		memset (dtsq->outfile, 0, SZ_PATH);
		strcpy (dtsq->outfile, ctrl->xferName);
	    } else if (i == 1) {
	        if (access (lfpath, F_OK) == 0) {
    	            dtsLog (dts, "%6.6s >  Error: LOCKFILE EXISTS %s", 
			dts_queueNameFmt (dtsq->name), lfpath);
		}
    	        dtsLog (dts, "%6.6s >  Error: missing ctrl %s", 
		    dts_queueNameFmt (dtsq->name), ctrlpath);
	    } else
		sleep (1);
	}


	/*  Delete the now-complete spool directory.
         */
        if (current && dtsq->auto_purge)
            dts_queueDelete (dts, ppath);

	/*  Lookup the file in the transfer database.  Use the first 
	 *  unprocessed image.
	 */
 	if ((entry = dts_dbLookup (ctrl->xferName, dtsq->name, &nres))) {
	    e = entry;
	    for (i=0; i < nres; i++) {
	        if (e && e->time_out[0])
		    e++;
		else {
		    key = e->key;
		    break;
		}  
	    }
	    free (entry);				
	    entry = (Entry *) NULL;
	}

	/*  Skip processing if the file was marked with an ERR file
	 *  from the deliver
	 */
	sprintf (rejpath, "%s/ERR", cpath);
	if (access (rejpath, F_OK) == 0) {
	    dtsLog (dts, "Skipping path '%s'\n", cpath);
            dtsq->qstat->failedxfers++;
	    goto err_init;
	}


	if (debug > 1) {
	    dtsLog (dtsq->dts, "%6.6s >  processing %s\n", 
		dts_queueNameFmt (dtsq->name), cpath);
	    dtsLog (dtsq->dts, "%6.6s >  remaining %d\n", 
		dts_queueNameFmt (dtsq->name),
		dts_semGetVal (dtsq->countSem));
	}

        sprintf (lpath, "%s%s", (lp = dts_sandboxPath(ctrl->queuePath)), 
	    ctrl->xferName);
	for (done=0; ! done; ) {
	    if (debug > 2)
	        dtsLog (dtsq->dts, "%6.6s >  verifying %s {%s}\n", 
		    dts_queueNameFmt (dtsq->name), dtsq->dest, dest);

	    qpath = dts_verifyDTS (dest, dtsq->name, lpath);
	    if (! qpath ) {
	        /*  There was some sort of error, wait and try again.
	         *
	         *  	FIXME -- Need something better here.....
	         */
	        dtsLog (dtsq->dts, "DTS verification failed to '%s'", 
		    dtsq->dest);
	        sleep (DTSD_PAUSE);
	    } else {
	        if (debug > 2)
	            dtsLog (dtsq->dts, "%6.6s >  %s verified\n", 
			dts_queueNameFmt (dtsq->name), dtsq->dest);
	        done++;
	    }
	}

 	/*  Make sure we can access the local file.
	 */
	while (access (lpath, R_OK) < 0) {
            dtsLog (dtsq->dts, "Error: Cannot access '%s'.\n", lpath);
	    sleep (2);

            // Would loop infinitely before - Travis fix
            if (dts_queueGetCurrent (curfil) != current)
		break;
	}
	free ((char *) lp);


        /* The code below makes it so if we change current while stuck in the
         * while loop, it will start at the top of the queue loop. Before it
         * would just sit there and be stuck until you fix the access
         * problem.  This way our "poke" can get us out of this problem and
	 * onto the next file.
         */
        if (dts_queueGetCurrent (curfil) != current) {
            /*  Failed because we couldn't access the file so we skipped.
             */
            dtsq->qstat->failedxfers++;
            if (qpath)
                free ((void *) qpath), qpath = NULL;
            continue;
        }

        /*  If we made it this far, we can talk to the DTS, so initialize
         *  the control file to be used.
         */
	if (debug)
	    dtsLog (dtsq->dts, "%6.6s >  INIT: initializing %s\n", 
		dts_queueNameFmt (dtsq->name), cpath);
        if (dts_queueInitControl (dest, dtsq->name, qpath, 
	    ctrl->igstPath, lpath, ctrl->filename, ctrl->deliveryName) != OK) {
                sprintf (msg, "%6.6s >  PROC: Cannot init transfer ERR: '%s'\n",
		    dts_queueNameFmt (dtsq->name), ctrl->xferName);
                dtsq->qstat->failedxfers++;	// failed transfer
                dtsLogMsg (dtsq->dts, 1, msg);
                dtsLogMsg (dtsq->dts, key, msg);
	        dts_semIncr (dtsq->countSem);      // restore count value
	        stat = ERR;
	        goto err_init;
	}
	    

	/*  Process the file transfer.
	 */
	if (debug > 2)
	    dtsLog (dtsq->dts, "%6.6s >  PROC: processing\n",
		dts_queueNameFmt (dtsq->name), cpath);

	dts_dbSetTime (key, DTS_TSTART);
        gettimeofday (&dtsq->init_time, NULL);
        if (dts_queueProcess(dtsq,ctrl->queuePath,qpath,ctrl->xferName) == OK) {

	    /*
	    dts_semSetVal (dtsq->activeSem, QUEUE_WAITING);
	    while (dts_semGetVal (dtsq->activeSem) == QUEUE_WAITING)
		sleep (1);
	    */

            if (dts_hostEndTransfer(dest, dtsq->name, qpath) != OK) {
	        memset (msg, 0, SZ_PATH);
                sprintf (msg, "%6.6s >  PROC: Error in endTransfer '%s'\n",
		    dts_queueNameFmt (dtsq->name), ctrl->xferName);
                dtsq->qstat->failedxfers++;	// failed transfer
                dtsLogMsg (dtsq->dts, 1, msg);
                dtsLogMsg (dtsq->dts, key, msg);
	        dts_semIncr (dtsq->countSem);		// restore count value
		stat = ERR;
	    }

        } else {
	    memset (msg, 0, SZ_PATH);
            sprintf (msg, "%6.6s >  PROC: File transfer fails '%s'\n", 
		dts_queueNameFmt (dtsq->name), ctrl->xferName);
            dtsq->qstat->failedxfers++;		// failed transfer
            dtsLogMsg (dtsq->dts, 1, msg);
            dtsLogMsg (dtsq->dts, key, msg);
	    dts_semIncr (dtsq->countSem);		// restore count value
	    stat = ERR;
	}

        gettimeofday (&dtsq->end_time, NULL);
        xfs.fsize = ctrl->fsize;
        xfs.time = (float) dts_timediff (dtsq->init_time, dtsq->end_time);
        xfs.sec =  (int) xfs.time;
        xfs.usec = (xfs.time - (int) xfs.time) * 1000000.0;
        xfs.tput_mb = transferMb (xfs.fsize, xfs.sec, xfs.usec);
        xfs.tput_MB = transferMB (xfs.fsize, xfs.sec, xfs.usec);
    	//dts_logControl (ctrl, stat, logpath);
    	dts_logXFerStats (ctrl, &xfs, stat, logpath);


	/*  Send the Transfer End time.
	 */
	dts_dbSetTime (key, DTS_TEND);


	/*  Log the completion transfer and free the db lookup.
	 */
	if (dts->debug > 2)
	    dtsLogMsg (dtsq->dts, key, "%6.6s >  XFER: xfer done %s\n", 
		dts_queueNameFmt (dtsq->name), cpath);
	dts_dbSetTime (key, DTS_TIME_OUT);
	memset (dtsq->outfile, 0, SZ_PATH);


	/*  If we're not paused, reset to active mode.
	 */
err_init:
	activeVal = dts_semGetVal (dtsq->activeSem);
#ifdef OLD_METHOD
	if (activeVal != QUEUE_PAUSED)
	    dts_semSetVal (dtsq->activeSem, QUEUE_ACTIVE);
#else
        if (( !(activeVal > 50) ) || 
	        activeVal != QUEUE_PAUSED || 
	        activeVal != QUEUE_RESPAWN || 
	        activeVal != QUEUE_KILLED || 
	        activeVal != QUEUE_RESPAWNING )
                    dts_semSetVal (dtsq->activeSem, QUEUE_ACTIVE);
#endif

	/*  If there was an error, don't increment the current value and we'll
	 *  resend the file.
	 */
	if (stat == OK)
	    dts_queueSetCurrent (curfil, ++current);
	else
	    dtsLog (dts, "%6.6s >  DONE: Resending file '%s'", 
		dtsq->name, ctrl->xferName);

	if (qpath)
	    free ((void *) qpath), qpath = NULL;

	if (debug)
	    dtsLog (dtsq->dts, "%6.6s >  DONE: loop complete %s >>>>>>>>>>>\n", 
		dtsq->name, cpath);

	// if (activeVal == QUEUE_SHUTDOWN || dts->shutdown) {
	if (activeVal > 50 || dts->shutdown) {
	    if (debug)
	        dtsLog (dtsq->dts, "%6.6s >  DTS shutdown requested %s\n", 
		    dtsq->name);
	    break;
	}
    }

    /*  Never returns ..... But if we do get here, make it clean.
     */
    if (count != -99)
	dtsLog (dtsq->dts, "ERROR: NormalQueueMgr exiting....count = %d\n",
	    count);
    else {
        pthread_mutex_lock (&dtsq->mutex);
        dts_semSetVal (dtsq->activeSem, QUEUE_KILLED);
        dtsLog (dtsq->dts, "ERROR: NormalQueueMgr exiting....it was killed\n");
        pthread_mutex_unlock (&dtsq->mutex);
	exit (0);
    }

    if (qpath)
        free ((void *) qpath), qpath = NULL;
    if (entry)
        free (entry), entry = (Entry *) NULL;

    pthread_exit (&stat);
}


/****************************************************************************
**  DTS_SCHEDULEDQUEUEMANAGER -- Manage scheduled transfer queues.
*/
void
dts_scheduledQueueManager (DTS *dts, dtsQueue *dtsq)
{
    char  curfil[SZ_PATH], ctrlpath[SZ_PATH], cpath[SZ_PATH], lpath[SZ_PATH];
    char *qpath = (char *) NULL, *lp = (char *) NULL, msg[SZ_PATH];
    int   count = 0, wait = 0, current = 0, done = 0, stat = OK;
    int   nres = 0, key = 0, i;
    time_t  last_time;
    struct timespec delay = { (time_t) 0, (long) 0 };
    Control *ctrl = (Control *) NULL;
    Entry   *entry = (Entry *) NULL, *e = (Entry *) NULL;


    last_time = time (NULL);			/* get current time	   */

    /*  For a scheduled queue, we process either at fixed intervals or
     *  at specific times.  When we first start up process any pending
     *  items in the queue, thereafter stick to the schedule.
     */
    dtsq->status = QUEUE_RUNNING;

    while (1) {

	/* First time through, do any processing necessary.
	 */
        count = dts_semGetVal (dtsq->countSem);
        while (count-- > 0) {

	    /* The startup initialized the activeSem to one.  Try to decrement
	     * the value; This will block if the queue has been paused 
	     * externally until the value is eventually reset.  Save the state
	     * at this time so we can reset at the bottom of the loop.
	     */
	    wait = dts_semDecr (dtsq->activeSem);

	    if ( dtsq->node == QUEUE_ENDPOINT )
		continue;

	    stat = OK;
            memset (lpath,    0, SZ_PATH);
            memset (cpath,    0, SZ_PATH);
            memset (curfil,   0, SZ_PATH);
            memset (ctrlpath, 0, SZ_PATH);

            /*  Initialize the queue status and validate our connection
             *  to the DTS before processing.
             */
            sprintf (curfil, "%s/spool/%s/current", dts->serverRoot,dtsq->name);
            current = dts_queueGetCurrent (curfil);

            sprintf (cpath, "%s/spool/%s/%d",               /* current path */ 
                dts->serverRoot, dtsq->name, current);
            sprintf (ctrlpath, "%s/_control", cpath);


	    /*  Lookup the file in the transfer database.  Use the first 
	     *  unprocessed image.
	     */
 	    if ((entry = dts_dbLookup (ctrl->xferName, dtsq->name, &nres))) {
	        e = entry;
	        for (i=0; i < nres; i++) {
	            if (e && e->time_out[0])
		        e++;
		    else {
		        key = e->key;
		        break;
		    }  
	        }
	        free (entry);				
		entry = (Entry *) NULL;
	    }


	    if (access (ctrlpath, F_OK) == 0) 
                ctrl = dts_loadControl (ctrlpath, &dtsq->ctrl);
	    else
    	        dtsLog (dtsq->dts, "%6.6s >  missing ctrl %s",dtsq->name,ctrlpath);

	    if (debug > 2)
	        dtsLog (dtsq->dts, "%6.6s >  PROC: processing %s\n", 
		    dtsq->name, cpath);

            sprintf (lpath, "%s%s", (lp = dts_sandboxPath(ctrl->queuePath)),
                ctrl->xferName);
	    for (done=0; ! done; ) {
                qpath = dts_verifyDTS (dtsq->dest, dtsq->name, lpath);
                if (! qpath) {
                    /*  There was some sort of error, wait and try again.
                     *
                     *      FIXME -- Need something better here.....
                     */
                    dtsLog (dts, "DTS verification failed");
                    sleep (DTSD_PAUSE);
		    continue;
                }
    	        done++;
            }


            /*  Make sure we can access the local file.
             */
            while (access (lpath, R_OK) < 0) {
                dtsLog (dtsq->dts, "Error: Cannot access '%s'.\n", lpath);
                sleep (2);
            }
            free ((char *) lp);


            /*  If we made it this far, we can talk to the DTS, so initialize
             *  the control file to be used.
             */
            if (debug > 2)
                dtsLog (dtsq->dts, "%6.6s >  INIT: initializing %s\n", 
		    dtsq->name,cpath);
            if (dts_queueInitControl(dtsq->dest, dtsq->name, qpath,
		ctrl->igstPath,lpath,ctrl->filename,ctrl->deliveryName) != OK) {
                    dtsLog (dtsq->dts, "Error: Cannot init transfer '%s'\n",
                        ctrl->xferName);
                    stat = ERR;
                    dts_semIncr (dtsq->countSem);  /* restore count value */
                    goto err_init;
            }


            /* Process the file transfer.
             */
	    dts_dbSetTime (key, DTS_TSTART);
            if (dts_queueProcess (dtsq, ctrl->queuePath, 
		qpath, ctrl->xferName) == OK) {
                    if (dts_hostEndTransfer (dtsq->dest,dtsq->name,qpath)!=OK) {
	        	memset (msg, 0, SZ_PATH);
               		sprintf (msg, "Error: Cannot end transfer '%s'\n",
			    lpath);
               		dtsLogMsg (dtsq->dts, 1, msg);
               		dtsLogMsg (dtsq->dts, key, msg);
                	/* dts_semIncr (dtsq->countSem); */
                	stat = ERR;
		    }
            } else {
	        memset (msg, 0, SZ_PATH);
                sprintf (msg, "Error: Cannot transfer file '%s'.\n", lpath);
                dtsLogMsg (dts, 1, msg);
                dtsLogMsg (dts, key, msg);
               	stat = ERR;
            }


	    /*  Send the Transfer End time.
	     */
	    dts_dbSetTime (key, DTS_TEND);

	    /*  Log the completion transfer and free the db lookup.
	     */
	    dtsLogMsg (dtsq->dts, key, "%6.6s >  xfer done %s\n", 
		dtsq->name, cpath);
	    dts_dbSetTime (key, DTS_TIME_OUT);
	    if (entry)  free (entry);				


	    /* If we're not paused, reset to active mode.
	     */
err_init:
	    if (dts_semGetVal (dtsq->activeSem) != QUEUE_PAUSED)
	        dts_semSetVal (dtsq->activeSem, QUEUE_ACTIVE);

	    if (stat == OK)
	        dts_queueSetCurrent (curfil, ++current);

	    if (stat == OK && count >= 0)
	        (void) dts_semDecr (dtsq->countSem);

            if (qpath)
		free ((void *) qpath), qpath = NULL;

            if (debug)
                dtsLog (dtsq->dts, "%6.6s >  DONE: loop complete %s\n", 
		    dtsq->name, cpath);
        }

	delay.tv_sec = dts_schedTime (dtsq, last_time);
	nanosleep (&delay, 0);
    }

    if (entry)
        free (entry), entry = (Entry *) NULL;

    /*  Never returns ..... But if we do get here, make it clean.
     */
    dtsLog (dts, "ERROR: NormalQueueManager exiting....\n");
    pthread_exit (&stat);	
}


time_t
dts_schedTime (dtsQueue *dtsq, time_t last)
{
    return (dtsq->interval);			/* Not Yet Implemented	*/
}


/****************************************************************************
**  DTS_THREADCLEANUP -- Exit handler for queue manager threads.
*/
void
dts_threadCleanup (void *flag)
{
    fprintf (stderr, "Queue thread %d is exiting....\n", (int)flag);
    return;
}



/****************************************************************************
**  DTS_PRIORITYQUEUEMANAGER -- Manage priority transfer queues.
*/
void
dts_priorityQueueManager (DTS *dts, dtsQueue *dtsq)
{
}


/****************************************************************************
**  DTS_QUEUEOBJXFER -- Transfer and object between queues.
*/
int
dts_queueObjXfer (DTS *dts, dtsQueue *dtsq)
{
    char  curfil[SZ_PATH], ctrlpath[SZ_PATH], cpath[SZ_PATH];
    char *qpath = (char *) NULL;
    int   current = 0, done = 0 ;
    Control *ctrl = (Control *) NULL;


    memset (ctrlpath, 0, SZ_PATH);
    memset (cpath, 0, SZ_PATH);
    memset (curfil, 0, SZ_PATH);

    /*  Initialize the queue status and validate our connection
     *  to the DTS before processing.
     */
    sprintf (curfil, "%s/spool/%s/current", dts->serverRoot, dtsq->name);
    current = dts_queueGetCurrent (curfil);

    sprintf (cpath, "%s/spool/%s/%d",		/* current path */ 
	dts->serverRoot, dtsq->name, current);
    sprintf (ctrlpath, "%s/_control", cpath);
    if (! (ctrl = dts_loadControl (ctrlpath, &dtsq->ctrl))) 
	return (ERR);
    dtsLog (dtsq->dts, "%s: processing %s", dtsq->name, cpath);

    while (! done) {
	//qpath = dts_verifyDTS (dtsq->dest, dtsq->name, ctrl->xferName);
	qpath = dts_verifyDTS (dtsq->dest, dtsq->name, ctrl->srcPath);
	if (! qpath ) {
	    /*  There was some sort of error, wait and try again.
	     *
	     *  	FIXME -- Need something better here.....
	     */
	    dtsLog (dts, "DTS verification failed");
	    sleep (DTSD_PAUSE);
	    continue;
	}
	done++;
    }

    /*  If we made it this far, we can talk to the DTS, so initialize
     *  the control file to be used.
     */
    dts_queueInitControl (dtsq->dest, dtsq->name, qpath, 
	ctrl->igstPath, ctrl->xferName, ctrl->filename, ctrl->deliveryName);

    /* Process the file transfer.
     */
    if (dts_queueProcess (dtsq,ctrl->queuePath,qpath,ctrl->xferName) == OK)
        dts_hostEndTransfer (dtsq->dest, dtsq->name, qpath);
    else {
        dtsq->qstat->nerrs++; 		// increment queue error
        dtsLog (dts, "Error: Cannot transfer file '%s'.\n", ctrl->xferName);
    }

    if (qpath)
	free ((void *) qpath), qpath = NULL;

    return (OK);
}


/****************************************************************************
**  DTS_QUEUERESTART -- Restart a queue from a previous failure or crash.
**  We perform any cleanup necessary and return the number of items to be
**  processed.
*/
int
dts_queueRestart (DTS *dts, dtsQueue *dtsq)
{
    char curfil[SZ_PATH], nextfil[SZ_PATH];
    int  current=0, next=0, pending=0;

    memset (curfil, 0, SZ_PATH);
    sprintf (curfil, "%s/spool/%s/current", dts->serverRoot, dtsq->name);
    current = dts_queueGetCurrent (curfil);

    memset (nextfil, 0, SZ_PATH);
    sprintf (nextfil, "%s/spool/%s/next", dts->serverRoot, dtsq->name);
    next = dts_queueGetNext (nextfil);

    if (current == 0 && next == 0) {
	/* Fresh queue with no processed or pending data. */
	return (0);

    } else { 
	char  lockfil[SZ_PATH];

        memset (lockfil, 0, SZ_PATH);
        sprintf (lockfil, "%s/spool/%s/_lock", dts->serverRoot, dtsq->name);

	if (access (lockfil, F_OK) == 0) {
	    /* The current dir was in the middle of a transfer.  Remove the 
	     * lockfile so we can try again.
	     */
	    unlink (lockfil);
	}
	pending = next - current;
    }

    if (dts->verbose > 1)
        dtsLog (dts, "Queue '%s' restarting w/ count = %d\n", 
	    dtsq->name, pending);
    return (pending);
}


/******************************************************************************
**  DTS_STARTTIMERSTATS -- Start the statistics timer thread.
*/
void 
dts_startTimerStats (DTS *dts)
{
    pthread_t  tid = (pthread_t) NULL;
    pthread_attr_t  attr;
    register int  rc;


    /* Initialize the service processing thread attribute.
    */
    pthread_attr_init (&attr);
    pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);

    if ((rc = pthread_create (&tid, &attr,
        (void *)dts_statTimer, (void *)dts))) {
            fprintf (stderr, "ERROR: statTimer create fails, code: %d\n", rc);
            exit (1);
    } else if (dts->verbose)
        dtsLog (dts, "Running StatTimer: tid=%d p=%d\n", tid, dts->stat_period);

    dts->stat_tid = tid;       /* save the thread ID           */

    return;
}


/******************************************************************************
**  DTS_STATTIMER -- Broadcast the DTSD statistics at the specified interval.
*/
void
dts_statTimer (void *data)
{
    char  msg[SZ_LINE], curfil[SZ_PATH], nextfil[SZ_LINE];
    int   current, next;
    DTS  *dts = (DTS *) data;


    while (1) {
	sleep (dts->stat_period);
        sleep (2);

        /*  This is the code that records the heartbeat status.  We get
         *  the number of in/out calls from the libxrpc code and format
         *  a message that is a simle keyw=val string.  This is sent to
         *  the DTSMON if one is connected.         *
         *  Note that dtsLogStat() can be used to log any sort of keyword
         *  value that we might want to sent to the web monitor.  The
         *  xr_getVal() code below should be replaced wiht the actual
         *  interface.
         */
        xr_dtsstat * mystatp = &mystat;
        xr_getStat (mystatp);

        float   dfree = 0.0, dused = 0.0;
        dtsQueue   *dtsq = (dtsQueue *) NULL;
        queueStat  *qs;
        char *rdir = dts_sandboxPath ("/"), root[SZ_PATH];

        memset (root, 0, SZ_PATH);              /* get root path        */
        strcpy (root, rdir);

#ifdef Linux
        struct statfs fs;
    	statfs (rdir, &fs);           /* get disk usage       */
#else
        struct statvfs fs;
    	statvfs (rdir, &fs);          /* get disk usage       */
#endif
        dfree = (fs.f_bavail * ((float)fs.f_frsize / (1024.*1024.)));
        dused = ((fs.f_blocks-fs.f_bavail) * 
            ((float)fs.f_frsize / (1024.*1024.)));

        int i;
        for (i=0; i < dts->nqueues; i++) {
            dtsq = dts->queues[i];
            qs = dts_queueGetStats (dtsq);
            // maybe use qs->avg_rate ?
            sprintf (msg, "QRATEOUT=%s=%f\n" ,dtsq->name, qs->tput_mb);
            dtsLogStat (dts, msg);

            memset (curfil,  0, SZ_PATH);
            sprintf (curfil, "%s/spool/%s/current", dts->serverRoot,dtsq->name);
            current = dts_queueGetCurrent (curfil);

            memset (nextfil, 0, SZ_PATH);
            sprintf (nextfil, "%s/spool/%s/next", dts->serverRoot, dtsq->name);
            next = dts_queueGetNext (nextfil);

            sprintf(msg, "QNUM=%s=%d\n", dtsq->name, (next - current) );
            dtsLogStat (dts, msg);
        }

        memset (msg, 0, SZ_LINE);
        sprintf (msg, "usedspace=%f\n", dused/1024.);
        dtsLogStat (dts, msg);

        memset (msg, 0, SZ_LINE);
        sprintf (msg, "dtsd_in=%lu\n", mystat.NumMethod);
        dtsLogStat (dts, msg);

        memset (msg, 0, SZ_LINE);
        sprintf (msg, "dtsd_out=%lu\n", mystat.NumCallSync);
        dtsLogStat (dts, msg);

        memset (msg, 0, SZ_LINE);

        // Set semaphores back to zero.
        if (semctl(stat_semid, 0, SETVAL, 0) == -1) {
            printf("error in semctl");
            exit(0);
        }
        if (semctl(stat_semid, 1, SETVAL, 0) == -1) {
            printf("error in semctl");
            exit(0);
        }
    }

    /*  never returns  */
}



/****************************************************************************
**
**  Error handling
*/


/*  DTSONEXIT -- Onexit handler for the DTS daemon.
*/
void
dtsOnExit (int stat, void *data)
{
    /*  FIXME -- Won't work since xml-rpc clients have been shut down
    **           by the time we are called.  No-op for now ....
    */
    if (!normal_exit && dts_monitor >= 0) {
        int     async;

        async = xr_newASync (DTSMON_CLIENT);
        xr_setStringInParam (async, dts->whoAmI);
        xr_setStringInParam (async, "DTS exiting");
        xr_callASync (async, "dtslog", dts_nullResponse);
    }
}




/**
 *  USAGE -- Print usage information
 */
static void
dtsd_Usage (void)
{
  fprintf (stderr, "\
    Usage:\n\
  	dtsd [options]\n\
  \n\
    Options:\n\
  	-h 		print help\n\
  \n\
   	-c <cfg>	specify the config file to use\n\
   	-d		run as a daemon\n\
  	-i 		run interactively\n\
  	-p <port>	specify server port\n\
        -t		trace\n\
  	-r <path>	specify server root dir\n\
        -v		verbose\n\
  \n\
  	-H <host>	specify the host name to simulate\n\
  	-I       	initialize DTSD working directory\n\
  \n");
}

