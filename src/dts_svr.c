/** 
**  DTS_SVR -- Minimal Data Transfer System Server.
**
**  Usage:
**	dts_svr [options]
**
**  Options:
**	-h 		print help
**
** 	-c <cfg>	specify the config file to use
** 	-d		run as a daemon
**	-i 		run interactively
**	-p <port>	specify server port
**      -t		trace
**	-r <path>	specify server root dir
**      -v		verbose
**
**	-H <host>	specify the host name to simulate
**	-I       	initialize DTSD working directory
**
**  @file	dts_svr.c
**  @author	Mike Fitzpatrick, NOAO
**  @date	July 2010
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>

#include "dts.h"
#include "dtsMethods.h"
#include "dtsClient.h"
#include "../build.h"

#define  _DTS_SRC_

#define	DTSD_PAUSE	5		/* Pause between retries (sec)	*/



DTS   *dts 		= (DTS *) NULL;	/* main DTS structure       	*/

int    debug		= 0;		/* debug flag			*/
int    trace		= 0;		/* trace flag			*/
int    verbose		= 0;		/* verbose flag			*/
int    daemonize	= 0;		/* run as daemon	    	*/
int    interactive	= 0;		/* run interactively	    	*/
int    init		= 0;		/* initialize server root    	*/

int    dts_monitor	= -1;		/* DTS monitor handle		*/
int    normal_exit	= 0;		/* did we exit normally?	*/


int     dts_initServerMethods (DTS *dts);

int     dts_queueRestart (DTS *dts, dtsQueue *dtsq);
int     dts_queueObjXfer (DTS *dts, dtsQueue *dtsq);
void    dts_threadCleanup (void *flag);

void    dts_cmdLoop (DTS *dts);
void    dtsFree (DTS *dts);
void 	dtsOnExit (int stat, void *data); 	/*  Not Used */

static  void dtsd_Usage (void);

typedef struct {
    char      qname[SZ_FNAME];			/* queue name		*/
    int       threadNum;			/* thread number	*/
    dtsQueue  *dtsq;				/* dtsQueue struct	*/
} qmArg, *qmArgP;




/**
 *  DTS Daemon Entry point.  
 */
int 
main (int argc, char **argv)
{
    register int i, j, len, port = DTS_PORT;
    char     ch, *host = NULL, *root = NULL, *config = NULL;



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
		case 'h':   dtsd_Usage ();		exit (0);
                case 'B':   use_builtin_config = 1; 	break;

		case 'c':   config = argv[++i];     	break;
		case 'd':   daemonize++; 	    	break;
		case 'i':   interactive++; 	    	break;
		case 'p':   port = atoi (argv[++i]); 	break;
		case 't':   trace++; 			break;
		case 'r':   root = argv[++i]; 		break;
		case 'v':   verbose++; 			break;

		case 'H':   host = argv[++i]; 	    	break;
		case 'I':   init++; 			break;

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


    /*  Initialize.  Passing in the port/host/svc let's us override the
    **  local setting or else pretend to be another server.
    */
    dts = dtsInit (config, port, host, root, init);

    dts->debug    = debug;
    dts->verbose  = verbose;

    dtsLog (dts, "Starting DTS server: %s", dts->serverUrl);
    dtsLog (dts, "DTS build version: %s", build_date);

    /*  Initialize the DTS working directory.
    */
    dts_initServerRoot (dts);

    /*  Initialize the DTS server methods.
    */
    dts_initServerMethods (dts);

    /*  Start the server running.  If interactive, run server on a separate
    **  thread and wait for input, otherwise we end in the server.
    */
    if (!interactive) {
	if (daemonize) {
	    dtsDaemonize (dts);
            dtsLog (dts, "DTS running as daemon\n");
	} else
            dtsLog (dts, "DTS server running\n");
        dtsLog (dts, "======================================================\n");
	xr_startServer ();

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
    xr_addServerMethod ("initDTS",         dts_initDTS,         NULL);
    xr_addServerMethod ("shutdownDTS",     dts_shutdownDTS,     NULL);
    xr_addServerMethod ("abort",           dts_abortDTS,        NULL);
    xr_addServerMethod ("cfg",             dts_Cfg,             NULL);


		/****  Queue Methods  ****/
    xr_addServerMethod ("startQueue",      dts_startQueue,      NULL);
    xr_addServerMethod ("stopQueue",       dts_stopQueue,       NULL);
    xr_addServerMethod ("flushQueue",      dts_flushQueue,      NULL);
    xr_addServerMethod ("restartQueue",    dts_restartQueue,    NULL);
    xr_addServerMethod ("addToQueue",      dts_addToQueue,      NULL);
    xr_addServerMethod ("removeFromQueue", dts_removeFromQueue, NULL);


		/****  Utility Methods  ****/
    xr_addServerMethod ("monitor",         dts_monAttach,       NULL);
    xr_addServerMethod ("console",         dts_monConsole,      NULL);
    xr_addServerMethod ("detach",          dts_monDetach,       NULL);

    xr_addServerMethod ("dtsList",         dts_List,            NULL);
    xr_addServerMethod ("dtsSet",          dts_Set,             NULL);
    xr_addServerMethod ("dtsGet",          dts_Get,             NULL);


		/****  File Utility Methods  ****/
    xr_addServerMethod ("access",          dts_Access,          NULL);
    xr_addServerMethod ("cat",             dts_Cat,             NULL);
    xr_addServerMethod ("copy",            dts_Copy,            NULL);
    xr_addServerMethod ("chmod",           dts_Chmod,           NULL);
    xr_addServerMethod ("cwd",             dts_Cwd,             NULL);
    xr_addServerMethod ("del",             dts_Delete,          NULL);
    xr_addServerMethod ("dir",             dts_Dir,             NULL);
    xr_addServerMethod ("isDir",           dts_CheckDir,        NULL);
    xr_addServerMethod ("diskFree",        dts_DiskFree,        NULL);
    xr_addServerMethod ("diskUsed",        dts_DiskUsed,        NULL);
    xr_addServerMethod ("echo",            dts_Echo,            NULL);
    xr_addServerMethod ("fsize",           dts_FSize,           NULL);
    xr_addServerMethod ("fmode",           dts_FMode,           NULL);
    xr_addServerMethod ("ftime",           dts_FTime,           NULL);
    xr_addServerMethod ("mkdir",           dts_Mkdir,           NULL);
    xr_addServerMethod ("ping",            dts_Ping,            NULL);
    xr_addServerMethod ("pingstr",         dts_PingStr,         NULL);
    xr_addServerMethod ("pingarray",       dts_PingArray,       NULL);
    xr_addServerMethod ("pingsleep",       dts_PingSleep,       NULL);
    xr_addServerMethod ("remotePing",      dts_remotePing,      NULL);
    xr_addServerMethod ("rename",          dts_Rename,          NULL);
    xr_addServerMethod ("setRoot",         dts_SetRoot,         NULL);
    xr_addServerMethod ("touch",           dts_Touch,           NULL);

		/****  Low-Level I/O Methods  ****/
    xr_addServerMethod ("read",        	   dts_Read,        	NULL);
    xr_addServerMethod ("write",           dts_Write,        	NULL);
    xr_addServerMethod ("prealloc",        dts_Prealloc,        NULL);
    xr_addServerMethod ("stat",            dts_Stat,            NULL);

		/****  Transfer Methods  ****/
    xr_addServerMethod ("xferPushFile",    dts_xferPushFile,    NULL);
    xr_addServerMethod ("xferPullFile",    dts_xferPullFile,    NULL);
    xr_addServerMethod ("receiveFile",     dts_xferReceiveFile, NULL);
    xr_addServerMethod ("sendFile",        dts_xferSendFile,    NULL);

    xr_addServerMethod ("initTransfer",    dts_initTransfer,    NULL);
    xr_addServerMethod ("doTransfer",      dts_doTransfer,      NULL);
    xr_addServerMethod ("endTransfer",     dts_endTransfer,     NULL);
    xr_addServerMethod ("cancelTransfer",  dts_cancelTransfer,  NULL);

		/****  Queue Methods  ****/
    xr_addServerMethod ("queueAccept",     dts_queueAccept,     NULL);
    xr_addServerMethod ("queueComplete",   dts_queueComplete,   NULL);
    xr_addServerMethod ("queueSetControl", dts_queueSetControl, NULL);
    xr_addServerMethod ("queueDest",       dts_queueDest,       NULL);
    xr_addServerMethod ("queueSrc",        dts_queueSrc,        NULL);

    return (OK);
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

