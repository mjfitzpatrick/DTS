/** 
**  DTSQ -- Queue a data object for transport.  Our job here is to 
**  verify that we can communicate with the DTS and that it will 
**  accept the object.  We have an optional mode to fork off and work
**  in the background so we return quickly, we can also keep a 
**  record of the transfer locally as well as provide recovery for
**  failed transfers.
**
**  Usage:
**
**    dtsq [-q <queue>] [-f] [-h <ip>] <path > [<path> ....]
**
**	-a			all hosts (recover mode only)
**	-b			bundle all files for transfer
**	-f			fork to do the data transfer
**	-m <method>		specify transfer method (push | give)
**	-p <param=value>	pass param/value pair
**	-s 			do file checksum
**
**	-R 			recover failed queue attempts
**	-V 			verify queue transfers
**
**	+f 			disable task fork
**	+s 			disable file checksum flag
**	+d 			debug flag
**
**  Common DTS Options
**
**	--debug <N>		set debug level
**	--help 			print task help
**	--noop 			no-op flag, print steps taken only
**	--verbose		verbose flag
**	--version 		print task version
**
**	-B 			use built-in configs
**	-c <file>		set DTSQ config file to be used
**	-q <queue>		submit to specified queue
**	-t <host>		name/IP address of target DTS host
**	-w <path>		set directory to DTSQ working directory
**
**	-H <port>		set hi transfer port
**	-L <port>		set low transfer port
**	-N <NThreads>		set number of transfer threads
**	-P <port>		set communications port
**
**	<path>			full path to file to queue
**
**
**  @file       dtsq.c
**  @author     Mike Fitzpatrick, NOAO
**  @date       July 2010
*/

#define _GNU_SOURCE
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "dts.h"
#include "dtsMethods.h"
#include "dtsClient.h"
#include "dtsPSock.h"
#include "build.h"


#define	 DTS_DEBUG	    0


#define  APP_NAME	    "dtsq"
#define  APP_VERSION	    "1.1"

#define  MAXPORTS	    256			/* max ports to open	      */
#define  DQ_NTHREADS	    4			/* default xfer threads	      */

#define  DQ_ERROR	    -1			/* error reading status       */
#define  DQ_READY	    0			/* queue is ready 	      */
#define  DQ_ACTIVE	    1			/* currently active	      */
#define  DQ_INIT	    2			/* initializing		      */
#define  DQ_XFER	    3			/* transferring		      */
#define  DQ_DONE	    4			/* complete		      */

#define  DQ_MINUTES	    0			/* (m) minutes unit	      */
#define  DQ_HOURS	    1			/* (h) hours unit	      */
#define  DQ_DAYS	    2			/* (d) days unit	      */
#define  DQ_WEEKS	    3			/* (w) weeks unit	      */

#define  DQ_PROCESS	    0			/* queue the object	      */
#define  DQ_LIST	    1			/* list queued objects	      */
#define  DQ_VALIDATE	    2			/* validate transfers	      */
#define  DQ_RECOVER	    3			/* recover failed queues      */
#define  DQ_LISTRECOVER	    4			/* list files to be recovered */


char	qhost[SZ_LINE];				/* queue host		      */
char	qmethod[SZ_LINE];			/* transfer method	      */
char   	config[SZ_PATH];			/* config file name	      */
char	qroot[SZ_PATH];				/* spool root dir	      */
char	qdir[SZ_PATH];				/* working dir		      */
char	rdir[SZ_PATH];				/* recovery dir		      */
char	tdir[SZ_PATH];				/* token dir		      */
char   	*queue		= NULL;			/* queue name		      */
char   	*tframe		= NULL;			/* list time frame	      */
char   	*env_var	= NULL;			/* envget result	      */
char   	*fname		= NULL;			/* name of file to transfer   */

int	status		= OK;			/* return status	      */
int     nfiles    	= 0;			/* no. of files to transfer   */
long    fsize    	= 0;			/* file size		      */
int	method		= TM_PSOCK;		/* transfer method	      */
int	qstat		= 0;			/* queue status		      */
int	nThreads	= DQ_NTHREADS;		/* no. transfer threads       */
int	port		= DTSQ_PORT;		/* application port	      */
int	hiPort		= DTSQ_PORT - 1;	/* hi transfer port	      */
int	loPort		= (DTSQ_PORT - 1 - DEF_MAX_PORTS);/* lo transfer port */
/*int	procSem		= 0;			   processing semaphore       */

/* Options
*/    
int	debug    	= FALSE;		/* debug flag		      */
int	verbose  	= FALSE;		/* verbose flag		      */
int	noop  		= FALSE;		/* no-op flag		      */
int	do_allhosts 	= FALSE;		/* all hosts flag	      */
int	do_checksum 	= TRUE;			/* checksum flag	      */
int	do_fork 	= FALSE;		/* fork to transfer	      */
int	do_give 	= TRUE;			/* use GIVE method	      */

int	rate		= 0;			/* UDT transfer rate 	      */
int	bundle		= 0;			/* bundle all files	      */
int	recover		= 0;			/* recover flag		      */
int	validate	= 0;			/* validate flag	      */
int	action		= DQ_PROCESS;		/* process an object	      */

FILE   *in 		= (FILE *) NULL;
DTS    *dts		= (DTS *) NULL;

char   *clientUrl;
char    localIP[SZ_PATH];
char    queuePath[SZ_LINE];
char   *xArgs[2];
char   *cwd = NULL;
char   *qnames[128];
int	nqueues = 0;

char   *sv_argv[2048];				/* saved cmdline args	      */
int	sv_argc = 0;

xferStat     xfs;				/* transfer stats	      */
Control	     control;				/* control params	      */
time_t 	     epoch	= (time_t) 0;


static char *dts_cfgKey (char *line);
static char *dts_cfgVal (char *line);

static void  dts_qInitControl (char *qhost, char *fname);
static void  dts_qUsage (void);
static void  dts_qLoadConfig (char *cfg, char **queue, char *host, char *qroot);
static void  dts_qInitWorkingDir (char *qdir);
static void  dts_qServerMethods (DTS *dts);
static void  dts_qSaveArglist (char *reason);
static void  dts_qSaveForRecovery (char *path, char *reason);
static void  dts_qSetStatus (char *qhost, char *qname, char *stat);
static void  dts_qLeaveToken (char *qhost, char *qname, char *fname,
				int stat, xferStat xfs);
static void  dts_qSetParam (char *nam, char *val); 
static void  dts_qParseParam (char *param); 
static void  dts_qPrintParams (void);
static void  dts_qFork (void);
static void  dts_dprintf (char *format, ...);
static char *dts_qArgs (void);
static void  dts_qParseRecover (char *line, char *host, char *path, 
				char *flags);

static void  dts_qInitArgs (void);
static int   dts_qParseArgs (int argc, char **argv);
static int   dts_qValidateDTS (char *fname);
static int   dts_qGetStatus (char *qname); 

static int   dts_qProcess (int sv_argc, char *sv_argv[]);
static int   dts_qProcessFile (char *fname);
static int   dts_qRecover (int action);
static int   dts_qList (char *timeframe);
static int   dts_qValidate (void);

static void  dts_qXferLogs (char *qhost, char *qname);


/**
 *  Task main().
 */
int 
main (int argc, char **argv)
{
    register int argno=1, j;
    char    fname[SZ_PATH], buf[SZ_PATH];
    int	    res = OK;


    /*  Initialize.
    */
    memset (qmethod,  0, SZ_LINE);
    memset (qhost,    0, SZ_LINE);
    memset (qroot,    0, SZ_PATH);
    memset (config,   0, SZ_PATH);
    memset (qdir,     0, SZ_PATH);
    memset (rdir,     0, SZ_PATH);
    memset (tdir,     0, SZ_PATH);
    memset (fname,    0, SZ_PATH);
    memset (localIP,  0, SZ_PATH);
    memset (buf,      0, SZ_PATH);
    memset (&control, 0, sizeof(Control));


    /*  Let environment variables define working/config information.  These
     *  can be overridden using cmdline flags, or defaults if neither are
     *  specified.
     */
    if ((env_var = getenv ("DTSQ_CONFIG")) != NULL)
        strcpy (config, env_var);
    if ((env_var = getenv ("DTSQ_WORKDIR")) != NULL)
        strcpy (qroot, env_var);


    /*  Parse the task arguments.  Note we do this again when recovering
     *  files to reinitialize the task each time.
     */
    dts_qInitArgs ();
    if (dts_qParseArgs (argc, argv) != OK)
	return (ERR);


    /*  Sanity checks.
     */
    if (!queue) {
	dts_dprintf ("Warning: No queue specified, using 'default'\n");
	queue = "default";
    } else if (strncasecmp ("null", queue, 4) == 0) {
	dts_dprintf ("INFO: Using 'null' queue\n");	/* no-op call       */
	return (OK);
    }


    /*  Load the configuration file and initialize the working directory.  We
     *  do it here because the config is used to map a queue name to the host.
     */
    dts_qLoadConfig (config, &queue, qhost, qroot);
    dts_qInitWorkingDir (qdir);

    /*  Get an open server port.  We need to do this in case there is an
     *  existing DTSQ already running on the selected port, if we exceed
     *  the max port range just save the files for later recovery and issue
     *  an ERR return.
     */
    if ((port = dts_getOpenPort (port, MAXPORTS)) < 0) {
	if (action == DQ_PROCESS) {
    	    dts_dprintf ("Error: Cannot get local DTSQ server  port\n");
	    dts_qSaveArglist ("Too many DTSQ processes\n");
	    return (ERR);
	}
    }


    /*  Initialize the DTS interface.  Use dummy args since we aren't setting
    **  up to be a DTS server process, but this allows us to load the config
    **  file and initialize the 'dts' struct with local information.
    */
    strcpy (localIP, dts_getLocalIP());
    cwd = getcwd (buf, (size_t) SZ_PATH);   	/* get current working dir */


    if (!qhost[0]) { 			/* setup some reasonable defaults     */
	dts_dprintf ("Warning: No host specified, using 'localhost'\n");
	strcpy (qhost, "localhost");
    }

    if ( (dts = dtsInit (NULL, port, localIP, "./", 0)) == (DTS *) NULL) {
	dts_dprintf ("Error: Cannot initialize task.\n");
	return (ERR);
    }
    dts->debug = debug;
    dts->verbose = verbose;


    if (action == DQ_PROCESS || action == DQ_RECOVER) {

        /*  Zero-level test:  If the queue is in an error state, immediately
         *  just save the requested files until manually recovered.  This
	 *  enforces the ordered delivery on the queue.
         */
        if (dts_qGetStatus (queue) == DQ_ERROR) {
    	    dts_dprintf ("Error: Queue '%s' requires manual recovery\n", queue);
	    dts_qSaveArglist ("Queue requires manual recovery");
    	    dts_qSetStatus (qhost, queue, "error");		/* reset */
	    return (ERR);
        }

        /*  First test:  Can we contact the DTS server at all, if not,
         *  save the requested files for later recovery.
         */
        if (debug)
	    fprintf (stderr, "Pinging host '%s' ....", qhost);
        if (!noop && dts_hostPing (qhost) != OK) {
    	    dts_dprintf ("Error: Cannot contact DTS host '%s'\n", qhost);
	    dts_qSaveArglist ("Cannot contact DTS host");
    	    dts_qSetStatus (qhost, queue, "error");		/* reset */
	    return (ERR);
        }
        if (debug)
	    fprintf (stderr, "OK\n");

        /*   Set any debug or verbose params in the server as well.
         */
        if (verbose) dts_hostSet (qhost, "dts", "verbose", "1");
        if (debug)   dts_hostSet (qhost, "dts", "debug",   "1");

        /*  Fork off the real work if things are time ctitical.
        */
        if (!noop && do_fork)
	    dts_qFork ();


        /*  Initialize the DTS server methods we'll need.  Note, this isn't a 
         *  complete server command set, only those we need to support the 
         *  commands we can send (e.g. push/pull).
         */
        if (!noop)
            dts_qServerMethods (dts);
    }


    /*  Do only what we were asked to do.
    */
    switch (action) {
    case     DQ_RECOVER:  res = dts_qRecover (action);            return (OK);
    case DQ_LISTRECOVER:  res = dts_qRecover (action);            return (OK);
    case    DQ_VALIDATE:  res = dts_qValidate (); 	          return (OK);
    case        DQ_LIST:  res = dts_qList (tframe); 	          return (OK);
    case     DQ_PROCESS:  res = dts_qProcess (sv_argc, sv_argv);  break;
    default:
	fprintf (stderr, "Invalid action request.\n");
	res = ERR;
    }


    /* Clean up and quit.
    */
    for (argno=0; argno < argc; argno++)	/* save the argument list   */
	free ((void *) sv_argv[argno]);
    for (j=0; j < nqueues; j++)
	free ((void *) qnames[j]);
    dtsFree (dts);

    return (res);
}


/**
 *  DTS_QINITARGS -- Initialize the task options.
 */
static void
dts_qInitArgs ()
{
    memset (qhost, 0, SZ_LINE);
    memset (qmethod, 0, SZ_LINE);
    memset (config, 0, SZ_PATH);
    memset (qroot, 0, SZ_PATH);
    memset (qdir, 0, SZ_PATH);
    memset (rdir, 0, SZ_PATH);
    memset (tdir, 0, SZ_PATH);

    queue	 = NULL;		/* global variable		*/
    tframe	 = NULL;
    env_var	 = NULL;
    fname	 = NULL;
    status	 = OK;
    nfiles    	 = 0;
    fsize    	 = 0;
    method	 = TM_PSOCK;
    qstat	 = 0;

    debug        = FALSE;		/* task options			*/
    verbose      = FALSE;
    noop         = FALSE;
    do_allhosts  = FALSE;
    do_checksum  = TRUE;
    do_fork      = FALSE;
    do_give      = TRUE;

    rate         = 0;
    bundle       = 0;
    recover      = 0;
    validate     = 0;
    action       = DQ_PROCESS;
}


/**
 *  DTS_QPARSEARGS -- Parse the task arguments.
 */
static int
dts_qParseArgs (int argc, char **argv)
{
    int  len = 0, j, argno = 0;


    memset (sv_argv, 0, (sizeof(char *) * 2048));
    for (argno=0; argno < argc; argno++)	/* save the argument list   */
	sv_argv[argno] = strdup (argv[argno]);
    sv_argc = argc;

    /* Process command-line arguments, and save them.
    */
    for (argno=1; argno < argc; argno++) {
        if (argv[argno][0] == '-') {
	    len = strlen (argv[argno]);
            for (j=1; j < len; j++) {
                switch ( argv[argno][j] ) {

                case 'a':   do_allhosts++; 				break;
                case 'b':   bundle++; 					break;
                case 'f':   do_fork++; 					break;
                case 'l':   tframe = argv[++argno], action = DQ_LIST;   break;
                case 'm':
		    strcpy (qmethod, argv[++argno]);	
		    do_give = (tolower(qmethod[0]) == 'g');
		    j += len;
		    break;
                case 'r':   action = DQ_LISTRECOVER;		        break;
                case 's':   do_checksum++; 				break;
                case 'u':   method = TM_UDT; 				break;

                case 'R':   action = DQ_RECOVER;		        break;
                case 'V':   action = DQ_VALIDATE;		        break;

                case 'p':
		    if (argv[argno][++j]) 	    /* set param=value pair */
		        dts_qParseParam (&argv[argno][j]);
		    else
		        dts_qParseParam (argv[++argno]);
		    j = len;
		    break;

		/* DTS Common Options.
		 */
                case 'B':   use_builtin_config = 1; 			break;
                case 'c':   strcpy (config, argv[++argno]);		break;
                case 'q':   queue = argv[++argno];			break;
                case 'w':   strcpy (qroot, argv[++argno]);		break;

                case 'P':   port     = atoi (argv[++argno]); 		break;
                case 'L':   loPort   = atoi (argv[++argno]); 		break;
                case 'H':   hiPort   = atoi (argv[++argno]); 		break;
                case 'N':   nThreads = atoi (argv[++argno]); 		break;
                case 't':   strcpy (qhost, argv[++argno]);		break;

                case '-':   
	    	    len = strlen (argv[argno]);
            	    for (j=2; j < len; j++) {
                	switch ( argv[argno][j] ) {
			case 'd':  debug++; 	   j=len;		break;
			case 'h':  dts_qUsage();   j=len;		break;
			case 'n':  noop++; 	   j=len;		break;
			case 'r':  rate = atoi (argv[++argno]); j=len;  break;
			case 'v':
		    	    if (strcmp (&(argv[argno][j+1]), "ersion") == 0) {
				printf ("Build Version: %s\n", build_date);
				return (OK);
		    	    } else
			        verbose++; 					
			    j=len;
			    break;
			default:
			    fprintf (stderr, "Invalid option '%s'\n",
				argv[argno]);
			    return (ERR);
			}
            	    }
		    break;
		   
                case 'v':   
		    if (strcmp (&(argv[argno][j+1]), "ersion") == 0) {
			printf ("Build Version: %s\n", build_date);
			return (OK);
		    } else
			verbose++; 					
		    break;

		default:
		    fprintf (stderr, "Unknown option '%c'\n", argv[argno][j]);
		    break;
		}
	    }

        } else if (argv[argno][0] == '+') {	/* inverse toggles	*/
	    len = strlen (argv[argno]);
            for (j=1; j < len; j++) {
                switch ( argv[argno][j] ) {
                case 'd':   debug++; 					break;
                case 'v':   verbose++; 					break;
                case 'f':   do_fork = 0; 				break;
                case 'g':   do_give = 0; 				break;
                case 's':   do_checksum--; 				break;
		default:						break;
		}
	    }

	} else if (strchr (argv[argno], (int) '=')) {
	    /*  A cmdline param=value with no '-p' flag.
	     */
	    dts_qParseParam (argv[argno]);

	} else 
	    break;
    }

    return (OK);
}


/**
 *  DTS_QVALIDATEDTS -- Validate the DTS connection.
 */
static int
dts_qValidateDTS (char *fname)
{
    int    res = OK;


    if (noop)
	return (OK);


    /*  FIXME -- Need to decide what to do here....
    if ((qstat = dts_qGetStatus (queue)) != DQ_READY) {
	fprintf (stderr, "Warning: queue '%s' already active.\n", queue);
	return (ERR);
    }
    */


    /* Check that we can operate as needed.   Tests performed:
     *
     *  0) Did we get a valid file or queue name?  (above) 
     *	1) Can we contact the DTS?
     *  3) Can the DTS handle a file of the given size?
     *	2) Is the queue name valid?
     *  4) Is the queue active?
     */

    /*  First test:  Can we contact the DTS server at all, if not,
     *  save then request to the recovery directory.
     */
    if (dts_hostPing (qhost) != OK) {
    	dts_dprintf ("Error: Cannot contact DTS host '%s'\n", qhost);
	dts_qSaveForRecovery (fname, "Cannot contact DTS host");
    	dts_qSetStatus (qhost, queue, "error");		/* reset */
	return (ERR);
    }

    /*  Second test:  Ask the DTS if it is willing to accept the 
     *  file.  If the return value is OK, the response string will 
     *  be the directory to which we should transfer the file. On 
     *  ERR, this will be a message string indicating the error.
     *  Error returns can be generated if the queue name is in-
     *  valid, a lack of disk space, a disabled queue, etc. (the 
     *  error message will indicate the cause).
     */ 
    memset (queuePath, 0, SZ_LINE);

    if (dts_hostInitTransfer (qhost, queue, fname, queuePath) != OK) {
    	dts_dprintf ("Init failure: %s\n", queuePath);
	dts_qSaveForRecovery (fname, queuePath);
    	dts_qSetStatus (qhost, queue, "error");		/* reset */
	return (ERR);
    }


    /*  If we made it this far, we can talk to the DTS, so initialize
     *  the control file to be used. 
     */
    dts_qInitControl (qhost, fname);

    return (res);
}


/**
 *  DTS_QINITCONTROL -- Initialize the control structure.
 */
static void
dts_qInitControl (char *qhost, char *fname)
{
    char *md5, opath[SZ_PATH];


    dts_qSetStatus (qhost, queue, "initializing");

    memset (opath, 0, SZ_PATH);
    sprintf (opath, "%s!%s", dts_getLocalHost(), fname);

    /*  Initialize the queue control file on the target machine.
     */
    strcpy (control.queueHost, dts_getLocalHost());
    strcpy (control.queueName, queue);
    strcpy (control.filename, dts_pathFname (fname));
    strcpy (control.xferName, dts_pathFname (fname));
    strcpy (control.srcPath, dts_pathDir (fname));
    strcpy (control.igstPath, opath);

    if ((md5 = dts_fileMD5 (fname))) {
        strcpy (control.md5, md5);
        free ((void *) md5);
    } else
        strcpy (control.md5, " \0");

    control.epoch = time (NULL);
    control.isDir = dts_isDir (fname);

    if (control.isDir == 1) {
        control.sum32 = control.crc32 = 0;
    } else {
        control.sum32 = dts_fileCRCChecksum (fname, &control.crc32);
    }
    control.fsize = dts_du (fname);

    /*  Call the method.
     */
    dts_hostSetQueueControl (qhost, queuePath, &control);
}


/**
 *  DTS_QPROCESS -- Process a list of files.
 */
static int
dts_qProcess (int sv_argc, char *sv_argv[])
{
    char   fname[SZ_PATH];
    int	   isDir = 0, stat = OK, fnum = 0;
    int    argno = OK;
    int    res = OK;


    /* Initialize the processing semaphore.
    if (procSem)
        dts_semRemove (procSem);
    procSem = dts_semInit ((int)getpid(), 0);
    */

    /*  Re-process the command-line arguments, but look only for 
     *  filenames.
     */
    fnum = 0;
    for (argno=1; argno < sv_argc; argno++) {
	/*
	dts_semSetVal (procSem, 0);
	*/

        if ((sv_argv[argno][0] == '-') || (sv_argv[argno][0] == '+')) {
	    if (strchr ("PLHNtcwlpq", (int)sv_argv[argno][1]))
		argno++;
	    continue;
	} else if (strchr (sv_argv[argno], (int)'=')) {
	    continue;
	}

    	memset (fname, 0, SZ_PATH);

	if (bundle) {
	    fprintf (stderr, "Bundling Not Yet Implemented: '%s'\n",
		sv_argv[argno++]);

	} else {
	    /*  The only non-flag arguments are the files to be queued.
	     *  If we have an absolute path, use it.  Otherwise construct
	     *  the path from the cwd.
	     */
	    if (sv_argv[argno][0] == '/')
	        strcpy (fname, sv_argv[argno]);
	    else
		sprintf (fname, "%s/%s", cwd, sv_argv[argno]);

	    /* Sanity Checks.
	     */
	    if (access (fname, R_OK) < 0) {
		dts_dprintf ("Error:  Cannot access file '%s'.\n", fname);
	    	dts_qSaveForRecovery (fname, "Cannot access file.");
		res += ERR;
		continue;
	    } else
		isDir = dts_isDir (fname);


    	    /*  Initialize the queue status and validate our connection 
	     *  to the DTS before processing.
	     */ 
	    dts_dprintf ("Processing file: %s\n", fname);
    	    dts_qSetStatus (qhost, queue, "active");

	     if (dts_qValidateDTS (fname) == OK) {
		/*  Initialize the active processing semaphore.
		dts_semSetVal (procSem, 1);
		 */

		if ((res += dts_qProcessFile (fname)) == OK) {

		    /* Block until the transfer is complete.
		    int  count = 0;

    		    while ((count = dts_semDecr (procSem)) >= 0) {
		        if (debug)
			    fprintf (stderr, "sem sleeping ....\n");
			sleep (1);
		    }
		    */

		    if (!noop)
		        stat = dts_hostEndTransfer(qhost, queue, queuePath);

		} else {
		    dts_dprintf ("Error:  Cannot transfer file '%s'.\n", fname);
	    	    dts_qSaveForRecovery (fname, "Cannot transfer file.");
		}
	    } else {
		dts_dprintf ("Error:  Cannot validate DTS '%s'.\n", fname);
	    	dts_qSaveForRecovery (fname, "Cannot access DTS.");
	    }
    	    dts_qSetStatus (qhost, queue, "ready");
	}
	fnum++;

	/*   FIXME
	 */
	sleep (1);
    }

    return (res);
}


/**
 *  DTS_QPROCESSFILE -- Process a file to submit it to the named queue.
 */
static int
dts_qProcessFile (char *fname)
{
    int    res = OK;


    /*  Log the command if possible.
    */
#ifdef DTSQ_LOG
    char   log_msg[SZ_LINE];

    memset (log_msg, 0, SZ_LINE);
    sprintf (log_msg, "%s: [DTSQ]  file = %s\n", localIP, fname);
    dtsLog (dts, log_msg);
#endif


    /* Finally, transfer the file.
    */
    xArgs[0] = calloc (1, SZ_PATH);
    xArgs[1] = calloc (1, SZ_PATH);
    sprintf (xArgs[0], "%s:%s", dts_getLocalIP(), fname);
    sprintf (xArgs[1], "%s:%s%s", qhost, queuePath, dts_pathFname(fname)); 
	
    if (debug) {
	fprintf (stderr, "xArgs[0] = '%s'\n", xArgs[0]);
	fprintf (stderr, "xArgs[1] = '%s'\n", xArgs[1]);
	fprintf (stderr, "port = %d  lo/hi = %d / %d  nthreads = %d\n",
	    port, loPort, hiPort, nThreads);
    }
	
    dts_qSetStatus (qhost, queue, "transferring");


    if (do_give) {
        res = (noop ? OK : dts_hostTo (qhost, port, method, rate, 
				loPort, hiPort,
				nThreads, XFER_PUSH, 2, xArgs, &xfs));
    } else {
        res = (noop ? OK : dts_hostTo (qhost, port, method, rate, 
				loPort, hiPort,
				nThreads, XFER_PULL, 2, xArgs, &xfs));
    }

    if (res != OK) {
        dts_dprintf ("Transfer to DTS failed.\n");
	dts_qSaveForRecovery (fname, "Transfer to DTS failed");
        return (ERR);

    } else if (verbose) {
        printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes\n",
            (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
            xfs.tput_MB, ((double)xfs.fsize/MBYTE));
    }
    
    dts_qSetStatus (qhost, queue, "complete");

    /*  Update the transfer statistics.
     */
    if (!noop) {
        dts_hostUpStats (qhost, queue, &xfs);

        /* Leave a token indicating we've completed the transfer.
        */
        dts_qLeaveToken (qhost, queue, fname, res, xfs);

        /* Transfer Log file to DTSD host.
        */
	dts_qXferLogs (qhost, queue);
    }

    /* Clean up.  On success, return zero to $status.
    */
    free ((char *) xArgs[0]);
    free ((char *) xArgs[1]);

    dts_qSetStatus (qhost, queue, "ready");

    return (res);
}


/**
 *  DTS_QFORK -- Fork off the real work to a child process.
 */
static void
dts_qFork ()
{
    pid_t  pid;

    if ((pid = fork ()) < 0) {
        dts_dprintf ("Child fork fails.\n");
        dts_qSaveForRecovery (fname, "Child fork fails.");
        exit (ERR);

    } else if (pid > 0)
        exit (OK);                         /* parent exits                 */
}


/**
 *  DTS_QVALIDATE -- Validate that files were transferred successfully.
 */
static int
dts_qValidate (void)
{

    return (OK);
}


/**
 *  DTS_QLIST -- List files queued during the specified timeframe.
 */
static int
dts_qList (char *timeframe)
{

    return (OK);
}


/**
 *  DTS_QRECOVER -- Recover, or list files to be recovered.
 */
static int
dts_qRecover (int action)
{
    char   pend[SZ_PATH], line[SZ_LINE], tpath[SZ_PATH], curhost[SZ_PATH];
    char  *qname = queue;
    int    nfailed = 0, nrecover = 0, nq = 0, skip = 0;
    FILE   *fd, *tmp;


    memset (tpath, 0, SZ_PATH);
    memset (line, 0, SZ_LINE);
    memset (pend, 0, SZ_PATH);

    memset (curhost, 0, SZ_PATH);
    gethostname (curhost, (size_t) SZ_PATH);

    if (strncmp (queue, "all", 3) == 0) {
	int i;
	for (i=0; i < nqueues; i++) 
	    printf ("%d : '%s'\n", i, qnames[i]);
    }

    /*  Open the log file to record the failure.  The log is an ever-growing
     *  file we need to manually delete, or else use recovery mode to prune.
     */
again_:
    sprintf (pend, "%s/%s/Recover", qdir, qname);

    if (verbose || debug)
	fprintf (stderr, "# Recovered files in %s\n", pend);

    if ((fd = dts_fopen (pend, "r")) ) {
	char   *fp, host[SZ_LINE], path[SZ_LINE], flags[SZ_LINE];
	size_t  sz = SZ_LINE;

	if (action == DQ_RECOVER) {
	    /*  Open a temp file that saves those files we can't recover
	     *  now.  At the end, we'll move this back to the recovery file
	     *  so we can repeatedly run until all done.
	     */
	    sprintf (tpath, "/tmp/dtsq.%d", (int) getpid());
	    if ((tmp = dts_fopen (tpath, "w+")) == (FILE *) NULL) {
		if (verbose || debug)
		    fprintf (stderr, "Can't open tmp file '%s'\n", tpath);
		dts_fclose (fd);
		return (ERR);
	    } 
	}

	while (dtsGets (line, sz, fd)) {
	    fp = line;
	    dts_qParseRecover (line, host, path, flags);

	    if (action == DQ_LISTRECOVER) {
		if (do_allhosts || strcmp (curhost, host) == 0)
		    fprintf (stderr, "dtsq %s %s\n", flags, path);

	    } else if (action == DQ_RECOVER) {
		char  c_argv[SZ_LINE][SZ_LINE], *ip, *op;
		int   c_argc = 0;


		memset (&c_argv[0][0], 0, (sizeof(char) * SZ_LINE * SZ_LINE));
		strcpy (c_argv[0], "dtsq");
		for (ip=flags, c_argc=0; *ip; ) {
		    op = c_argv[++c_argc];
		    memset (op, 0, SZ_LINE);
		    for ( ; ip && *ip != ' ' && *ip; ip++)
			*op++ = *ip;
		    *op = '\0';
		    ip++;
		}
		strcpy (c_argv[c_argc++], path);

    		dts_qInitArgs ();
    		if (dts_qParseArgs (c_argc, (char **)c_argv) != OK)
		    return (ERR);

		/*  Skip recovery if we're not on the same host as the
		 *  original submission.
		 */
		skip = (!do_allhosts && strcmp (curhost, host) != 0);

		/*  Re-Submit the file to the queue.  
		 */
		if (verbose || debug)
		    fprintf (stderr, "Recovering %s ...", path);
		if (!skip && dts_qProcessFile (path) == OK) {
		    if (!noop)
		        dts_hostEndTransfer (qhost, queue, queuePath);
		    if (verbose || debug)
		        fprintf (stderr, "OK\n");
		    nrecover++;

		} else {
		    /* Save the file for later recovery.
		    */
		    char *fpath = dts_pathDir (path);

		    if (verbose || debug)
		        fprintf (stderr, "Error\n");

		    if (strcmp ("./", fpath) == 0)
	    		fprintf (tmp, "%s %s/%s %s\n", host, cwd, path, args);
		    else
	    		fprintf (tmp, "%s %s %s\n", host, path, args);
		    nfailed++;
		}
	    }
	    memset (line, 0, SZ_LINE);
	}

	dts_fclose (fd);
	if (action == DQ_RECOVER) {
	    dts_fclose (tmp);

	    dts_fileCopy (tpath, pend);		/* Will truncate if no errors */
	    unlink (tpath);
	}

        if (strncmp (queue, "all", 3) == 0 && nq < nqueues) {
	    qname = qnames[nq++];
	    goto again_;
        }
	return (nfailed);

    } else {
        if (strncmp (queue, "all", 3) == 0 && nq < nqueues) {
	    qname = qnames[nq++];
	    goto again_;
        } else
	    dts_dprintf ("Error: Cannot open recovery file '%s'\n", pend);
	return (ERR);
    }
}


/**
 *  DTS_QPARSERECOVER -- Parse the recover file line.
 *
 *  [Note:  We assume the output strings are all at least SZ_LINE big.]
 */
static void
dts_qParseRecover (char *line, char *host, char *path, char *flags)
{
    char *ip, *op;


    memset (host, 0, SZ_LINE);
    for (ip=line, op=host; *ip != ' '; ip++)
	*op++ = *ip;

    memset (path, 0, SZ_LINE);
    for (ip++, op=path; *ip != ' '; ip++)
	*op++ = *ip;

    memset (flags, 0, SZ_LINE);
    for (ip++, op=flags; *ip; ip++)
	*op++ = *ip;
}


/**
 *  DTS_QSAVEARGLIST -- Save all filenames in the arglist for recovery.
 */
static void
dts_qSaveArglist (char *reason)
{
    char fname[SZ_PATH], buf[SZ_PATH], *cwd;
    int  argno;


    cwd = getcwd (buf, (size_t) SZ_PATH);   	/* get current working dir */

    /*  Process the saved argument list.
     */
    for (argno=1; argno < sv_argc; argno++) {
        if ((sv_argv[argno][0] == '-') || (sv_argv[argno][0] == '+')) {
	    if (strchr ("PLHNtcwlpq", (int)sv_argv[argno][1]))
	        argno++;
	    continue;
	} else if (strchr (sv_argv[argno], (int)'=')) {
	    continue;
	}

    	memset (fname, 0, SZ_PATH); 		/* Get the filename        */
	if (sv_argv[argno][0] == '/')
	    strcpy (fname, sv_argv[argno]);
	else
	    sprintf (fname, "%s/%s", cwd, sv_argv[argno]);

	dts_qSaveForRecovery (fname, reason);	/* save it		   */
    }
}


/**
 *  DTS_QSAVEFORRECOVERY -- Save the request for later recovery.
 */
static void
dts_qSaveForRecovery (char *path, char *reason)
{
    char   *cwd, buf[SZ_PATH], host[SZ_PATH], *tstr = dts_UTTime();
    char   log_file[SZ_PATH], pending_file[SZ_PATH], *args = dts_qArgs();
    FILE   *fd;


    /* Get current working directory and a timestamp
    */
    memset (buf, 0, SZ_PATH);
    cwd = getcwd (buf, (size_t) SZ_PATH);

    memset (host, 0, SZ_PATH);
    gethostname (host, (size_t) SZ_PATH);

    /*  Open the log file to record the failure.  The log is an ever-growing
     *  file we need to manually delete, or else use recovery mode to prune.
     */
    memset (log_file, 0, SZ_PATH);
    sprintf (log_file, "%s/Log", rdir);

    memset (pending_file, 0, SZ_PATH);
    sprintf (pending_file, "%s/Recover", rdir);


    if ((fd = dts_fopen (log_file, "a")) ) {
	fprintf (fd, "ERR %s %s %s '%s' %s\n", tstr, host, path, args, reason);
	dts_fclose (fd);
    }
    if ((fd = dts_fopen (pending_file, "a")) ) {
	char *fpath = dts_pathDir (path);

	if (strcmp ("./", fpath) == 0)
	    fprintf (fd, "%s %s/%s %s\n", host, cwd, path, args);
	else
	    fprintf (fd, "%s %s %s\n", host, path, args);
	dts_fclose (fd);
    } else
	dts_dprintf ("Error: Cannot open recovery file\n");


    /*  Append to the offline file for transfer to the DTSD.
     */
    strcat (log_file, ".offline");
    strcat (pending_file, ".offline");

    if ((fd = dts_fopen (log_file, "a")) ) {
	fprintf (fd, "ERR %s %s %s '%s' %s\n", tstr, host, path, args, reason);
	dts_fclose (fd);
    }
    if ((fd = dts_fopen (pending_file, "a")) ) {
	char *fpath = dts_pathDir (path);

	if (strcmp ("./", fpath) == 0)
	    fprintf (fd, "%s %s/%s %s\n", host, cwd, path, args);
	else
	    fprintf (fd, "%s %s %s\n", host, path, args);
	dts_fclose (fd);
    } else
	dts_dprintf ("Error: Cannot open offline recovery file\n");

    dts_qSetStatus (fname, queue, reason);	/* reset status		*/
}


/**
 *  DTS_QARGS -- Get the options from the cmdline args.
 */
static char *
dts_qArgs (void)
{
    static char  cmdargs[SZ_PATH];
    int  argno = 0;


    memset (cmdargs, 0, SZ_PATH);
    for (argno=1; argno < sv_argc; argno++) {
        if ((sv_argv[argno][0] == '-')) {
            strcat (cmdargs, sv_argv[argno]);
            strcat (cmdargs, " ");
            if (strchr ("PLHNtcwlpq", (int)sv_argv[argno][1])) {
                argno++;
                strcat (cmdargs, sv_argv[argno]);
                strcat (cmdargs, " ");
	    }
        } else if (strchr (sv_argv[argno], (int) '=')) {
            strcat (cmdargs, sv_argv[argno]);
            strcat (cmdargs, " ");
        } else
            continue;
    }

    return (cmdargs);
}


/**
 *  DTS_QSETSTATUS -- Leave a status message in the $HOME/.dtsq dir.
 */
static void
dts_qSetStatus (char *qhost, char *qname, char *stat)
{
    char    tpath[SZ_PATH], *tstr = dts_UTTime();
    FILE   *fd;


    memset (tpath, 0, SZ_PATH);			/* token-file path	*/
    sprintf (tpath, "%s/%s/status", qdir, qname);

    if (access (tpath, W_OK) != 0)
        dts_makePath (tpath, FALSE);
    if ((fd = dts_fopen (tpath, "w+"))) {
	fprintf (fd, "%s %s\n", tstr, stat);
	dts_fclose (fd);
    }

    if (DTS_DEBUG)
	fprintf (stderr, "Queue status = %d\n", dts_qGetStatus (qname));
}


/**
 *  DTS_QGETSTATUS -- Get the status of the queue from the $HOME/.dtsq file.
 */
static int
dts_qGetStatus (char *qname)
{
    char    tpath[SZ_PATH];
    FILE   *fd;
    int	    status = DQ_READY;


    memset (tpath, 0, SZ_PATH);
    sprintf (tpath, "%s/%s/status", qdir, qname);

    if (access (tpath, R_OK) == 0) {
	char  tstr[SZ_PATH], fname[SZ_PATH], stat[SZ_PATH];
	char  *t = tstr, *f = fname, *s = stat;

	memset (stat, 0, SZ_PATH);
        if ((fd = dts_fopen (tpath, "r+"))) {
	    fscanf (fd, "%s %s %s\n", t, f, s);

	    if (strcmp (s, "active") == 0)    	 status = DQ_ACTIVE;
	    if (strcmp (s, "initializing") == 0) status = DQ_INIT;
	    if (strcmp (s, "transferring") == 0) status = DQ_XFER;
	    if (strcmp (s, "complete") == 0)     status = DQ_DONE;
	    if (strcmp (s, "ready") == 0)    	 status = DQ_READY;
	    if (strcmp (s, "error") == 0)    	 status = DQ_ERROR;
        }
	dts_fclose (fd);
    }

    return (status);
}

		        
/**
 *  DTS_QXFERLOGS -- Transfer the Recover and Log file to the DTSD.
 */
static void
dts_qXferLogs (char *qhost, char *qname)
{
    int   client = 0, res, fd;
    char *logged = NULL, *recovered = NULL;
    char  log_file[SZ_PATH], pending_file[SZ_PATH];


    /*  Open the offline log and recovery files.
     */
    memset (pending_file, 0, SZ_PATH);
    sprintf (pending_file, "%s/Recover.offline", rdir);
    if (access (pending_file, R_OK|W_OK) == 0) {
        if ((fd = dts_fileOpen (pending_file, O_RDWR)) ) {
	    size_t  nread = 0, 
	            len   = dts_fileSize (fd);

	    if (len) {
	        recovered = calloc (1, (size_t) len);
	        nread = dts_fileRead (fd, recovered, (int) len);
	    }
	    dts_fileClose (fd);
        }
    }

    memset (log_file, 0, SZ_PATH);
    sprintf (log_file, "%s/Log.offline", rdir);
    if (access (log_file, R_OK|W_OK) == 0) {
        if ((fd = dts_fileOpen (log_file, O_RDWR)) ) {
	    size_t  nread = 0, 
	            len   = dts_fileSize (fd);

	    if (len) {
	        logged = calloc (1, (size_t) len);
	        nread = dts_fileRead (fd, logged, (int) len);
	    }
	    dts_fileClose (fd);
        }
    }

    /* Make the service call.
    */
    res = dts_hostSubmitLogs (qhost, qname, logged, recovered);

    /*  Truncate the offline file if we've had a good transfer.
     */
    if (res == OK) {
	truncate (log_file, 0);
	truncate (pending_file, 0);
    }
    if (logged) 
	free ((void *) logged);
    if (recovered) 
	free ((void *) recovered);

    dts_closeClient (client);
}



/* ***************************************************************************
**   INTERNAL PROCEDURES.
** **************************************************************************/

/**
 *  DTS_PARSEPARAM -- Parse a parameter for the control file.
 */
static void
dts_qSetParam (char *nam, char *val)
{
    int npars = control.nparams;

    strcpy (control.params[npars].name, nam);
    strcpy (control.params[npars].value, val);
    control.nparams++;
}


/**
 *  DTS_PARSEPARAM -- Parse a parameter for the control file.
 */
static void
dts_qParseParam (char *param)
{
    char  *eq = strstr (param, (char *)"=");

    *eq = '\0';
    dts_qSetParam (param, ++eq);

    if (DTS_DEBUG)
	dts_qPrintParams ();
}


/**
 *  DTS_PRINTPARAM -- Print the control parameter list.
 */
static void
dts_qPrintParams ()
{
    int  i;

    fprintf (stderr, "Params:\n");
    for (i=0; i <= control.nparams; i++) 
	fprintf (stderr, "    %d: %s = %s\n", 
	    i, control.params[i].name, control.params[i].value);
}


/**
 *  DTS_QLEAVETOKEN -- Leave a token indicating the transfer is complete.
 */
static void
dts_qLeaveToken (char *qhost, char *qname, char *fname, int stat, xferStat xfs)
{
    char     tpath[SZ_PATH], lpath[SZ_PATH], *tstr = dts_UTTime();
    char     host[SZ_PATH];
    FILE    *fd;


    memset (tpath, 0, SZ_PATH);
    memset (lpath, 0, SZ_PATH);
    sprintf (tpath, "%s/%s", tdir, fname);
    sprintf (lpath, "%s/%s/Log", qdir, qname);

    memset (host,  0, SZ_PATH);
    gethostname (host, (size_t) SZ_PATH);

    dts_makePath (tpath, FALSE);
    if ((fd = dts_fopen (tpath, "a"))) {
	fprintf (fd, "Status:             %s\n", (stat == OK ? "OK" : "ERR"));
	fprintf (fd, "   Request UT:      %s\n", tstr);
	fprintf (fd, "   Completion UT:   %s\n", tstr);
	fprintf (fd, "   DTS Host:        %s\n", qhost);
	fprintf (fd, "   Queue Name:      %s\n", qname);
	fprintf (fd, "   Transfer Stats:  %.3f sec  %8.3f Mb/s  (%.3f MB/s)\n",
	    xfs.time, xfs.tput_mb, xfs.tput_MB);
	fprintf (fd, "\n");
	dts_fclose (fd);
    }

    /* Make a log entry.
    */
    if (access (lpath, R_OK|W_OK) != 0)
	creat (lpath, O_CREAT);
    if ((fd = dts_fopen (lpath, "a+"))) {
        fprintf (fd, "OK  %s %s %7.2f %7.2f %s\n",
            tstr, host, xfs.time, xfs.tput_mb, fname);
	dts_fclose (fd);
    }

    strcat (lpath, ".offline");
    if (access (lpath, R_OK|W_OK) != 0)
	creat (lpath, O_CREAT);
    if ((fd = dts_fopen (lpath, "a+"))) {
        fprintf (fd, "OK  %s %s %7.2f %7.2f %s\n",
            tstr, host, xfs.time, xfs.tput_mb, fname);
	dts_fclose (fd);
    }
}



/**
 *  DTS_QLOADCONFIG -- Load the config file.
 */
    
#define	 DTSQ_ENV 		"DTSQ_CONFIG"
#define	 DTSQ_DEF_CONFIG 	".dtsq_config"

static void
dts_qLoadConfig (char *cfg, char **qname, char *host, char *qroot)
{
    FILE  *fd = (FILE *) NULL;
    char  *cfg_file = NULL, *env = NULL, *key, *val, *queue = *qname;
    char   cfgpath[SZ_PATH], path[SZ_PATH];
    char   fhost[SZ_PATH], fqueue[SZ_PATH];
    int    found = 0, lnum = 0;
    static char line[SZ_LINE];


    memset (qdir, 0, SZ_PATH);
    memset (rdir, 0, SZ_PATH);
    memset (tdir, 0, SZ_PATH);
    memset (fhost, 0, SZ_PATH);
    memset (fqueue, 0, SZ_PATH);

    /*  Set defaults 
     */ 
    strcpy (qmethod, "give");
    if (host != qhost)
        strcpy (qhost, host);
    sprintf (qdir, "%s/.dtsq", (qroot[0] ? qroot : getenv ("HOME")));
    sprintf (rdir, "%s/.dtsq/%s", 
	(qroot[0] ? qroot : getenv ("HOME")), queue);
    sprintf (tdir, "%s/.dtsq/%s/tokens",  
	(qroot[0] ? qroot : getenv ("HOME")), queue);
       

    /*  If we're given a specific config file, use it.  Otherwise, look
     *  for an environment definition, the default config file and if
     *  none can be found, set default values.
     */
    memset (cfgpath, 0, SZ_PATH);
    sprintf (cfgpath, "%s/%s", getenv("HOME"), DTSQ_DEF_CONFIG);

    if (cfg[0])
	cfg_file = cfg;

    else if ( (env = getenv (DTSQ_ENV)) )
	cfg_file = env;

    else if (access (cfgpath, R_OK) == 0)
	cfg_file = cfgpath;

    else {
	if (verbose)
	    fprintf (stderr, "Warning: Using default config values.\n");
	return;
    }

    if (debug) fprintf (stderr, "Using cfg_file '%s'\n", cfg_file);


    if ((access (cfg_file, R_OK) != 0) ||
        (fd = dts_fopen (cfg_file, "r")) == (FILE *) NULL ) {
	    fprintf (stderr, "Error: cannot open DTSQ config file '%s'\n",
	        cfg_file);
	    exit (1);
    }

    /*  Now parse the config file.
     */
    memset (line, 0, SZ_LINE);
    while (dtsGets (line, SZ_LINE, fd)) {
	lnum++;

        /*  Skip comments and empty lines.
        */
        if (line[0] == '#' || line[0] == '\0')
            continue;

        if (found && strncmp (line, "queue", 5) == 0) 
	    break;

        /*  New contexts or global variables begin at the first column
        */
        key = dts_cfgKey (line);
        if (strncmp (key, "queue", 5) != 0)
            val = dts_cfgVal (line);

        if (strncmp (key, "name", 4) == 0) {
	    if (strcmp (queue, "default") == 0) {
		queue = *qname = strdup (val);
		found++;
	    } else if (strcmp (queue, val) == 0)
		found++;

        } else if (strncmp (key, "host", 4) == 0) {
	    if (found || strcmp (queue, "default") == 0)
		strcpy (qhost, val);

        } else if (strncmp (key, "method", 6) == 0) {
	    if (found) {
		strcpy (qmethod, val);
		do_give = (strcmp (qmethod, "give") == 0);
	    }

        } else if (strncmp (key, "nthreads", 6) == 0) {
	    /* Allow the cmdline to reset the number of threads.
	     */
	    if (found && nThreads <= 0)
		nThreads = atoi (val);

        } else if (strncmp (key, "serverPort", 10) == 0) {
	    if (found)
		port = atoi (val);

        } else if (strncmp (key, "port", 4) == 0) {
	    if (found)
		loPort = atoi (val);

        } else if (strncmp (key, "loPort", 6) == 0) {
	    if (found)
		loPort = atoi (val);

        } else if (strncmp (key, "hiPort", 6) == 0) {
	    if (found)
		hiPort = atoi (val);

        } else if (strncmp (key, "dir", 3) == 0) {
	    if (found) {
		if (strcmp (val, "default") != 0) {
		    strcpy  (qdir, val);
		    sprintf (rdir, "%s/%s/", val, queue);
		    sprintf (tdir, "%s/%s/tokens/", val, queue);
		} else {
		    strcat (rdir, "/");
		    strcat (tdir, "/");
		}
	    }
        }

        memset (line, 0, SZ_LINE);
    }

    /* Set up defaults for values not found.
     */
    if (nThreads <= 0)
	nThreads = DQ_NTHREADS;
    else 
	hiPort = loPort + nThreads - 1;

    if (hiPort == 0)
	hiPort = loPort + nThreads - 1;


    /* Create the queue working directories and log file.
    */
    sprintf (qdir, "%s/.dtsq", (qroot[0] ? qroot : getenv ("HOME")));

    sprintf (rdir, "%s/.dtsq/%s", 
	(qroot[0] ? qroot : getenv ("HOME")), queue);
    dts_makePath (rdir, TRUE);

    sprintf (tdir, "%s/.dtsq/%s/tokens",  
	(qroot[0] ? qroot : getenv ("HOME")), queue);
    dts_makePath (tdir, TRUE);

    memset (path, 0, SZ_PATH);
    sprintf (path, "%s/%s/Log", qdir, queue);
    if (access (path, R_OK|W_OK) < 0)
	creat (path, 0664);

    sprintf (path, "%s/%s/Recover", qdir, queue);
    if (access (path, R_OK|W_OK) < 0)
	creat (path, 0664);

    sprintf (path, "%s/%s/Log.offline", qdir, queue);
    if (access (path, R_OK|W_OK) < 0)
	creat (path, 0664);

    sprintf (path, "%s/%s/Recover.offline", qdir, queue);
    if (access (path, R_OK|W_OK) < 0)
	creat (path, 0664);

    if (debug) {
	fprintf (stderr, "qname   '%s'\n", queue);
	fprintf (stderr, "qhost   '%s'\n", qhost);
	fprintf (stderr, "qmethod '%s'\n", qmethod);
	fprintf (stderr, "nThreads %d \n", nThreads);
	fprintf (stderr, "port     %d \n", port);
	fprintf (stderr, "loPort   %d \n", loPort);
	fprintf (stderr, "hiPort   %d \n", hiPort);
	fprintf (stderr, "qdir    '%s'\n", qdir);
	fprintf (stderr, "rdir    '%s'\n", rdir);
	fprintf (stderr, "tdir    '%s'\n", tdir);
	fprintf (stderr, "\n");
    }

    /*  Rewing the config file and scan for a list of queues.
     */
    rewind (fd);
    memset (line, 0, SZ_LINE);
    memset (qnames, 0, (sizeof(char *) * MAX_QUEUES));
    while (dtsGets (line, SZ_LINE, fd)) {
        key = dts_cfgKey (line);
        if (strncmp (key, "queue", 5) != 0)
            val = dts_cfgVal (line);

        if (strncmp (key, "name", 4) == 0)
            qnames[nqueues++] = strdup (val);
    }

    /*  Clean up.
     */
    dts_fclose (fd);
}


/**
 *  DTS_INITWORKINGDIR -- Initialize the working directory.
 */
static void
dts_qInitWorkingDir (char *qdir)
{
    char  path[SZ_PATH];


    if (access (qdir, R_OK|W_OK|X_OK) != 0)
	mkdir (qdir, 0775);

    memset (path, 0, SZ_PATH);			/* default queue dir  	*/
    sprintf (path, "%s/default", qdir);
    if (access (path, R_OK|W_OK|X_OK) != 0)
	mkdir (path, 0775);

    sprintf (path, "%s/default/Log", qdir);               creat (path, 0664);
    sprintf (path, "%s/default/Recover", qdir);           creat (path, 0664);
    sprintf (path, "%s/default/Log.offline", qdir);       creat (path, 0664);
    sprintf (path, "%s/default/Recover.offline", qdir);   creat (path, 0664);

    memset (path, 0, SZ_PATH);
    sprintf (path, "%s/default/tokens", qdir);
    if (access (path, R_OK|W_OK|X_OK) != 0)
	mkdir (path, 0775);
}


/**
 *  DTS_QSERVERMETHODS -- Initialize the server-side of the DTSQ.  Create
 *  an XML-RPC server instance, define the commands and start the thread.
 */
static void
dts_qServerMethods (DTS *dts)
{
    if (dts->trace)
        fprintf (stderr, "Initializing Server Methods:\n");

    /*  Create the server and launch.  Note we never return from this
    **  and make no calls to other servers.
    */
    xr_createServer ("/RPC2", dts->serverPort, NULL);

    /*  Define the server transfer methods.
    */
    xr_addServerMethod ("xferPushFile",    dts_xferPushFile,    NULL);
    xr_addServerMethod ("xferPullFile",    dts_xferPullFile,    NULL);
    xr_addServerMethod ("receiveFile",     dts_xferReceiveFile, NULL);
    xr_addServerMethod ("sendFile",        dts_xferSendFile,    NULL);

    xr_addServerMethod ("pingstr",         dts_PingStr,    NULL);

    /* Start the server thread.
    */
    xr_startServerThread ();
}


/**
 *  DTS_GETKEY -- Get the keyword from the config line.  Ignore leading
 *  whitespace, the context is derived in the caller.  We also ignore any
 *  inline comments and return only the first word on the line.
 */
static char *
dts_cfgKey (char *line)
{
    static char key[SZ_FNAME];
    char   *ip = line, *op = key;


    memset (key, 0, SZ_FNAME);
    for (ip=line; *ip && isspace (*ip); ip++)   /* skip leading whitespace */
        ;

    while ( *ip && !isspace(*ip) )
        *op++ = *ip++;

    return (key);
}


/**
 *  DTS_GETVAL -- Get the value from the config line.  Ignore leading
 *  whitespace, the context is derived in the caller.  We also ignore any
 *  inline comments and return only the first word on the line.
 */
static char *
dts_cfgVal (char *line)
{
    static char val[SZ_LINE];
    char   *ip = line, *op = val;


    memset (val, 0, SZ_LINE);

    for (ip=line; *ip && isspace(*ip); ip++) ;  /* skip leading w/s       */
    while (!isspace(*ip++))  ;                  /* skip keyword           */
    while (isspace(*ip++))   ;                  /* skip intermediate w/s  */ 
    for (ip--; *ip && *ip != '#'; )
        *op++ = *ip++;

    while (isspace(*op))                        /* trim trailing w/s      */
        *op-- = '\0';

    return (val);
}


/**
 *  DTS_DPRINTF -- DTS debug printf.
 */
static void
dts_dprintf (char *format, ...)
{

    if (debug || verbose) {
	va_list  argp;
	char *buf;
	int   len;

        va_start (argp, format);
        buf = calloc (1, (4 * SZ_LINE) );
        (void) dts_encodeString (buf, format, &argp);
        va_end (argp);

        len = strlen (buf);                 /* ensure a newline             */
        if (strcmp ("\n", &buf[len-1]))
            strcat (buf, "\n");

        fprintf (stderr, "%s", buf); 

	free ((char *) buf);
    }
}


/**
 *  DTS_QUSAGE -- Print usage information
 */
static void
dts_qUsage (void)
{

fprintf (stderr, "\n\
  Usage:\n\
  \n\
      dtsq [-q <queue>] [-f] [-h <ip>] <path> [<path> ....] \n\
  \n\
      -b                      bundle all files for transfer\n\
      -f                      fork to do the data transfer\n\
      -m <method>             specify transfer method (push | give)\n\
      -p <param=value>        pass param/value pair\n\
      -s                      do file checksum\n\
      -u                      use UDT transfer mode\n\
\n\
      -R                      recover failed queue attempts\n\
      -V                      verify queue transfers\n\
\n\
      +f                      disable task fork\n\
      +s                      disable file checksum flag\n\
      +d                      debug flag\n\
\n\
  Common DTS Options\n\
\n\
      --debug <N>             set debug level\n\
      --help                  print task help\n\
      --noop                  no-op flag, print steps taken only\n\
      --verbose               verbose flag\n\
      --version               print task version\n\
\n\
      -c <file>               set DTSQ config file to be used\n\
      -q <queue>              submit to specified queue\n\
      -t <host>               name/IP address of target DTS host\n\
      -w <path>               set directory to DTSQ working directory\n\
\n\
      -H <port>               set hi transfer port\n\
      -L <port>               set low transfer port\n\
      -N <NThreads>           set number of transfer threads\n\
      -P <port>               set communications port\n\
\n\
      <path>                  full path to file to queue\n\
  \n\
\n\n");

}
