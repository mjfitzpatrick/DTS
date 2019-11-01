/* 
**  DTSQ -- Queue a data object for transport.  Our job here is to 
**  verify that we can communicate with the DTS and that it will 
**  accept the object.  We have an optional mode to fork off and work
**  in the background so we return quickly, we can also keep a 
**  record of the transfer locally as well as provide recovery for
**  failed transfers.
**
**  Usage:
**
**    dtsq [-q <queue>] [-f] [-h <ip>] <path>
**
**	-q <queue>		submit to specified queue
**	-f			fork to do the data transfer
**	-c <file>		set config file to be used
**	-d <path>		set directory to transfer/recovery dir
**	-t <host>		name/IP address of target DTS host
**	-m <method>		specify transfer method (push | give)
**	-p			use PUSH transfer method
**
**	--h 			print task help
**	--n 			no-op flag, print steps taken only
**	--v 			verbose flag
**
**	-H <port>		set hi transfer port
**	-L <port>		set low transfer port
**	-N <nThreads>		set number of transfer threads
**	-P <port>		set communications port
**
**	-R 			recover failed queue attempts
**	-V 			verify queue transfers
**
**	+f 			disable task fork
**	+s 			disable file checksum flag
**	+d 			debug flag
**
**	<path>			full path to file to queue
**
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
#include "dtsPSock.h"


#define  APP_NAME	"dtsq"
#define  APP_VERSION	"1.0"

#define  MBYTE		1048576.0		/* 1024 * 1024		      */
#define  MAXPORTS	256			/* max ports to open	      */
#define  NTHREADS	8			/* max ports to open	      */

#define  DQ_ERROR	-1			/* error reading status       */
#define  DQ_READY	0			/* queue is ready 	      */
#define  DQ_ACTIVE	1			/* currently active	      */
#define  DQ_INIT	2			/* initializing		      */
#define  DQ_XFER	3			/* transferring		      */
#define  DQ_DONE	4			/* complete		      */

#define  DQ_MINUTES	0			/* (m) minutes unit	      */
#define  DQ_HOURS	1			/* (h) hours unit	      */
#define  DQ_DAYS	2			/* (d) days unit	      */
#define  DQ_WEEKS	3			/* (w) weeks unit	      */

#define  DQ_PROCESS	0			/* queue the object	      */
#define  DQ_LIST	1			/* list queued objects	      */
#define  DQ_VALIDATE	2			/* validate transfers	      */
#define  DQ_RECOVER	3			/* recover failed queues      */
#define  DQ_LISTRECOVER	4			/* list files to be recovered */


char   	*config		= NULL;			/* config file name	      */
char   	*fname		= NULL;			/* file name		      */

char	qhost[SZ_LINE];				/* queue host		      */
char	qmethod[SZ_LINE];			/* transfer method	      */
char	qroot[SZ_PATH];				/* spool root dir	      */
char	qdir[SZ_PATH];				/* working dir		      */
char	rdir[SZ_PATH];				/* recovery dir		      */
char	tdir[SZ_PATH];				/* token dir		      */
char   	*queue		= NULL;			/* queue name		      */
char   	*tframe		= NULL;			/* list time frame	      */

int	status		= OK;			/* return status	      */
long    fsize    	= 0;			/* file size		      */
int	mode		= 0;			/* access mode		      */
int	qstat		= 0;			/* queue status		      */
int	nThreads	= NTHREADS;		/* no. transfer threads       */
int	port		= DTSQ_PORT;		/* application port	      */
int	loPort		= DEF_XFER_PORT;	/* low transfer port	      */
int	hiPort		= (DEF_XFER_PORT+DEF_MAX_PORTS);

/* Options
*/    
int	debug    	= FALSE;		/* debug flag		      */
int	verbose  	= FALSE;		/* verbose flag		      */
int	noop  		= FALSE;		/* no-op flag		      */
int	do_checksum 	= TRUE;			/* checksum flag	      */
int	do_fork 	= FALSE;		/* fork to transfer	      */
int	do_give 	= TRUE;			/* use GIVE mode	      */

int	recover		= 0;			/* recover flag		      */
int	validate	= 0;			/* validate flag	      */
int	action		= DQ_PROCESS;		/* process an object	      */


FILE   *in 		= (FILE *) NULL;
DTS    *dts		= (DTS *) NULL;

xferStat xfs;

char   *clientUrl;
char    localIP[SZ_PATH];
char	*xArgs[2];


static char *dts_cfgKey (char *line);
static char *dts_cfgVal (char *line);
static int   dts_qGetStatus (char *qname); 
static void  dts_qUsage (void);
static void  dts_qLoadConfig (char *cfg, char *queue, char *host, char *qroot);
static void  dts_qInitWorkingDir (char *qdir);
static void  dts_qServerMethods (DTS *dts);
static void  dts_qSaveForRecovery (char *path, char *reason);
static void  dts_qSetStatus (char *fname, char *qname, char *stat);
static void  dts_qLeaveToken (char *qhost, char *qname, char *fname,
			int stat, xferStat xfs);

static int   dts_qProcess (char *fname);
static int   dts_qRecover (int action);
static int   dts_qList (char *timeframe);
static int   dts_qValidate (void);



/**
 *  Task main().
 */
int 
main (int argc, char **argv)
{
    register int i, j, len;
    int	   res = OK, isDir = 0;
    char   fname[SZ_PATH], msg[SZ_LINE], log_msg[SZ_LINE], localIP[SZ_PATH];


    /*  Initialize.
    */
    memset (msg,     0, SZ_LINE);
    memset (log_msg, 0, SZ_LINE);
    memset (qmethod, 0, SZ_LINE);
    memset (qhost,   0, SZ_LINE);
    memset (qroot,   0, SZ_PATH);
    memset (qdir,    0, SZ_PATH);
    memset (rdir,    0, SZ_PATH);
    memset (tdir,    0, SZ_PATH);
    memset (fname,   0, SZ_PATH);


    /* Process command-line arguments.
    */
    for (i=1; i < argc; i++) {
        if ((argv[i][0] == '-')) {
	    len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                switch ( argv[i][j] ) {
                case 'h':   dts_qUsage ();			return (0);

                case 'f':   do_fork++; 				break;
                case 'n':   noop++; 				break;
                case 'p':   do_give = 0; 			break;
                case 'q':   queue = argv[++i];			break;
                case 's':   do_checksum++; 			break;
                case 'v':   verbose++; 				break;

                case 'P':   port     = atoi (argv[++i]); 	break;
                case 'L':   loPort   = atoi (argv[++i]); 	break;
                case 'H':   hiPort   = atoi (argv[++i]); 	break;
                case 'N':   nThreads = atoi (argv[++i]); 	break;
                case 'c':   config   = argv[++i]; 		break;
                case 'd':   strcpy (qroot, argv[++i]);		break;
                case 't':   strcpy (qhost, argv[++i]);		break;
                case 'm':
		    strcpy (qmethod, argv[++i]);	
		    do_give = (tolower(qmethod[0]) == 'g');
		    j += len;
		    break;

                case 'R':   action = DQ_RECOVER;		    break;
                case 'V':   action = DQ_VALIDATE;		    break;
                case 'r':   action = DQ_LISTRECOVER;		    break;
                case 'l':   tframe = argv[++i],	action = DQ_LIST;   break;

		default:
		    fprintf (stderr, "Unknown option '%c'\n", argv[i][j]);
		    break;
		}
	    }

        } else if ((argv[i][0] == '+')) {	/* inverse toggles	*/
	    len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                switch ( argv[i][j] ) {
                case 's':   do_checksum--; 			break;
                case 'f':   do_fork--; 				break;
                case 'g':   do_give--; 				break;

                case 'd':   debug++; 				break;
		default:					break;
		}
	    }

	} else {
	    /*  The only non-flag argument is the file to be queued.
	     *  If we have an absolute path, use it.  Otherwise construct
	     *  the path from the cwd.
	     */
	    if (argv[i][0] == '/') {
	        strcpy (fname, argv[i]);

	    } else {
    		char  *cwd, buf[SZ_PATH];

    		memset (buf, 0, SZ_PATH);   /* get current working directory */
    		cwd = getcwd (buf, (size_t) SZ_PATH);

		sprintf (fname, "%s/%s", cwd, argv[i]);
		isDir = dts_isDir (fname);
	    }
	}
    }


    /*  Sanity checks.
     */
    if (action == DQ_PROCESS) {
        if (fname == (char *) NULL) {
	    if (debug || verbose)
	        fprintf (stderr, "Error: No filename specified.\n");
	    dts_qUsage ();
	    return (ERR);
        } else if (access (fname, R_OK) < 0) {
	    if (debug || verbose)
	        fprintf (stderr, "Error:  Cannot access file '%s'.\n", fname);
	    return (ERR);
        }
    }

    if (!queue) {
	if (debug || verbose)
	    fprintf (stderr, "Warning: No queue specified, using 'default'\n");
	queue = "default";
    } else if (strncasecmp ("null", queue, 4) == 0) {
	if (debug || verbose)
	    fprintf (stderr, "INFO: Using 'null' queue\n");
	return (OK);
    }


    /*  Load the configuration file and initialize the working directory.  We
     *  do it here because the config is used to map a queue name to the host.
     */
    dts_qLoadConfig (config, queue, qhost, qroot);
    dts_qInitWorkingDir (qdir);

    /*  Do only what we were asked to do.
    */
    switch (action) {
    case     DQ_RECOVER:  dts_qRecover (action);	return (OK);
    case DQ_LISTRECOVER:  dts_qRecover (action); 	return (OK);
    case    DQ_VALIDATE:  dts_qValidate (); 		return (OK);
    case        DQ_LIST:  dts_qList (tframe); 		return (OK);
    }

    if ((qstat = dts_qGetStatus (queue)) != DQ_READY) {
	/*  FIXME -- Need to decide what to do here....
	*/
	fprintf (stderr, "Warning: queue '%s' already active.\n", queue);
	return (ERR);
    }

    /*  Initialize the queue status.
     */
    dts_qSetStatus (fname, queue, "active");

    /*  Setup some reasonable defaults.
    */
    if (!qhost[0]) {
	if (debug || verbose)
	    fprintf (stderr, "Warning: No host specified, using 'localhost'\n");
	strcpy (qhost, "localhost");
    }


    /* Check that we can operate as needed.   Tests performed:
     *
     *  0) Did we get a valid file or queue name?  (above) 
     *	1) Can we contact the DTS?
     *  3) Can the DTS handle a file of the given size?
     *	2) Is the queue name valid?
     *  4) Is the queue active?
     */

    
    /*  First test:  Can we contact the DTS server at all, if not save the
     *  request to the recovery directory.
     */
    if (dts_hostPing (qhost) != OK) {
	if (debug || verbose)
	    fprintf (stderr, "Error: Cannot contact DTS host '%s'\n", qhost);
	dts_qSaveForRecovery (fname, "Cannot contact DTS host");
	return (ERR);
    }

    /*  Second test:  Ask the DTS if it is willing to accept the file.
     *  If the return value is OK, the response string will be the directory
     *  to which we should transfer the file.  On ERR, this will be a
     *  message string indicating the error.  Error returns can be generated
     *  if the queue name is invalid, a lack of disk space, a disabled queue,
     *  etc. (the error message will indicate the cause).
     */ 
    if (dts_hostQueueAccept (qhost, queue, fname, msg) != OK) {
	if (debug || verbose)
	    fprintf (stderr, "%s\n", msg);
	dts_qSaveForRecovery (fname, msg);
    	dts_qSetStatus (fname, queue, "ready");		/* reset	*/
	return (ERR);
    }

     
    /*  Fork off the real work if thing are time ctitical.
    */
    if (do_fork) {
 	pid_t  pid;

        if ((pid = fork ()) < 0) {
	    if (debug || verbose)
                fprintf (stderr, "Child fork fails.\n");
	    dts_qSaveForRecovery (fname, "Child fork fails.");
            exit (1);
    
        } else if (pid > 0) {
            exit (0);                         /* parent exits                 */
        }
    }


    dts_qSetStatus (fname, queue, "initializing");

    /* Initialize the DTS interface.  Use dummy args since we aren't setting
    ** up to be a DTS server process, but this allows us to load the config
    ** file and initialize the 'dts' struct with local information.
    */
    memset (localIP, 0, SZ_PATH);
    strcpy (localIP, dts_getLocalIP());

    dts = dtsInit (config, port, localIP, "./", 0);
    dts->debug = debug;
    dts->verbose = verbose;


    /* Initialize the DTS server methods we'll need.  Note, this isn't a 
     * complete server command set, only those we need to support the 
     * commands we can send (e.g. push/pull).
     */
    dts_qServerMethods (dts);


    /*   Set any debug or verbose params in the server as well.
     */
    if (verbose) dts_hostSet (qhost, "dts", "verbose", "true");
    if (debug)   dts_hostSet (qhost, "dts", "debug",   "true");


    /*  Log the command if possible.
    */
    memset (log_msg,     0, SZ_LINE);
    sprintf (log_msg, "%s: [DTSQ]  file = %s\n", localIP, fname);
    dtsLog (dts, log_msg);


    /* Finally, transfer the file.
    */
    xArgs[0] = calloc (1, SZ_PATH);
    xArgs[1] = calloc (1, SZ_PATH);
    sprintf (xArgs[0], "%s:%s", dts_getLocalIP(), fname);
    sprintf (xArgs[1], "%s:%s%s", qhost, msg, dts_pathFname(fname)); 
	
    if (debug) {
	fprintf (stderr, "xArgs[0] = '%s'\n", xArgs[0]);
	fprintf (stderr, "xArgs[1] = '%s'\n", xArgs[1]);
    }

    dts_qSetStatus (fname, queue, "transferring");

    if (do_give) {
        res = (noop ? OK : dts_hostTo (qhost, port, loPort, hiPort,
				nThreads, XFER_PUSH, 2, xArgs, &xfs));
    } else {
        res = (noop ? OK : dts_hostTo (qhost, port, loPort, hiPort,
				nThreads, XFER_PULL, 2, xArgs, &xfs));
    }

    if (res != OK) {
	if (debug || verbose)
            fprintf (stderr, "Transfer to DTS failed.\n");
	dts_qSaveForRecovery (fname, "Transfer to DTS failed");
        return (ERR);

    } else if (verbose) {
        printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes\n",
            (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
            xfs.tput_MB, ((double)xfs.fsize/MBYTE));
    }
    
    dts_qSetStatus (fname, queue, "complete");

    /* Leave a token indicating we've completed the transfer.
    */
    dts_qLeaveToken (qhost, queue, fname, res, xfs);

    /* Clean up.  On success, return zero to $status.
    */

    free ((char *) xArgs[0]);
    free ((char *) xArgs[1]);
    dtsFree (dts);

    dts_qSetStatus (fname, queue, "ready");

    return (res);
}


/**
 *  DTS_QPROCESS -- Process a file to submit it to the named queue.
 */
static int
dts_qProcess (char *fname)
{
if (strstr (fname, "dtsq") == NULL) return (ERR);
    return (OK);
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
    char   pend[SZ_PATH], line[SZ_LINE], tpath[SZ_PATH];
    int    nfailed = 0, nrecover = 0;
    FILE   *fd, *tmp;


    memset (tpath, 0, SZ_PATH);
    memset (line, 0, SZ_LINE);
    memset (pend, 0, SZ_PATH);

    /*  Open the log file to record the failure.  The log is an ever-growing
     *  file we need to manually delete, or else use recovery mode to prune.
     */
    sprintf (pend, "%s/Pending", rdir);
    if ((fd = fopen (pend, "r")) ) {
	char   *fp, *rp;
	size_t  sz = SZ_LINE;

	if (action == DQ_RECOVER) {
	    /*  Open a temp file that saves those files we can't recover
	     *  now.  At the end, we'll move this back to the recovery file
	     *  so we can repeatedly run until all done.
	     */
	    sprintf (tpath, "/tmp/dtsq.%d", (int) getpid());
	    if ((tmp = fopen (tpath, "w+")) == (FILE *) NULL) {
		if (verbose || debug)
		    fprintf (stderr, "Can't open tmp file '%s'\n", tpath);
		fclose (fd);
		return (ERR);
	    } 
	}

	while (dtsGets (line, sz, fd)) {
	    fp = line;
	    for (rp=line; *rp != ' '; rp++) ;
	    *rp++ = '\0';

	    if (action == DQ_LISTRECOVER) {
		fprintf (stderr, "%s\n", (verbose ? line : fp) );

	    } else if (action == DQ_RECOVER) {
		/*  Re-Submit the file to the queue.  
		 */
		fprintf (stderr, "Recovering %s ...", fp);
		if (dts_qProcess (fp) == OK) {
		    fprintf (stderr, "OK\n");
		    nrecover++;
		} else {
		    /* Save the file for later recovery.
		    */
		    fprintf (stderr, "Error\n");
		    fprintf (tmp, "%s %s\n", fp,  "Failed recovery");
		    nfailed++;
		}
	    }
	    memset (line, 0, SZ_LINE);
	}

	fclose (fd);
	if (action == DQ_RECOVER) {
	    fclose (tmp);

	    dts_fileCopy (tpath, pend);		/* Will truncate if no errors */
	    unlink (tpath);
	}
	return (OK);

    } else {
	if (debug || verbose)
	    fprintf (stderr, "Error: Cannot open recovery file\n");
	return (ERR);
    }
}


/**
 *  DTS_QSAVEFORRECOVERY -- Save the request for later recovery.
 */
static void
dts_qSaveForRecovery (char *path, char *reason)
{
    time_t      t  = time (NULL);
    struct tm  *tm = localtime (&t);
    char   *cwd, buf[SZ_PATH], tstr[128];
    FILE   *fd;


    /* Get current working directory and a timestamp
    */
    memset (buf, 0, SZ_PATH);
    cwd = getcwd (buf, (size_t) SZ_PATH);

    memset (tstr, 0, 128);
    strftime (tstr, 128, "%FT%T", tm);


    /*  Open the log file to record the failure.  The log is an ever-growing
     *  file we need to manually delete, or else use recovery mode to prune.
     */
    chdir (rdir);
    if ((fd = fopen ("../Log", "a")) ) {
	fprintf (fd, "ERR %s %s %s\n", tstr, path, reason);
	fclose (fd);
    }
    if ((fd = fopen ("Pending", "a")) ) {
	char *fpath = dts_pathDir (path);

	if (strcmp ("./", fpath) == 0)
	    fprintf (fd, "%s/%s\n", cwd, path);
	else
	    fprintf (fd, "%s\n", path);
	fclose (fd);
	    
    } else {
	if (debug || verbose)
	    fprintf (stderr, "Error: Cannot open recovery file\n");
	return;
    }

    dts_qSetStatus (fname, queue, reason);	/* reset status		*/
}


/**
 *  DTS_QSETSTATUS -- Leave a status message in the queue dir.
 */
static void
dts_qSetStatus (char *fname, char *qname, char *stat)
{
    char    tpath[SZ_PATH], tstr[128];
    time_t      t  = time (NULL);
    struct  tm *tm = localtime (&t);
    FILE   *fd;


    memset (tpath, 0, SZ_PATH);			/* token-file path	*/
    sprintf (tpath, "%s/%s/status", qdir, qname);

    memset (tstr, 0, 128);			/* timestamp		*/
    strftime (tstr, 128, "%FT%T", tm);

    if (access (tpath, W_OK) != 0)
        dts_makePath (tpath, FALSE);
    if ((fd = fopen (tpath, "w+"))) {
	fprintf (fd, "%s %s %s\n", 
	    tstr, (strcmp ("ready", stat) == 0 ? " " : fname), stat);
	fclose (fd);
    }
}


/**
 *  DTS_QGETSTATUS -- Get the status of the queue.
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
        if ((fd = fopen (tpath, "r+"))) {
	    fscanf (fd, "%s %s %s\n", t, f, s);

	    if (strcmp (s, "active") == 0)    	 status = DQ_ACTIVE;
	    if (strcmp (s, "initializing") == 0) status = DQ_INIT;
	    if (strcmp (s, "transferring") == 0) status = DQ_XFER;
	    if (strcmp (s, "complete") == 0)     status = DQ_DONE;
	    if (strcmp (s, "ready") == 0)    	 status = DQ_READY;
        }
	fclose (fd);
    }

    return (status);
}


/**
 *  DTS_QLEAVETOKEN -- Leave a token indicating the transfer is complete.
 */
static void
dts_qLeaveToken (char *qhost, char *qname, char *fname, int stat, xferStat xfs)
{
    char    tpath[SZ_PATH], lpath[SZ_PATH], tstr[128];
    time_t      t  = time (NULL);
    struct  tm *tm = localtime (&t);
    FILE   *fd;


    memset (tstr, 0, 128);
    memset (tpath, 0, SZ_PATH);
    memset (lpath, 0, SZ_PATH);
    sprintf (tpath, "%s/%s", tdir, fname);
    sprintf (lpath, "%s/%s/Log", qdir, qname);
    strftime (tstr, 128, "%FT%T", tm);

    dts_makePath (tpath, FALSE);
    if ((fd = fopen (tpath, "w"))) {
	fprintf (fd, "Status:           %s\n", (stat == OK ? "OK" : "ERR"));
	fprintf (fd, "Completion Time:  %s\n", tstr);
	fprintf (fd, "DTS Host:         %s\n", qhost);
	fprintf (fd, "Queue Name:       %s\n", qname);
	fprintf (fd, "Transfer Stats:   %.3f sec  %8.3f Mb/s  (%.3f MB/s)\n",
	    xfs.time, xfs.tput_mb, xfs.tput_MB);
	fclose (fd);
    }

    /* Make a log entry.
    */
    if (access (lpath, W_OK) != 0)
	creat (lpath, 0644);
    if ((fd = fopen (lpath, "a"))) {
        fprintf (fd, "OK  %s %7.2f %7.2f %s\n",
            tstr, xfs.time, xfs.tput_mb, fname);
	fclose (fd);
    }
}



/**
 *  DTS_QLOADCONFIG -- Load the config file.
 */
    
#define	 DTSQ_ENV 		"DTSQ_CONFIG"
#define	 DTSQ_DEF_CONFIG 	".dtsq_config"

static void
dts_qLoadConfig (char *cfg, char *qname, char *host, char *qroot)
{
    FILE  *fd = (FILE *) NULL;
    char  *cfg_file = NULL, *env = NULL, *key, *val;
    int   found = 0, lnum = 0;
    char  cfgpath[SZ_PATH], path[SZ_PATH];
    static char line[SZ_LINE];



    memset (qdir, 0, SZ_PATH);
    memset (rdir, 0, SZ_PATH);
    memset (tdir, 0, SZ_PATH);


    /*  Set defaults 
     */ 
    strcpy (qmethod, "give");
    strcpy (qhost, host);
    sprintf (qdir, "%s/.dtsq", (qroot[0] ? qroot : getenv ("HOME")));
    sprintf (rdir, "%s/.dtsq/%s/recover", 
	(qroot[0] ? qroot : getenv ("HOME")), qname);
    sprintf (tdir, "%s/.dtsq/%s/tokens",  
	(qroot[0] ? qroot : getenv ("HOME")), qname);
       

    /*  If we're given a specific config file, use it.  Otherwise, look
     *  for an environment definition, the default config file and if
     *  none can be found, set default values.
     */
    memset (cfgpath, 0, SZ_PATH);
    sprintf (cfgpath, "%s/%s", getenv("HOME"), DTSQ_DEF_CONFIG);

    if (cfg)
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


    if ( (fd = fopen (cfg_file, "r")) == (FILE *) NULL ) {
	fprintf (stderr, "Error: cannot open config file '%s'\n", cfg_file);
	return;
    }


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
	    if (strcmp (qname, val) == 0)
		found++;

        } else if (strncmp (key, "host", 4) == 0) {
	    if (found)
		strcpy (qhost, val);

        } else if (strncmp (key, "method", 6) == 0) {
	    if (found) {
		strcpy (qmethod, val);
		do_give = (strcmp (qmethod, "give") == 0);
	    }

        } else if (strncmp (key, "nThreads", 6) == 0) {
	    if (found)
		nThreads = atoi (val);

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
		    sprintf (rdir, "%s/%s/recover/", val, qname);
		    sprintf (tdir, "%s/%s/tokens/", val, qname);
		} else {
		    strcat (rdir, "/");
		    strcat (tdir, "/");
		}
	    }

        }

        memset (line, 0, SZ_LINE);
    }


    /* Create the queue working directories and log file.
    */
    dts_makePath (rdir, TRUE);
	memset (path, 0, SZ_PATH);
        sprintf (path, "%s/Pending", rdir);
	if (access (path, F_OK) < 0)
            creat (path, 0644);
    dts_makePath (tdir, TRUE);

    memset (path, 0, SZ_PATH);
    sprintf (path, "%s/default/Log", qdir);
    creat (path, 0644);

    if (debug) {
	fprintf (stderr, "qname   '%s'\n", qname);
	fprintf (stderr, "qhost   '%s'\n", qhost);
	fprintf (stderr, "qmethod '%s'\n", qmethod);
	fprintf (stderr, "nThreads %d \n", nThreads);
	fprintf (stderr, "loPort   %d \n", loPort);
	fprintf (stderr, "hiPort   %d \n", hiPort);
	fprintf (stderr, "qdir    '%s'\n", qdir);
	fprintf (stderr, "rdir    '%s'\n", rdir);
	fprintf (stderr, "tdir    '%s'\n", tdir);
    }

    fclose (fd);
}



/**
 *  DTS_INITWORKINGDIR -- Initialize the working directory.
 */
static void
dts_qInitWorkingDir (char *qdir)
{
    char  path[SZ_PATH];


    if (access (qdir, R_OK|W_OK|X_OK) != 0)
	mkdir (qdir, 0755);

    memset (path, 0, SZ_PATH);			/* default queue dir  	*/
    sprintf (path, "%s/default", qdir);
    if (access (path, R_OK|W_OK|X_OK) != 0) {
	mkdir (path, 0755);

        sprintf (path, "%s/default/tokens/Log", qdir);
        creat (path, 0644);
    }

    memset (path, 0, SZ_PATH);
    sprintf (path, "%s/default/recover", qdir);
    if (access (path, R_OK|W_OK|X_OK) != 0) {
	mkdir (path, 0755);

        sprintf (path, "%s/default/recover/Pending", qdir);
	if (access (path, F_OK) < 0)
            creat (path, 0644);
    }

    memset (path, 0, SZ_PATH);
    sprintf (path, "%s/default/tokens", qdir);
    if (access (path, R_OK|W_OK|X_OK) != 0)
	mkdir (path, 0755);
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
 *  DTS_QUSAGE -- Print usage information
 */
static void
dts_qUsage (void)
{
}
