/**
 *  DTSCMD -- Utility app to send a command to a DTS daemon on a remote
 *  host.  Note we only support a subset of commands that might be useful
 *  in a scripting environment, the DTSMON as a console has full access to
 *  all the DTSD functionality.
 *	The first argument is always the host to be contacted, either a
 *  direct IP address or a DTS logical name if a local config file can be
 *  found.  The second argument is the name of the command followed by 
 *  whichever arguments that command requires.
 *
 *  Usage:
 *      dtscmd [options] <cmd> <arg> [<arg> ...]
 *
 *  Options:
 *      -d			      debug flag
 *      -n			      no-op flag
 *      -q			      quiet - suppress output flag
 *      -c [fname]		      set config file
 *      -t [name]		      target host flag
 *      -v			      verbose output flag
 *
 *  Commands:
 *      access <path>		      test file access
 *      cat <file>		      concatenate a file 
 *      cfg			      get DTS config n target
 *      cwd			      print current working directory
 *      del [-r] <pattern> [passwd]   delete files matching patter
 *      dir [-l] [<pattern>]   	      list filenames matching patter
 *      df			      return free disk (KB)
 *      du			      return used disk (KB)
 *      echo  <str>		      echo a string
 *      fsize <path>		      print file size
 *      ftime <path> [a|m|c]	      print file <mode> time
 *      ls [-l] [<pattern>]	      list directory contents
 *      mkdir <path>		      make a directory
 *      mv <old> <new>		      rename a file
 *      ping 			      ping a host
 *      prealloc <path> <nbytes>      preallocate file space
 *      pwd			      print current working directory
 *      rename <old> <new>	      rename a file
 *      rm <pattern>		      delete files matching patter
 *      stat <path>		      print fille stat() of file
 *      touch <path>		      touch a file's access time
 *
 *
 *  @file       dtscmd.c
 *  @author     Mike FItzpatrick
 *  @date       6/15/09
 */


#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include <time.h>

#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "dts.h"
#include "dtsMethods.h"


#define	 min(a,b)	"(a<b?a:b)"
#define	 max(a,b)	"(a>b?a:b)"

#define  APP_NAME	"dtscmd"
#define  APP_VERSION	"1.0"

#define  MBYTE		1048576			/* 1024 * 1024		*/


char	*host 		= NULL;			/* target host		*/
char	*cmd 		= NULL;			/* command		*/
char   	*path 		= NULL;			/* path name		*/
char   	*config		= NULL;			/* config file name	*/
long     fsize    	= 0;			/* file size		*/
int	 mode		= 0;			/* access mode		*/

/* Options
*/    
int	debug    	= 0;			/* debug flag		*/
int	verbose  	= 0;			/* verbose flag		*/
int	quiet  		= 0;			/* quiet flag		*/
int	noop  		= 0;			/* no-op flag		*/

DTS    *dts		= (DTS *) NULL;
char   *clientUrl;


static void  Usage (void);
static void  dts_cmdServerMethods (DTS *dts);



/*  Usage:
**
**    dtscmd <host> <cmd> [arg] [arg] [...]
*/
int 
main (int argc, char **argv)
{
    register int i, j, len;
    int	     astart = 1, res = OK, port = DTSSH_PORT;
    long     size;
    char    *sres = (char *) NULL, ch;
    char    *old = (char *) NULL, *new = (char *) NULL;
    char     msg[SZ_LINE], localIP[SZ_PATH];
    xferStat xfs;



    /* Process command-line arguments.
    */
    for (i=1; i < argc; i++) {
        if (argv[i][0] == '-' && !(isdigit(argv[i][1]))) {
	    len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                switch ( argv[i][j] ) {
                case 'd':   debug++; 			break;
                case 'h':   Usage (); 			return (0);
                case 'n':   noop++; 			break;
                case 'q':   quiet++; 			break;
                case 'c':   config = argv[++i]; 	break;
                case 't':   host = argv[++i]; 		break;
                case 'p':   port = atoi (argv[++i]);	break;
                case 'v':   verbose++; 			break;
		default:
		    fprintf (stderr, "Unknown option '%c'\n", argv[i][j]);
		    break;
		}
	    }

	} else {
	    /* The only non-flag arguments are the source and destination
	    ** designations.  The last argument is always the destination
	    ** but we can have multiple src files.
	    */
	    cmd = argv[i];
	    astart = i + 1;
	    break;
	}
    }


    /* Initialize the DTS interface.  Use NULL args since we aren't setting
    ** up to be a DTS server process, but this allows us to load the config
    ** file and initialize the 'dts' struct with local information.
    */
    memset (localIP, 0, SZ_PATH);
    strcpy (localIP, dts_getLocalIP());
    dts = dtsInit (config, port, localIP, "./", 0);

    if (debug) {
	 fprintf (stderr, "After Init:\n");
	 fprintf (stderr, "  serverName = %s\n", dts->serverName);
	 fprintf (stderr, "  serverRoot = %s\n", dts->serverRoot);
	 fprintf (stderr, "  serverPort = %d\n", dts->serverPort);
	 fprintf (stderr, "   serverUrl = %s\n", dts->serverUrl);
	 fprintf (stderr, "  serverHost = %s\n", dts->serverHost);
	 fprintf (stderr, "    serverIP = %s\n", dts->serverIP);
	 fprintf (stderr, "\n");
    }


    /* Initialize the DTS server methods we'll need.  Note, this isn't a 
     * complete server command set, only those we need to support the 
     * commands we can send (e.g. push/pull).
     */
    dts_cmdServerMethods (dts);


    /*   Set any debug or verbose params in the server as well.
     */
    if (verbose) dts_hostSet (host, "dts", "verbose", "true");
    if (debug)   dts_hostSet (host, "dts", "debug",   "true");


    /*  Log the command if possible.
    */
    sprintf (msg, "%s: cmd = %s\n", (host ? host : "localhost"), cmd);
    dtsLog (dts, msg);

    if (debug)
	fprintf (stderr, "msg: %s", msg);


    /**
     *    Process it.
     */
    /*************************************************************************
    **  File Commands
    *************************************************************************/

    if (strcasecmp (cmd, "access") == 0) {			/* ACCESS     */
	path = argv[astart++];
	if (astart < argc) {
	    switch (tolower ((int)(ch = argv[astart][0])) ) {
	    case 'r':  mode = R_OK;	break;
	    case 'w':  mode = W_OK;	break;
	    case 'x':  mode = X_OK;	break;
	    default:
	 	if (!quiet)
		    fprintf (stderr, "Invalid access mode '%c'\n", ch);
		return (1);
	    }
	} else 
	    mode = F_OK;	/* no mode specified */

	res = (noop ? OK : dts_hostAccess (host, path, mode));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

	return (res);

    } else if (strcasecmp (cmd, "cat") == 0) {			/* CAT        */
	path = argv[astart++];
	sres = (noop ? NULL : dts_hostCat (host, path));
	if (!quiet)
	    printf ("%s", sres);
	if (!noop && sres) free (sres);

    } else if (strcasecmp (cmd, "config") == 0 ||		/* CFG        */
               strcasecmp (cmd, "cfg") == 0) {
	sres = (noop ? NULL : dts_hostCfg (host));
	if (!quiet)
	    printf ("%s", sres);
	if (!noop && sres) free (sres);

    } else if (strcasecmp (cmd, "cwd") == 0 ||			/* CWD        */
               strcasecmp (cmd, "pwd") == 0) {
	sres = (noop ? NULL : dts_hostCwd (host));
	if (!quiet)
	    printf ("%s\n", sres);
	if (!noop && sres) free (sres);

    } else if (strcasecmp (cmd, "del") == 0 || strcasecmp (cmd, "rm")  == 0) {
								/* DEL        */
	char *passwd   = NULL;
	int  recursive = 0;

	if (strncmp (argv[astart], "-r", 2) == 0) {
	    recursive++;
	    astart++;
	}
	path = argv[astart++];
	passwd = argv[astart];

	res = (noop ? OK : dts_hostDel (host, path, passwd, recursive));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "ls") == 0 || strcasecmp (cmd, "dir") == 0) {
								/* LS         */
	int islong = 0;

	path = argv[astart];
	if (!path) path = "/";

	if (strncmp (path, "-l", 2) == 0) {
	    islong++;
	    path = argv[++astart];
	}
	if (!noop)
	    sres = dts_hostDir (host, path, islong);
	if (!quiet)
	    printf ("%s\n", (sres ? sres : "Error"));
	if (!noop && sres) free (sres);

    } else if (strcasecmp (cmd, "df") == 0) {			/* DF(ree)    */
	path = argv[astart];
	res = (noop ? -1 : dts_hostDiskFree (host, path));
	if (!quiet)
	    printf ("%9d KB Free\t%s\n", res, (path ? path : "/"));

    } else if (strcasecmp (cmd, "du") == 0) {			/* DU(sed)    */
	path = argv[astart];
	res = (noop ? -1 : dts_hostDiskUsed (host, path));
	if (!quiet)
	    printf ("%9d KB Used\t%s\n", res, (path ? path : "/"));

    } else if (strcasecmp (cmd, "echo") == 0) {			/* ECHO       */
	sres = (noop ? NULL : dts_hostEcho (host, argv[astart]));
	if (!quiet)
	    printf ("%s\n", sres);
	if (!noop && sres) free (sres);

    } else if (strcasecmp (cmd, "fsize") == 0) {		/* FSIZE      */
	path = argv[astart];
	res = (noop ? -1 : dts_hostFsize (host, path));
	if (!quiet)
	    printf ("%9d\t%s\n", res, path);

    } else if (strcasecmp (cmd, "ftime") == 0) {		/* FTIME      */
	path = argv[astart];
	res = (noop ? -1 : dts_hostFtime (host, path, "a"));
	if (!quiet) {
	    time_t tm = (time_t) res;
	    printf ("%s\t%s\n", path, (char *) ctime(&tm));
	}

    } else if (strcasecmp (cmd, "mkdir") == 0) {		/* MKDIR      */
	path = argv[astart];
	res = (noop ? OK : dts_hostMkdir (host, path));
	if (!quiet)
	    printf ("Directory '%s' %s\n", path,
	        (res == OK ? "created" : "not created"));

    } else if (strcasecmp (cmd, "ping") == 0) {			/* PING       */
	res = (noop ? OK : dts_hostPing (host));
	if (!quiet)
	printf ("DTS host '%s' is %s\n", 
	    (host ? host : "localhost"), 
	    (res == OK ? "alive" : "not responding"));

    } else if (strcasecmp (cmd, "prealloc") == 0) {		/* PREALLOC   */
	path = argv[astart++];
	size = atoi (argv[astart]);
	res = (noop ? OK : dts_hostPrealloc (host, path, size));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "rename") == 0 ||		/* RENAME     */
               strcasecmp (cmd, "mv")  == 0) {
	old = argv[astart++];
	new = argv[astart];
	res = (noop ? OK : dts_hostRename (host, old, new));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "stat") == 0) {			/* STAT       */
	struct stat st;

	path = argv[astart];
	res = (noop ? OK : dts_hostStat (host, path, &st));
	if (!quiet) {
	    time_t tm;

	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

	    printf ("%s:\n", path);
	    printf ("  device = %d\n", (int) st.st_dev);
	    printf ("   inode = %d\n", (int) st.st_ino);
	    printf ("    mode = %o\n", (int) st.st_mode);
	    printf ("   nlink = %d\n", (int) st.st_nlink);
	    printf ("     uid = %d\n", (int) st.st_uid);
	    printf ("     gid = %d\n", (int) st.st_gid);
	    printf ("    rdev = %d\n", (int) st.st_rdev);
	    printf ("    size = %d\n", (int) st.st_size);
	    printf (" blksize = %d\n", (int) st.st_blksize);
	    printf ("  blocks = %d\n", (int) st.st_blocks);
	    tm = st.st_atime; printf ("   atime = %s", (char *) ctime (&tm));
	    tm = st.st_mtime; printf ("   mtime = %s", (char *) ctime (&tm));
	    tm = st.st_ctime; printf ("   ctime = %s", (char *) ctime (&tm));
	}

    } else if (strcasecmp (cmd, "touch") == 0) {		/* TOUCH      */
	path = argv[astart];
	res = (noop ? OK : dts_hostTouch (host, path));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));





    } else if (strcasecmp (cmd, "fget") == 0) {			/* FGET       */
	int  nb;
	char *local;

	path = argv[astart++];
	local = argv[astart++];
	nb   = atoi (argv[astart]);
	res = (noop ? OK : dts_hostFGet (host, path, local, nb));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));


    } else if (strcasecmp (cmd, "read") == 0) {			/* READ       */
	int  nb;
	unsigned char *ures;

	path = argv[astart++];
	ures = (noop ? (unsigned char *)NULL : 
			dts_hostRead (host, path, 0, -1, &nb));
	if (!quiet)
	    printf ("%s", ures);
	if (!noop && ures) free (ures);




    /*************************************************************************
    **  File Transfer Commands
    *************************************************************************/

    } else if (strcasecmp (cmd, "give") == 0) {			/* GIVE       */
	res = (noop ? OK : dts_hostTo (host, port, XFER_PUSH,
					(argc - astart), &argv[astart], &xfs));
	if (!quiet)
	    printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		(res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		xfs.tput_MB, ((double)xfs.fsize/MBYTE), cmd);

    } else if (strcasecmp (cmd, "push") == 0) {			/* PUSH       */
	res = (noop ? OK : dts_hostTo (host, port, XFER_PULL,
					(argc - astart), &argv[astart], &xfs));
	if (!quiet)
	    printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		(res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		xfs.tput_MB, ((double)xfs.fsize/MBYTE), cmd);


    } else if (strcasecmp (cmd, "take") == 0) {			/* TAKE       */
	res = (noop ? OK : dts_hostFrom (host, port, XFER_PULL,
					(argc - astart), &argv[astart], &xfs));
	if (!quiet)
	    printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		(res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		xfs.tput_MB, ((double)xfs.fsize/MBYTE), cmd);

    } else if (strcasecmp (cmd, "pull") == 0) {			/* PULL       */
	res = (noop ? OK : dts_hostFrom (host, port, XFER_PUSH,
					(argc - astart), &argv[astart], &xfs));
	if (!quiet)
	    printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		(res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		xfs.tput_MB, ((double)xfs.fsize/MBYTE), cmd);





    /*************************************************************************
    **  Queue Commands
    *************************************************************************/

    /*************************************************************************
    **  Admin Commands
    *************************************************************************/

    } else if (strcasecmp (cmd, "abort") == 0) {		/* ABORT      */
	char *passwd = NULL;

	passwd = argv[astart];
	res = (noop ? OK : dts_hostAbort (host, passwd));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else {
	if (!quiet)
	    fprintf (stderr, "Command '%s' is not supported.\n", cmd);
	return (1);
    }


    /* Clean up.  On success, return zero to $status.
    */
    return (res);
}



/**
 *  DTS_CMDSERVERMETHODS -- Initialize the server-side of the DTS.  Create
 *  an XML-RPC server instance, define the commands and start the thread.
 */
static void
dts_cmdServerMethods (DTS *dts)
{
    if (dts->trace)
        fprintf (stderr, "Initializing Server Methods:\n");


    /*  Create the server and launch.  Note we never return from this
    **  and make no calls to other servers.
    */
    xr_createServer ("/RPC2", dts->serverPort, NULL);


    /*  Define the server methods.
    */
                /*  Not Yet Implemented  */
                /****  Queue Methods  ****/
    xr_addServerMethod ("startQueue",      dts_startQueue,      NULL);
    xr_addServerMethod ("stopQueue",       dts_stopQueue,       NULL);
    xr_addServerMethod ("flushQueue",      dts_flushQueue,      NULL);
    xr_addServerMethod ("restartQueue",    dts_restartQueue,    NULL);
    xr_addServerMethod ("addToQueue",      dts_addToQueue,      NULL);
    xr_addServerMethod ("removeFromQueue", dts_removeFromQueue, NULL);

                /****  Low-Level I/O Methods  ****/
    xr_addServerMethod ("read",            dts_Read,            NULL);
    xr_addServerMethod ("write",           dts_Write,           NULL);
    xr_addServerMethod ("prealloc",        dts_Prealloc,        NULL);
    xr_addServerMethod ("stat",            dts_Stat,            NULL);

                /****  Transfer Methods  ****/
    xr_addServerMethod ("xferPush",        dts_xferPush,        NULL);
    xr_addServerMethod ("xferPull",        dts_xferPull,        NULL);
    xr_addServerMethod ("receiveFile",     dts_xferReceiveFile, NULL);
    xr_addServerMethod ("sendFile",        dts_xferSendFile,    NULL);


    /* Start the server thread.
    */
    xr_startServerThread ();
}



/**
 *  USAGE -- Print usage information
 */
static void
Usage (void)
{
  fprintf (stderr, "\
Usage:\n\
    dtscmd [options] <cmd> <arg> [<arg> ...]\n\
\n\
Options:\n\
    -d				    debug flag\n\
    -h [name]			    host flag\n\
    -n				    no-op flag\n\
    -q				    quiet - suppress output flag\n\
    -v				    verbose output flag\n\
\n\
File Commands:\n\
    access <path>		    test file access\n\
    cwd				    print current working directory\n\
    del <pattern> [<passwd>]	    delete files matching pattern\n\
    dir [-l] [<pattern>]	    list filenames matching pattern\n\
    df				    return free disk (KB)\n\
    du				    return used disk (KB)\n\
    echo  <str>			    echo a string\n\
    fsize <path>		    print file size\n\
    ftime <path> [a|m|c]	    print file <mode> time\n\
    ls [-l] [<pattern>]	    	    list directory contents\n\
    mkdir <path>		    make a directory\n\
    mv <old> <new>		    rename a file\n\
    ping 			    ping a host\n\
    prealloc <path> <nbytes>	    preallocate file space\n\
    pwd				    print current working directory\n\
    rename <old> <new>		    rename a file\n\
    rm <pattern>		    delete files matching pattern\n\
    stat <path>			    print fille stat() of file\n\
    touch <path>		    touch a file's access time\n\n\
\n\
Queue Commands:\n\
\n\
\n\
Admin Commands:\n\
    abort			    shutdown the DTS\n\
    cfg			      	    get DTS config n target\n\
    init			    initialize the DTS\n\
    restart			    restart (stop/start) the DTS\n\
    set <key> <value>    	    set a DTS value\n\
    get <key>		    	    get a DTS value\n\
\n\
");
}
