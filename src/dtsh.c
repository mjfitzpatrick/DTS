/**
**  DTSH -- Utility app to send a command to a DTS daemon on a remote
**  host.  Note we only support a subset of commands that might be useful
**  in a scripting environment, the DTSMON as a console has full access to
**  all the DTSD functionality.
**	The first argument is always the host to be contacted, either a
**  direct IP address or a DTS logical name if a local config file can be
**  found.  The second argument is the name of the command followed by 
**  whichever arguments that command requires.
**
**  Usage:
**      dtsh [options] <cmd> <arg> [<arg> ...]
**
**  Options:
**      -h			      print help
**
**      -c [fname]		      set config file
**      -i			      run interactive shell
**      -n			      no-op flag
**      -q			      quiet - suppress output flag
**      -t [name]		      target host flag
**      -s			      run as command shell
**
**      -H <port>		      set hi transfer port
**      -L <port>		      set low transfer port
**      -N <port>		      set number of threads
**      -P <port>		      set (local) command port
**
**      -d			      debug flag
**      -v			      verbose output flag
**
**
**  Commands:
**	!<cmd>			      shell escape
**	rewind			      rewind command input (primitive loop)
**	sleep <N>		      suspend execution for <N> seconds
**	print <str>		      print <str> to stdout
**	port  <port>		      restart on specified port
**      target <host>		      set command target host (name or IP)
**
**	debug			      turn on debug output
**	verbose			      set verbose output flag
**	logout			      quit the shell
**
**	//  FILE COMMANDS
**      access <path>		      test file access to sanbox <path>
**      cat <file>		      concatenate <file> to stdout
**      cfg			      print DTS config on target machine
**      cwd			      print current working directory
**      del [-r] <pattern> [passwd]   delete files matching pattern
**      dir [-l] [<pattern>]   	      list filenames matching pattern
**      ddir <qname>   	      	      list delivery directory for named queue
**      df			      return free disk (KB)
**      du			      return used disk (KB)
**      echo  <str>		      echo a string
**      fget <path>		      get file from remote host
**      fsize <path>		      print file size
**      ftime <path> [a|m|c]	      print file <mode> time
**      ls [-l] [<pattern>]	      list filenames matching pattern
**      mkdir <path>		      make a directory
**      poke 			      poke a host
**      svrping 		      (server) ping a host
**      tping 			      threaded ping
**      ping 			      ping a host
**      pingstr 		      ping a host (string return)
**      pingsleep <n> 		      ping a host and sleep (int return)
**      pingarray 		      ping a host (array return)
**	pclient			      print client information
**	pqueue <queue>		      print queue information
**      mv <old> <new>		      rename a file
**      nping <N>		      ping a host <N> times
**      prealloc <path> <nbytes>      preallocate file space
**      pwd			      print current working directory
**	read <path>		      read from file
**      rename <old> <new>	      rename a file
**      rm <pattern>		      delete files matching patter
**      stat <path>		      print fille stat() of file
**      touch <path>		      touch a file's access time
**
**	//  FILE TRANSFER COMMANDS
**	give <file> <args>	      transfer local to remote, server
**	take <file> <args>	      transfer remote to local, server
**	push <file> <args>	      transfer local to remote, client
**	pull <file> <args>	      transfer remote to local, client
**
**	//  QUEUE COMMANDS
**      dest <qname>		      get destination of named queue
**      trace <qname>		      trace connectivity of named queue
**
**	//  ADMIN COMMANDS
**	abort			      abort dtsd
**	shutdown		      shutdown dtsd
**
**
**  @file       dtsh.c
**  @author     Mike Fitzpatrick, NOAO
**  @date	July 2010
*/


#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdarg.h>
#include <pthread.h>
#include <ctype.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "dts.h"
#include "dtsMethods.h"
#include "dtsClient.h"
#include "dtsPSock.h"
#include "build.h"


#define  APP_NAME	"dtsh"
#define  APP_VERSION	"1.0"

#define	 SZ_SBUF	4096
#define	 MAX_ARGS	32


char *host 	= "localhost";		/* target host          */
char *lhost 	= "";			/* logical host         */
char *cmd 	= NULL;			/* command              */
char *path 	= NULL;			/* path name            */
char *config 	= NULL;			/* config file name     */
char *alias 	= NULL;			/* node alias name      */
long fsize 	= 0;			/* file size            */
int mode 	= 0;			/* access mode          */

/* Options
*/
int debug 	= 0;			/* debug flag           */
int verbose 	= 0;			/* verbose flag         */
int quiet 	= 0;			/* quiet flag           */
int noop 	= 0;			/* no-op flag           */
int poke 	= 0;			/* poke flag            */
int doShell 	= 0;			/* command shell flag   */
int doTime 	= 0;			/* time the command     */
int contact 	= 0;			/* contact-only         */
int rate 	= 0;			/* transfer rate        */
int method 	= TM_PSOCK;		/* transfer method      */

int res 	= OK;
int cmdPort 	= DTSSH_PORT;
int loPort 	= DTSSH_PORT + 5;
int hiPort 	= DTSSH_PORT + 5 + DEF_MAX_PORTS;
int nthreads 	= DEF_NTHREADS;

FILE *in 	= (FILE *) NULL;
DTS  *dts 	= (DTS *) NULL;
char *clientUrl;
char  cmdbuf[SZ_SBUF];
char  localIP[SZ_PATH];

char *subArg[MAX_ARGS];
int   numArgs = 0;
char *xArgs[MAX_ARGS];
int   nXArgs = 0;


static int   dts_shPatchArgv (char *host, int dir, int argc, char *argv[]);
static void  dts_shFreeXArgv ();
static int   dts_shProcCmd (char *cmdbuf);
static int   dts_shProcess (int argc, char *argv[]);
static void  dts_shUsage (void);
static void  dts_shServerMethods (DTS * dts);
static int   dts_shTPing (char *host, int N);
static void  dts_shPing (void *data);
static char *dts_shCombineArgs (int argc, char *argv[]);
static char *dts_cfgAttr (char *outstr, char *attr);


/**
 *  Task entry point.
 */
int
main (int argc, char *argv[])
{
    register int i, j, len, astart = 1, interactive = 0, aseen = 0;
    char msg[SZ_LINE], cmdline[SZ_LINE];


    memset(msg, 0, SZ_LINE);
    memset(localIP, 0, SZ_PATH);
    memset(cmdbuf, 0, SZ_SBUF);
    memset(cmdline, 0, SZ_LINE);


    /* Process command-line arguments.
     */
    if (argc >= 3 && strstr (argv[1], "-f")) {

	/* Only allow '-f" as an arg, ignore anything else but save the
	 * non-flag arguments for later substitution.
	 *
	 *  Save any substitution arguments from the script commandline.
	 *  Note hat in this case the arg vector looks like
	 *
	 *      argv[0]         command interpreter (i.e. 'dtsh')
	 *      argv[1]         arguments on the #! line
	 *      argv[2]         path to the script file executed
	 *      argv[3]         script command line arg 1
	 *         :
	 *      argv[N]         script command line args N
	 */
	subArg[numArgs] = calloc (1, SZ_LINE);	/* init subArgs      */
	strcpy (subArg[numArgs++], argv[0]);

	for (i = 3; i < argc; i++) {
	    subArg[numArgs] = calloc (1, SZ_LINE);	/* copy arg          */
	    strcpy (subArg[numArgs++], argv[i]);
	}
	doShell++;

    } else {

	for (i = 1; i < argc; i++) {
	    if (!aseen && (argv[i][0] == '-' && !(isdigit(argv[i][1])))) {
		len = strlen(argv[i]);
		for (j = 1; j < len; j++) {
		    switch (argv[i][j]) {
		    case 'h':
			dts_shUsage();
			return (0);
                    case 'B':
			use_builtin_config = 1;
			break;

		    case 'a':
			alias = argv[++i];
			break;
		    case 'i':
			interactive++;
			doShell++;
			break;
		    case 'n':
			noop++;
			break;
		    case 'p':
			poke++;
			break;
		    case 'q':
			quiet++;
			break;
		    case 'r':
			rate = atoi(argv[++i]);
			break;
		    case 's':
			doShell++;
			break;
		    case 'c':
			config = argv[++i];
			break;
		    case 'l':
			lhost = argv[++i];
			break;
		    case 't':
			host = argv[++i];
			break;
		    case 'u':
			method = TM_UDT;
			break;

		    case 'C':
			contact++;
			break;
		    case 'H':
			hiPort = atoi(argv[++i]);
			break;
		    case 'L':
			loPort = atoi(argv[++i]);
			break;
		    case 'N':
			nthreads = atoi(argv[++i]);
			break;
		    case 'P':
			cmdPort = atoi(argv[++i]);
			break;
		    case 'T':
			doTime++;
			break;

		    case 'd':
			debug++;
			break;
		    case 'v':
			if (strcmp (&(argv[i][j + 1]), "ersion") == 0) {
			    printf ("Build Version: %s\n", build_date);
			    return (0);
			} else
			    verbose++;
			break;

		    default:
			fprintf (stderr, "Unknown option '%c'\n",
				argv[i][j]);
			break;
		    }
		}

	    } else {
		/*  The only non-flag arguments are command arguments.
		 */
		if (!aseen)
		    astart = i;
		aseen++;
		strcat (cmdbuf, argv[i]);
		if (i < (argc - 1))
		    strcat (cmdbuf, " ");
	    }
	}
    }


    if (argc == 1)
	interactive++, doShell++;


    /*  Initialize the DTS interface.  Use dummy args since we aren't setting
     *  up to be a DTS server process, but this allows us to load the config
     *  file and initialize the 'dts' struct with local information.
     */
    strcpy (localIP, dts_getLocalIP ());
    dts = dtsInit (config, cmdPort, localIP, "./", 0);
    dts->debug = debug;
    dts->verbose = verbose;

    /*  Initialize the DTS server methods we'll need.  Note, this isn't a 
     *  complete server command set, only those we need to support the 
     *  commands we can send (e.g. data transfer).
     */
    dts_shServerMethods (dts);

    /*  If we were given an alias name, lookup the host.
     */
    if (alias) {
	int port;

	for (i = 0; i < dts->nclients; i++) {
	    if (strcmp (alias, dts->clients[i]->clientName) == 0) {
		port = dts->clients[i]->clientPort;
		host = dts->clients[i]->clientHost;
		if (strchr (host, (int) ':') == NULL)
		    sprintf (host, "%s:%d", host, port);
		break;
	    }
	}
    }



    /*  To 'poke' a host means to check on its status without doing anything
     *  to cause the daemon to restart, i.e. if it's dead, it's dead.
     */
    if (poke) {
	int stat = (noop ? OK : dts_hostPoke (host));

	if (!quiet)
	    fprintf (stderr, "Host '%s', status = %s\n", host,
		    (stat == OK ? "OK" : "ERR"));
	return (stat);
    }

    /*  To 'contact' a host means to try connecting to the contact port
     *  for the node, i.e. try to restart the daemon.
     */
    if (contact) {
	int stat = (noop ? OK : dts_hostContact (host));

	if (!quiet)
	    fprintf (stderr, "Contacted host '%s', status = %s\n", host,
		    (stat == OK ? "OK" : "ERR"));
	return (stat);
    }

    /*  First test:  Can we contact the DTS server at all?
     */
    if (dts_hostPing (host) != OK) {
	fprintf (stderr, "Error: Cannot contact DTS host '%s'\n", host);

	/*  If we couldn't contact them we need to write in the XML file 
	 *  that the server is down.  This way we can show on the webmon
	 *  that the server is "red".
	 */
	if (strcasecmp (argv[astart], "xml") == 0) {
	    int   seekto;
	    FILE *file;
	    char  myfile[SZ_LINE];

	    seekto = atoi(argv[astart + 1]);

	    sprintf (myfile, "/web/dts%d.xml", seekto);
	    file = fopen (myfile, "w+");
	    fprintf (file, "<dts%d>\n", seekto);
	    fprintf (file, "<server>\n");
	    fprintf (file, "<status>dead</status>\n");
	    fprintf (file, "<config>\n");
	    fprintf (file, "<host>%s</host>\n", host);
	    fprintf (file, "</config>\n");
	    fprintf (file, "</server>\n");
	    fprintf (file, "</dts%d>\n", seekto);
	    fclose (file);
	}
	return (ERR);
    }


    /*   Set any debug or verbose params in the server as well.
     */
    if (!host) {
	fprintf (stderr, "Error:  No target host specified.\n");
	exit (1);
    }
    if (verbose)
	dts_hostSet (host, "dts", "verbose", "true");
    if (debug)
	dts_hostSet (host, "dts", "debug", "true");


    /*  Log the command if possible.
     */
    sprintf (msg, "%s: cmd = %s\n", (host ? host : "localhost"), cmd);
    dtsLog (dts, msg);

    if (debug)
	fprintf (stderr, "msg: %s", msg);


    if (!doShell) {
	dts_shProcess ((argc - astart), &argv[astart]);

    } else {
	int len, cnum = 1;


	/*  Open the script file or standard input if interactive.
	 */
	if (interactive) {
	    in = stdin;
	    printf ("dts %d> ", cnum++);

	} else if ((in = fopen (argv[2], "r")) == (FILE *) NULL) {
	    fprintf (stderr, "Cannot open script file '%s'\n", argv[2]);
	    exit (1);
	}

	/* Begin processing.
	 */
	while (fgets (cmdline, SZ_LINE, in)) {

	    if (cmdline[0] != '\n') {
		len = strlen (cmdline);	/* kill newline              */
		cmdline[len - 1] = '\0';

		dts_shProcCmd (cmdline);	/* process command           */
		cnum++;
	    }

	    memset (cmdline, 0, SZ_LINE);	/* reset                     */
	    if (interactive)
		printf ("dts %d> ", cnum);
	}

	if (in != stdin)	/*  close the input file     */
	    fclose (in);
	else
	    printf ("\n");
    }


    /* Clean up.  On success, return zero to $status.
     */
    dtsFree (dts);
    return (res);
}


/**
 *  DTS_SHPROCCMD -- Process a DTS shell command line, e.g. as when reading
 *  from a script file.  Break the command into argc/argv before processing.
 */
static int
dts_shProcCmd (char *cmdbuf)
{
    int anum = 0, argc = 0, status = OK, quote = 0, nq = 0, i;
    char *ip, *op, buf[SZ_LINE];
    char *argv[MAX_ARGS];


    if (cmdbuf[0] == '#' || cmdbuf[0] == '\n')
	return (OK);

    /*  Break the command buffer into an argument vector, making 
     **  substitutions as we process.
     */
    memset (argv, 0, MAX_ARGS);
    for (ip = cmdbuf; *ip; ip++) {

	/* Pull out the token. */
	memset (buf, 0, SZ_LINE);
	quote = nq = (*ip == '"');
	if (quote)
	    ip++;

	while (*ip && isspace(*ip))
	    ip++;

	for (op = buf; *ip;) {
	    if (!quote && isspace(*ip))
		break;
	    else {
		if (*ip == '"') {
		    if (nq++ >= 1) {
			ip++;
			break;
		    }
		}
	    }
	    *op++ = *ip++;
	}

	if (buf[0] == '$') {
	    anum = atoi(&buf[1]); 		/* argument substitution */
	    memset (buf, 0, SZ_LINE);
	    strcpy (buf, subArg[anum]);
	}

	argv[argc] = calloc (1, SZ_LINE);
	strcpy (argv[argc++], buf);
    }

    status = dts_shProcess (argc, argv);

    for (i = 0; i < argc; i++)
	if (argv[i])
	    free ((char *) argv[i]);

    return (status);
}


/**
 *  DTS_SHPROCESS -- Process a DTS shell command.  In this version, we
 *  use the "standard" argc/argv to pass in the command and arguments.
 *  Argument substitution has already been done by the caller.
 */
static int
dts_shProcess (int argc, char *argv[])
{
    long  size, astart = 1, i = 0;
    char *sres = (char *) NULL, ch;
    char *old = (char *) NULL, *new = (char *) NULL;
    char  shost[SZ_FNAME], dhost[SZ_FNAME];
    struct timeval s_time, e_time;
    xferStat xfs;


    /**
     *    Process it.
     */
    cmd = argv[0];
    astart = 1;

    gettimeofday (&s_time, NULL);

    if (cmd[0] == '#' || cmd[0] == '\n') {		/* comment line      */
	return (OK);

    } else if (cmd[0] == '!') {				/* shell escape      */
	strcpy(argv[0], ++cmd);
	system(dts_shCombineArgs(argc, argv));

    } else if (strcasecmp (cmd, "rewind") == 0) {	/* rewind input      */
	if (in != stdin)
	    rewind (in);

    } else if (strcasecmp (cmd, "sleep") == 0) {	/* quit              */
	sleep (atoi(argv[1]));

    } else if (strcasecmp (cmd, "print") == 0) {	/* print             */
	printf ("%s\n", dts_shCombineArgs ((argc - 1), &argv[1]));

    } else if (strcasecmp (cmd, "port") == 0) {		/* port              */
	dtsFree (dts);
	dts = dtsInit (config, (cmdPort = atoi(argv[1])), localIP, "./", 0);

    } else if (strcasecmp (cmd, "target") == 0) {	/* target            */
	host = strdup (argv[1]);
	if (verbose)
	    dts_hostSet (host, "dts", "verbose", "true");
	if (debug)
	    dts_hostSet (host, "dts", "debug", "true");

    } else if (strcasecmp (cmd, "debug") == 0) {	/* debug             */
	debug++;

    } else if (strcasecmp (cmd, "verbose") == 0) {	/* verbose           */
	verbose++;

    } else if (strncasecmp (cmd, "logout", 4) == 0) {	/* quit              */
	printf ("\n");
	exit (0);


    /*************************************************************************
    **  File Commands
    *************************************************************************/
    } else if (strcasecmp (cmd, "access") == 0) {	/* ACCESS     	*/
	path = argv[astart++];
	if (astart < argc) {
	    switch (tolower((int) (ch = argv[astart][0]))) {
	    case 'r':
		mode = R_OK;
		break;
	    case 'w':
		mode = W_OK;
		break;
	    case 'x':
		mode = X_OK;
		break;
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

    } else if (strcasecmp (cmd, "cat") == 0) {		/* CAT        */
	path = argv[astart++];
	sres = (noop ? NULL : dts_hostCat (host, path));
	if (!quiet)
	    printf ("%s", sres);
	if (!noop && sres)
	    free (sres);

    } else if (strcasecmp (cmd, "checksum") == 0) {
	unsigned int sum32, crc;
	char *md5 = NULL;

	path = argv[astart];
	res =
	    (noop ? -1 : dts_hostChecksum (host, path, &sum32, &crc, &md5));
	if (!quiet)
	    printf ("%s: sum32=%d crc=%d md5='%s'\n", path, sum32, crc, md5);
	if (md5)
	    free ((char *) md5);

    } else if (strcasecmp (cmd, "config") == 0 ||	/* CFG        */
	       strcasecmp (cmd, "cfg") == 0) {
	char *cfg = dts_hostCfg (host, 0);

	sres = (noop ? NULL : (cfg ? cfg : strdup ("none")));
	if (!quiet)
	    printf ("%s", sres);
	if (!noop && sres)
	    free (sres);
	if (cfg)
	    free ((void *) cfg);

    } else if (strcasecmp (cmd, "cwd") == 0 ||		/* CWD        */
	       strcasecmp (cmd, "pwd") == 0) {
	char *cwd = dts_hostCwd (host);

	sres = (noop ? NULL : (cwd ? cwd : strdup ("./")));
	if (!quiet)
	    printf ("%s\n", sres);
	if (!noop && sres)
	    free (sres);
	if (cwd)
	    free (cwd);

    } else if (strcasecmp (cmd, "del") == 0 || 		/* DEL        */
	       strcasecmp (cmd, "rm") == 0) {
	char *passwd = NULL;
	int recursive = 0;

	if (strncmp (argv[astart], "-r", 2) == 0) {
	    recursive++;
	    astart++;
	}
	path = argv[astart++];
	if (argv[astart])
	    passwd = argv[astart];
	else {
	    fprintf (stderr, "Warning: no passwd specified!\n");
	    passwd = "xyzzy";
	}

	res = (noop ? OK : dts_hostDel (host, path, passwd, recursive));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "ls") == 0 || 		/* LS         */
	       strcasecmp(cmd, "dir") == 0) {
	int islong = 0;

	path = argv[astart];
	if (!path)
	    path = "/";

	if (strncmp (path, "-l", 2) == 0) {
	    islong++;
	    path = argv[++astart];
	}
	if (!noop)
	    sres = dts_hostDir (host, path, islong);
	if (!quiet)
	    printf ("%s\n", (sres ? sres : "Error"));
	if (!noop && sres)
	    free (sres);

    } else if (strcasecmp (cmd, "qdir") == 0) {		/* QDIR       */
	char qpath[SZ_PATH];

	memset (qpath, 0, SZ_PATH);
	sprintf (qpath, "/spool/%s/", argv[astart]);

	if (!noop)
	    sres = dts_hostDir (host, qpath, 0);
	if (!quiet)
	    printf ("%s\n", (sres ? sres : "Error"));
	if (!noop && sres)
	    free (sres);

    } else if (strcasecmp (cmd, "ddir") == 0) {		/* DDIR       	*/
	if (!noop)
	    sres = dts_hostDestDir (host, argv[astart]);
	if (!quiet)
	    printf ("%s\n", (sres ? sres : "Error"));

    } else if (strcasecmp (cmd, "isDir") == 0) {
	path = argv[astart];
	res = (noop ? -1 : dts_hostIsDir (host, path));
	if (!quiet)
	    printf ("%s %s a directory\n", path,
		   (res > 0 ? "is" : "is not"));

    } else if (strcasecmp (cmd, "df") == 0) {		/* DF(ree)    	*/
	path = argv[astart];
	res = (noop ? -1 : dts_hostDiskFree (host, path));
	if (!quiet)
	    printf ("%9d KB Free\t%s\n", res, (path ? path : "/"));

    } else if (strcasecmp (cmd, "du") == 0) {		/* DU(sed)    	*/
	path = argv[astart];
	res = (noop ? -1 : dts_hostDiskUsed(host, path));
	if (!quiet)
	    printf ("%9d KB Used\t%s\n", res, (path ? path : "/"));

    } else if (strcasecmp (cmd, "echo") == 0) {		/* ECHO       	*/
	sres = (noop ? NULL : dts_hostEcho (host, argv[astart]));
	if (!quiet)
	    printf ("%s\n", sres);
	if (!noop && sres)
	    free (sres);

    } else if (strcasecmp (cmd, "fsize") == 0) {	/* FSIZE      	*/
	path = argv[astart];
	res = (noop ? -1 : dts_hostFSize (host, path));
	if (!quiet)
	    printf ("%9d\t%s\n", res, path);

    } else if (strcasecmp (cmd, "fmode") == 0) {	/* FMODE      	*/
	path = argv[astart];
	res = (noop ? -1 : dts_hostFMode (host, path));
	if (!quiet)
	    printf ("%9d\t%s\n", res, path);

    } else if (strcasecmp (cmd, "ftime") == 0) {	/* FTIME      	*/
	path = argv[astart];
	res = (noop ? -1 : dts_hostFTime (host, path, "a"));
	if (!quiet) {
	    time_t tm = (time_t) res;
	    printf ("%s\t%s\n", path, (char *) ctime(&tm));
	}

    } else if (strcasecmp (cmd, "submitLogs") == 0) {	/* GETLOGS      */
	char *qname = argv[astart++];
	res = (noop ? -1 : dts_hostSubmitLogs (host, qname, 
	    "Testlog2", "TestRecover2"));
	if (!quiet)
	    printf ("Submitted logss to queue '%s'\n", qname);

    } else if (strcasecmp (cmd, "eraseLog") == 0) {	/* GETLOGS      */
	char *qname = argv[astart++];
	char *fname = argv[astart++];
	res = (noop ? -1 : dts_hostEraseQLog (host, qname, fname));
	if (!quiet)
	    printf ("Erased file '%s' in queue '%s'\n", fname, qname);

    } else if (strcasecmp (cmd, "getLog") == 0) {	/* GETLOGS      */
	char *qname = argv[astart++];
	char *log = (noop ? NULL : dts_hostGetQLog (host, qname, "Log"));
	char *rec = (noop ? NULL : dts_hostGetQLog (host, qname, "Recover"));
	if (!quiet)
	    printf ("Log:\n%s\n\nRecover:\n%s", log, rec);
	if (!noop && log)  free (log);
	if (!noop && rec)  free (rec);

    } else if (strcasecmp (cmd, "mkdir") == 0) {	/* MKDIR      	*/
	path = argv[astart];
	res = (noop ? OK : dts_hostMkdir (host, path));
	if (!quiet)
	    printf ("Directory '%s' %s\n", path,
		   (res == OK ? "created" : "not created"));

    } else if (strcasecmp (cmd, "poke") == 0) {		/* POKE       	*/
	res = (noop ? OK : dts_hostPoke (host));
	if (!quiet)
	    printf ("DTS host '%s' is %s (poke)\n",
		   (host ? host : "localhost"),
		   (res == OK ? "alive" : "not responding"));

    } else if (strcasecmp (cmd, "ping") == 0) {		/* PING       	*/
	res = (noop ? OK : dts_hostPing (host));
	if (!quiet)
	    printf ("DTS host '%s' is %s (ping)\n",
		   (host ? host : "localhost"),
		   (res == OK ? "alive" : "not responding"));

    } else if (strcasecmp (cmd, "pingstr") == 0) {	/* PINGSTR    	*/
	if (!noop)
	    sres = dts_hostPingStr (host);
	if (!quiet)
	    printf ("%s\n", (sres ? sres : "Error"));
	if (!noop && sres)
	    free (sres);

    } else if (strcasecmp (cmd, "pingarray") == 0) {	/* PINGARRAY  	*/
	res = (noop ? OK : dts_hostPingArray (host));
	if (!quiet)
	    printf ("DTS host '%s' is %s\n",
		   (host ? host : "localhost"),
		   (res == OK ? "alive" : "not responding"));

    } else if (strcasecmp (cmd, "pingsleep") == 0) {	/* PING SLEEP 	*/
	int t = atoi(argv[astart]);

	/* Called as a sync method.
	 */
	res = (noop ? OK : dts_hostPingSleep (host, t));
	if (!quiet)
	    printf ("DTS host '%s' returns\n", (host ? host : "localhost"));

    } else if (strcasecmp (cmd, "svrping") == 0) {	/* SVR PING   	*/
	int t = atoi(argv[astart]);

	/* Called as an async method.
	 */
	res = (noop ? OK : dts_hostSvrPing (host, t));
	xr_asyncWait();		/* wait for completion */
	if (!quiet)
	    printf ("DTS host '%s' returns\n", (host ? host : "localhost"));

    } else if (strcasecmp (cmd, "tping") == 0) {	/* THREAD PING 	*/
	int n = atoi(argv[astart]);
	res = (noop ? OK : dts_shTPing (host, n));
	if (!quiet)
	    printf ("DTS host '%s' is %s, pinged %d times in parallel\n",
		   (host ? host : "localhost"),
		   (res == OK ? "alive" : "not responding"), n);

    } else if (strcasecmp (cmd, "nping") == 0) {	/* PING <N>   	*/
	int i, n = atoi(argv[astart]), nerr = 0;

	for (i = 0; i < n; i++) {
	    res = (noop ? OK : dts_hostPing (host));
	    if (res != OK)
		nerr++;
	    if (verbose && (i % 1000) == 0)
		fprintf (stderr, "%6d of %d...\n", i, n);
	}
	if (!quiet)
	    printf ("DTS host '%s' is %s, pinged %d times, %d errors\n",
		   (host ? host : "localhost"),
		   (res == OK ? "alive" : "not responding"), n, nerr);

    } else if (strcasecmp (cmd, "npingstr") == 0) {
	int i, n = atoi(argv[astart]), nerr = 0;

	for (i = 0; i < n; i++) {
	    if (!noop)
		sres = dts_hostPingStr (host);
	    if (sres == NULL)
		nerr++;
	    if (!noop && sres)
		free (sres);
	}
	if (!quiet)
	    printf ("DTS host '%s' is %s, pinged %d times, %d errors\n",
		   (host ? host : "localhost"),
		   (res == OK ? "alive" : "not responding"), n, nerr);

    } else if (strcasecmp (cmd, "npingarray") == 0) {
	int i, n = atoi(argv[astart]), nerr = 0;

	for (i = 0; i < n; i++) {
	    res = (noop ? OK : dts_hostPingArray (host));
	    if (res != OK)
		nerr++;
	}
	if (!quiet)
	    printf ("DTS host '%s' is %s, pinged %d times, %d errors\n",
		   (host ? host : "localhost"),
		   (res == OK ? "alive" : "not responding"), n, nerr);

    } else if (strcasecmp (cmd, "remotePing") == 0) {	/* REMOTEPING 	*/
	res = (noop ? OK : dts_hostRemotePing (host, argv[astart]));
	if (!quiet)
	    printf ("DTS host '%s' says '%s' is %s\n",
		   (host ? host : "localhost"),
		   (argv[astart] ? argv[astart] : "localhost"),
		   (res == OK ? "alive" : "not responding"));

    } else if (strcasecmp (cmd, "prealloc") == 0) {	/* PREALLOC   	*/
	path = argv[astart++];
	size = atoi(argv[astart]);
	res = (noop ? OK : dts_hostPrealloc (host, path, size));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "qstart") == 0) {	/* QSTART	*/
	char *qname = argv[astart++];
	dts_hostStartQueue (host, qname);

    } else if (strcasecmp (cmd, "qstop") == 0) {	/* QSTOP 	*/
	char *qname = argv[astart++];
	dts_hostShutdownQueue (host, qname);

    } else if (strcasecmp (cmd, "qpause") == 0) {	/* QPAUSE	*/
	char *qname = argv[astart++];
	dts_hostPauseQueue (host, qname);

    } else if (strcasecmp (cmd, "qpoke") == 0) {	/* QPOKE	*/
	char *qname = argv[astart++];
	dts_hostPokeQueue (host, qname);

    } else if (strcasecmp (cmd, "qflush") == 0) {	/* QFLUSH	*/
	char *qname = argv[astart++];
	dts_hostFlushQueue (host, qname);


    } else if (strcasecmp (cmd, "XML") == 0) {		/* XML WEB INTERFACE */
	int   seekto, servernum = 1;
	FILE *xfd;
	char  tmp[SZ_LINE], str[SZ_LINE], fpath[SZ_LINE];
	nodeStat nstat;


	sprintf (str, "%s\n", (dts_hostCfg(host, 1)));

	seekto = atoi(argv[astart]);
	sprintf (fpath, "/web/dts%d.xml", seekto);
	if ((xfd = fopen (fpath, "w+")) == (FILE *) NULL) {
	    fprintf (stderr, "Cannot open %s\n", fpath);
	    return (1);
	}

	servernum = seekto;

	printf ("%s\n", str);

	memset (&nstat, 0, sizeof(nstat));
	dts_hostNodeStat (host, "all", 0, &nstat);

	printf ("Disk Free: %7.2f\n", ((float) nstat.dfree / 1024.));

	/*  Write server config, queue config to XML file.  It's possible
	 *  to write method that you pass a struct to a lot like 
	 *  dts_hostNodeStat().   For now we're just parsing strings.       
	 *  
	 *  FIXME - Later add for multiple servers.
	 */
	fprintf (xfd, "<dts%d>\n", servernum);		// marker

	fprintf (xfd, "<server>\n");			// marker
	fprintf (xfd, "<status>%s</status>\n",
		(dts_hostPoke(host) == OK ? "alive" : "not responding"));
	fprintf (xfd, "<config>\n");			// marker
	fprintf (xfd, "<name>%s</name>\n", 		// container
		dts_cfgAttr (&str[0], "serverName:"));
	fprintf (xfd, "<host>%s</host>\n", 		// container
		dts_cfgAttr (&str[0], "serverHost:"));
	fprintf (xfd, "<port>%s</port>\n", 		// might need to change.
		dts_cfgAttr (&str[0], "serverPort:"));
	fprintf (xfd, "<configfile>%s</configfile>\n", // might need to change.
		dts_cfgAttr (&str[0], "configFile:"));
	fprintf (xfd, "<hiport>%s</hiport>\n",
		dts_cfgAttr (&str[0], "hiPort:"));
	fprintf (xfd, "<loport>%s</loport>\n",
		dts_cfgAttr (&str[0], "loPort:"));
	fprintf (xfd, "<root>%s</root>\n",
		dts_cfgAttr (&str[0], "serverRoot:"));
	fprintf (xfd, "<contact>%s</contact>\n",
		dts_cfgAttr (&str[0], "contactPort:"));
	fprintf (xfd, "<nclients>%s</nclients>\n",
		dts_cfgAttr (&str[0], "nclients:"));
	fprintf (xfd, "<nqueues>%s</nqueues>\n",
		dts_cfgAttr (&str[0], "nqueues:"));
	fprintf (xfd, "<copyMode>%s</copyMode>\n",
		dts_cfgAttr (&str[0], "copyMode:"));

	time_t now;
	time (&now);
	if (dts_cfgAttr (&str[0], "startTime:"))
	    fprintf (xfd, "<uptime>%ld</uptime>\n", 	// time in seconds
		now - atoi(dts_cfgAttr (&str[0], "startTime:")));

	if (debug)
	    printf ("build date: %s\n", dts_hostGet (host, "dts","build_date"));
	fprintf (xfd, "<version>%s</version>\n", 
		dts_hostGet (host, "dts", "build_date"));//build version
	fprintf (xfd, "<usedspace>%.2f GB</usedspace>\n",
		((float) nstat.dused / 1024.));
	fprintf (xfd, "<freespace>%.2f GB</freespace>\n",
		((float) nstat.dfree / 1024.));

	struct timeval s, e;

	gettimeofday (&s, NULL);
	dts_hostSet (host, "xfer", "foo", "bar");	//doesn't do anything.
	gettimeofday (&e, NULL);

	int tsec, tusec;
	tsec = e.tv_sec - s.tv_sec;
	tusec = e.tv_usec - s.tv_usec;
	if (tusec >= 1000000) {
	    tusec -= 1000000;
	    tsec++;
	} else if (tusec < 0) {
	    tusec += 1000000;
	    tsec--;
	}

	if (((double) tsec + (double) tusec / 1000000.0) < 1) {
	    fprintf (stderr, "Done: time = %.3f ms\n",
		    ((double) tsec + (double) tusec / 1000000.0) * 1000);
	    fprintf (xfd, "<ping>%4.2f ms</ping>\n", 
		((double) tsec + (double) tusec / 1000000.0) * 1000);	

	} else {
	    fprintf (stderr, "Done: time = %.3f s\n",
		    ((double) tsec + (double) tusec / 1000000.0));
	    fprintf (xfd, "<ping>%4.2f s</ping>\n", 
		((double) tsec + (double) tusec / 1000000.0));
	}


	// cant do not sure what numerrors is.
	//fprintf (xfd,"<failedtransfer></failedtransfer> 

	// all of these code in.
	//fprintf (xfd,"<recordbandwidth></recordbandwidth> 
	//fprintf (xfd,"<projectedbandwidth></projectedbandwidth>

	int i = 0;
	int totalflush = 0, totalcanceled = 0;

	for (i = 0; i < nstat.nqueues; i++) {
	    totalflush = nstat.queue[i].numflushes + totalflush;
	    totalcanceled = nstat.queue[i].canceledxfers + totalcanceled;
	}

	// not a method yet
	fprintf (xfd, "<totalflushes>%d</totalflushes>\n", totalflush);
	fprintf (xfd, "<canceledxfers>%d</canceledxfers>\n", totalcanceled);
	// not a method yet.

	fprintf (xfd, "</config>\n");
	fprintf (xfd, "</server>\n");


	/*  Loop  through our queues.
	 */
	int qact = 0;

	for (i = 0; i < nstat.nqueues; i++) {

	    sprintf (str, "%s\n",
		    dts_hostPrintQueueCfg (host, nstat.queue[i].name));
	    printf ("%s\n", str);

	    fprintf (xfd, "<queue>\n");
	    fprintf (xfd, "<qname>%s</qname>\n", nstat.queue[i].name);
	    fprintf (xfd, "<infile>%s</infile>\n", nstat.queue[i].infile);
	    fprintf (xfd, "<outfile>%s</outfile>\n",
		    nstat.queue[i].outfile);
	    fprintf (xfd, "<numflushes>%d</numflushes>\n",
		    nstat.queue[i].numflushes);
	    fprintf (xfd, "<canceledxfers>%d</canceledxfers>\n",
		    nstat.queue[i].canceledxfers);

	    fprintf (xfd, "<status>%s</status>\n", 
		nstat.queue[i].status);
	    fprintf (xfd, "<current>%d</current>\n", 	// number of current
		nstat.queue[i].current);
	    fprintf (xfd, "<pending>%d</pending>\n", 	// number of pending
		nstat.queue[i].pending);
	    fprintf (xfd, "<qcount>%d</qcount>\n", 	// queue count semaphore
		nstat.queue[i].qcount);
	    fprintf (xfd, "<qrate>%.2f</qrate>\n", 	// avg mbs transfer rate
		nstat.queue[i].rate);
	    fprintf (xfd, "<time>%.2f</time>\n", 	// avg transfer time
		nstat.queue[i].time);
	    fprintf (xfd, "<size>%.2f</size>\n", 	// avg transfer size
		nstat.queue[i].size);
	    fprintf (xfd, "<total>%.2f</total>\n", 	// total xfer size gb
		nstat.queue[i].total);
	    fprintf (xfd, "<nxfer>%d</nxfer>\n", 	// number xfered
		nstat.queue[i].nxfer);
	    fprintf (xfd, "<Mbs>%g</Mbs>\n", 		// average Mbs
		nstat.queue[i].Mbs);

	    /* Maybe write a struct that contains all the queue configuration.
	     */
	    fprintf (xfd, "<src>%s</src>\n",
		dts_cfgAttr (&str[0], "src:"));

	    // Not sure what to do with these yet.
	    fprintf (xfd, "<dest>%s</dest>\n", 	
		dts_cfgAttr (&str[0], "dest:"));
	    fprintf (xfd, "<deliveryDir>%s</deliveryDir>\n", 
		dts_cfgAttr (&str[0], "deliveryDir:"));
	    fprintf (xfd, "<deliveryCmd>%s</deliveryCmd>\n", 
		dts_cfgAttr (&str[0], "deliveryCmd:"));

	    fprintf (xfd, "<node>%s</node>\n",
		dts_cfgAttr (&str[0], "node:"));
	    fprintf (xfd, "<type>%s</type>\n",
		dts_cfgAttr (&str[0], "type:"));
	    fprintf (xfd, "<mode>%s</mode>\n",
		dts_cfgAttr (&str[0], "mode:"));
	    fprintf (xfd, "<method>%s</method>\n",
		dts_cfgAttr (&str[0], "method:"));
	    fprintf (xfd, "<udt_rate>%s</udt_rate>\n",
		dts_cfgAttr (&str[0], "udt_rate:"));
	    fprintf (xfd, "<nthreads>%s</nthreads>\n",
		dts_cfgAttr (&str[0], "nthreads:"));
	    fprintf (xfd, "<nerrs>%s</nerrs>\n",
		dts_cfgAttr (&str[0], "nerrs:"));

	    // in the format of lo -> hi
	    strcpy(tmp, dts_cfgAttr (&str[0], "port:"));	

	    /*  put a null where the space before the - began so we 
	     *  only display the loport.
	     */
	    if (strstr(tmp, "-"))	
		tmp[strstr(tmp, "-") - tmp - 1] = '\0';
	    printf ("%s\n", tmp);
	    fprintf (xfd, "<port>%s</port>\n", tmp);

	    fprintf (xfd, "</queue>\n");

	    /* 	FIXME
	    fprintf (xfd,"<keepalive> </keepalive>
	    fprintf (xfd,"<dest></dest>
	    */

	    qStat *q = &nstat.queue[i];
	    qact += (q->pending ? 1 : 0);
	}

	fprintf (xfd, "</dts%d>\n", servernum);

	/* fprintf (xfd,"<stats>\n");
	   fprintf (xfd,"<uptime> </uptime>
	   fprintf (xfd,"<type> </type>
	   fprintf (xfd,"<downloadrate></downloadrate>
	   fprintf (xfd,"<uploadrate></uploadrate>
	   fprintf (xfd,"<averagefiletime></averagefiletime>
	   fprintf (xfd,"<averagefilesize></averagefilesize>
	   fprintf (xfd,"<successrate></successrate>
	   fprintf (xfd,"<flags> </flags>
	   fprintf (xfd,"</stats>\n");
	 */

	fclose (xfd);	/* done! 	*/

    } else if (strcasecmp (cmd, "pqueue") == 0) {	/* PRINTQUEUE */
	char *sres, *qname = argv[astart++];

	printf ("%s\n", (sres = dts_hostPrintQueueCfg (host, qname)));

	if (!noop && sres)
	    free (sres);

    } else if (strcasecmp (cmd, "rename") == 0 ||	/* RENAME     */
	       strcasecmp (cmd, "mv") == 0) {
	old = argv[astart++];
	new = argv[astart];
	res = (noop ? OK : dts_hostRename (host, old, new));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "stat") == 0) {		/* STAT       */
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
	    printf ("    size = %lld\n", (long long) st.st_size);
	    printf (" blksize = %d\n", (int) st.st_blksize);
	    printf ("  blocks = %d\n", (int) st.st_blocks);
	    tm = st.st_atime;
	    printf ("   atime = %s", (char *) ctime(&tm));
	    tm = st.st_mtime;
	    printf ("   mtime = %s", (char *) ctime(&tm));
	    tm = st.st_ctime;
	    printf ("   ctime = %s", (char *) ctime(&tm));
	}

    } else if (strcasecmp (cmd, "touch") == 0) {	/* TOUCH      */
	path = argv[astart];
	res = (noop ? OK : dts_hostTouch (host, path));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "fget") == 0) {		/* FGET       */
	int nb;
	char *local;

	path = argv[astart++];
	local = argv[astart++];
	nb = atoi(argv[astart]);
	res = (noop ? OK : dts_hostFGet (host, path, local, nb));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "read") == 0) {		/* READ       */
	int nb;
	unsigned char *ures;

	path = argv[astart++];
	ures = (noop ? (unsigned char *) NULL :
		dts_hostRead (host, path, 0, -1, &nb));
	if (!quiet)
	    printf ("%s", ures);
	if (!noop && ures)
	    free (ures);




    /*************************************************************************
    **  File Transfer Commands
    *************************************************************************/

    } else if (strcasecmp (cmd, "give") == 0) {		/* GIVE       */
	nXArgs = dts_shPatchArgv (host, XFER_TO, (argc-astart), &argv[astart]);
	res = (noop ? OK :
	      dts_hostTo (host, cmdPort, method, rate, loPort, hiPort,
			nthreads, XFER_PUSH, nXArgs, &xArgs[0], &xfs));
	if (!quiet)
	    printf
		("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		 (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		 xfs.tput_MB, ((double) xfs.fsize / MBYTE), cmd);
	dts_shFreeXArgv ();

    } else if (strcasecmp (cmd, "push") == 0) {		/* PUSH       */
	nXArgs = dts_shPatchArgv (host, XFER_TO, (argc-astart), &argv[astart]);
	res = (noop ? OK :
	       dts_hostTo (host, cmdPort, method, rate, loPort, hiPort,
			nthreads, XFER_PULL, nXArgs, &xArgs[0], &xfs));
	if (!quiet)
	    printf
		("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		 (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		 xfs.tput_MB, ((double) xfs.fsize / MBYTE), cmd);
	dts_shFreeXArgv ();

    } else if (strcasecmp (cmd, "take") == 0) {	/* TAKE       */
	nXArgs = dts_shPatchArgv (host, XFER_FROM, (argc-astart),&argv[astart]);
	res = (noop ? OK :
	       dts_hostFrom (host, cmdPort, method, rate, loPort, hiPort,
			  nthreads, XFER_PULL, nXArgs, &xArgs[0], &xfs));
	if (!quiet)
	    printf
		("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		 (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		 xfs.tput_MB, ((double) xfs.fsize / MBYTE), cmd);
	dts_shFreeXArgv ();


    } else if (strcasecmp (cmd, "pull") == 0) {	/* PULL       */
	nXArgs = dts_shPatchArgv (host, XFER_FROM, (argc-astart),&argv[astart]);
	res = (noop ? OK :
	       dts_hostFrom (host, cmdPort, method, rate, loPort, hiPort,
			  nthreads, XFER_PUSH, nXArgs, &xArgs[0], &xfs));
	if (!quiet)
	    printf
		("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		 (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		 xfs.tput_MB, ((double) xfs.fsize / MBYTE), cmd);
	dts_shFreeXArgv ();




    /*************************************************************************
    **  Queue Commands
    *************************************************************************/

    } else if (strcasecmp (cmd, "dest") == 0) {		/* DEST       */
	sres = (noop ? NULL : dts_hostDest (host, argv[astart]));
	if (!quiet)
	    printf ("%s\n", sres);
	if (!noop && sres)
	    free (sres);

    } else if (strcasecmp (cmd, "src") == 0) {		/* SRC        */
	sres = (noop ? NULL : dts_hostSrc (host, argv[astart]));
	if (!quiet)
	    printf ("%s\n", sres);
	if (!noop && sres)
	    free (sres);

    } else if (strcasecmp (cmd, "startQueue") == 0) {	/* STARTQ     */
	res = (noop ? OK : dts_hostStartQueue (host, argv[astart]));
	if (!quiet)
	    printf ("%d\n", res);

    } else if (strcasecmp (cmd, "pauseQueue") == 0) {	/* PAUSEQ      */
	res = (noop ? OK : dts_hostPauseQueue (host, argv[astart]));
	if (!quiet)
	    printf ("%d\n", res);

    } else if (strcasecmp (cmd, "getQueueStat") == 0) {	/* GETQSTAT   */
	res = (noop ? OK : dts_hostGetQueueStat (host, argv[astart]));
	if (!quiet)
	    printf ("%d\n", res);

    } else if (strcasecmp (cmd, "setQueueStat") == 0) {	/* SETQSTAT   */
	char *qname = argv[astart++];
	int stat = atoi(argv[astart]);

	res = (noop ? OK : dts_hostSetQueueStat (host, qname, stat));
	if (!quiet)
	    printf ("%d\n", res);

    } else if (strcasecmp (cmd, "getQueueCount") == 0) {/* GETQCOUNT  */
	res = (noop ? OK : dts_hostGetQueueCount (host, argv[astart]));
	if (!quiet)
	    printf ("%d\n", res);

    } else if (strcasecmp (cmd, "setQueueCount") == 0) {/* SETQCOUNT  */
	char *qname = argv[astart++];
	int count = atoi(argv[astart]);

	res = (noop ? OK : dts_hostSetQueueCount (host, qname, count));
	if (!quiet)
	    printf ("%d\n", res);

    } else if (strcasecmp (cmd, "getQueueDir") == 0) {	/* GETQDIR    */
	sres = (noop ? "" : dts_hostGetQueueDir (host, argv[astart]));
	if (!quiet)
	    printf ("%s\n", sres);

    } else if (strcasecmp (cmd, "setQueueDir") == 0) {	/* SETQDIR    */
	char *qname = argv[astart++];
	char *dir = argv[astart++];

	res = (noop ? OK : dts_hostSetQueueDir (host, qname, dir));
	if (!quiet)
	    printf ("%d\n", res);

    } else if (strcasecmp (cmd, "getQueueCmd") == 0) {	/* GETQCMD    */
	sres = (noop ? "" : dts_hostGetQueueCmd (host, argv[astart]));
	if (!quiet)
	    printf ("%s\n", sres);

    } else if (strcasecmp (cmd, "setQueueCmd") == 0) {	/* SETQCMD    */
	char *qname = argv[astart++];
	char *dir = argv[astart++];

	res = (noop ? OK : dts_hostSetQueueCmd (host, qname, dir));
	if (!quiet)
	    printf ("%d\n", res);

    } else if (strcasecmp (cmd, "listQueue") == 0) {	/* LISTQ      */
	sres = (noop ? NULL : dts_hostListQueue (host, argv[astart]));
	if (!quiet)
	    printf ("%s\n", sres);
	if (!noop && sres)
	    free (sres);


    } else if (strcasecmp (cmd, "strace") == 0) {	/* STRACE     */
	if (noop)
	    return (OK);

	sres = dts_hostSrc (host, argv[astart]);
	memset (shost, 0, SZ_FNAME); strcpy (shost, host);
	memset (dhost, 0, SZ_FNAME); strcpy (dhost, sres);
	while (strcasecmp (shost, "start") != 0) {
	    res = dts_hostRemotePing (shost, dhost);
	    if (!quiet) {
		printf ("%s (%s) %s  ",
		       shost, (res == OK ? "OK" : "not responding"),
		       (++i ? "->" : ""));

		sres = dts_hostSrc (dhost, argv[astart]);
		if (strcasecmp (sres, "start") == 0) {
		    printf ("%s (%s)\n",
			   dhost, (res == OK ? "OK" : "not responding"));
		    if (res == ERR)
			return (ERR);
		    break;
		}
		memset (shost, 0, SZ_FNAME); strcpy (shost, dhost);
		memset (dhost, 0, SZ_FNAME); strcpy (dhost, sres);
	    }
	    if (sres)
		free ((void *) sres), sres = NULL;
	    if (res == ERR)
		return (ERR);
	}
	if (sres)
	    free ((void *) sres), sres = NULL;

    } else if (strcasecmp (cmd, "dtrace") == 0) {	/* DTRACE     */
	if (noop)
	    return (OK);

	sres = dts_hostDest (host, argv[astart]);
	memset (shost, 0, SZ_FNAME); strcpy (shost, host);
	memset (dhost, 0, SZ_FNAME); strcpy (dhost, sres);
	free (sres), sres = NULL;

	while (strcasecmp (shost, "end") != 0) {
	    res = dts_hostRemotePing (shost, dhost);
	    if (!quiet) {
		printf ("%s (%s) %s  ",
		       shost, (res == OK ? "OK" : "not responding"),
		       (++i ? "->" : ""));

		sres = dts_hostDest (dhost, argv[astart]);
		if (strcasecmp (sres, "end") == 0) {
		    printf ("%s (%s)\n",
			   dhost, (res == OK ? "OK" : "not responding"));
		    if (res == ERR)
			return (ERR);
		    break;
		}
		memset (shost, 0, SZ_FNAME); strcpy (shost, dhost);
		memset (dhost, 0, SZ_FNAME); strcpy (dhost, sres);
		free (sres), sres = NULL;
	    }
	    if (sres)
		free ((void *) sres), sres = NULL;
	    if (res == ERR)
		return (ERR);
	}
	if (sres)
	    free ((void *) sres), sres = NULL;



    /*************************************************************************
    **  Admin Commands
    *************************************************************************/

    } else if (strcasecmp (cmd, "abort") == 0) {	/* ABORT      */
	char *passwd = argv[astart];

	res = (noop ? OK : dts_hostAbort (host, passwd));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "shutdown") == 0) {	/* SHUTDOWN   */
	char *passwd = argv[astart];

	res = (noop ? OK : dts_hostShutdown (host, passwd));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else {
	if (!quiet)
	    fprintf (stderr, "Command '%s' is not supported.\n", cmd);
	return (1);
    }

    gettimeofday(&e_time, NULL);
    if (!quiet && doTime) {
	int tsec, tusec;

	tsec = e_time.tv_sec - s_time.tv_sec;
	tusec = e_time.tv_usec - s_time.tv_usec;
	if (tusec >= 1000000) {
	    tusec -= 1000000;
	    tsec++;
	} else if (tusec < 0) {
	    tusec += 1000000;
	    tsec--;
	}
	fprintf (stderr, "Done: time = %.3f sec\n",
		((double) tsec + (double) tusec / 1000000.0));
    }


    /*  Clean up.  On success, return zero to $status.
     */
    return (res);
}



/**
 *  DTS_SHPATCHARGV --  Given the commandline arguments for the task, 
 *  convert these to forms used by the DTS.
 */
static int
dts_shPatchArgv (char *host, int dir, int argc, char *argv[])
{
    register int i;
    int   have_src = 0, have_dest = 0;
    char *src = NULL, *dest = NULL, first;
    char *cwd, buf[SZ_PATH];


    memset (buf, 0, SZ_PATH);	/* get current working directory  */
    cwd = getcwd (buf, (size_t) SZ_PATH);
    nXArgs = argc;

    for (i = 0; i < argc; i++) {

	first = argv[i][0];
	if (first == '-' || (isdigit(first) && !strchr(argv[i], (int) ':'))) {
	    xArgs[i] = calloc (1, strlen(argv[i] + 1));
	    strcpy (xArgs[i], argv[i]);

	} else {
	    char *fname = argv[i];

	    xArgs[i] = calloc (1, SZ_URL);

	    if (have_src) {
		if (strchr (fname, (char) ':'))
		    strcpy (xArgs[i], fname);
		else if (dir == XFER_TO)
		    sprintf (xArgs[i], "%s:%s", host, fname);
		else if (dir == XFER_FROM)
		    sprintf (xArgs[i], "%s:%s", host, dts_pathFname(fname));
		dest = xArgs[i];
		have_dest++;

	    } else {
		if (strchr (fname, (char) ':'))
		    strcpy (xArgs[i], fname);
		else if (dir == XFER_TO) {	/*  push/give  */
		    sprintf (xArgs[i], "%s:%s/%s", dts_getLocalIP(), cwd,fname);
		    src = dts_pathFname (fname);

		} else if (dir == XFER_FROM) {	/*  pull/take  */
		    sprintf (xArgs[i], "%s:/%s", dts_resolveHost(host), fname);
		    src = fname;
		}
		have_src++;
	    }
	}
    }

    if (!have_dest) {
	nXArgs++;

	xArgs[nXArgs - 1] = calloc (1, SZ_LINE);
	if (dir == XFER_TO)
	    sprintf (xArgs[nXArgs - 1], "%s", src);
	else if (dir == XFER_FROM)
	    sprintf (xArgs[nXArgs - 1], "%s/%s", cwd, dts_pathFname (src));
    }

    if (debug) {
	fprintf (stderr, "src  = '%s'\n", src);
	fprintf (stderr, "dest = '%s'\n", dest);
	for (i = 0; i < nXArgs; i++)
	    fprintf (stderr, "xArgs[%d] = '%s'\n", i, xArgs[i]);
    }

    return (nXArgs);
}


/**
 *  DTS_SHFREEARGV --  Free the patched argument vector.
 */
static void
dts_shFreeXArgv (void)
{
    register int i;

    for (i = 0; i < nXArgs; i++)
	free ((void *) xArgs[i]);
}


/**
 *  DTS_SHSERVERMETHODS -- Initialize the server-side of the DTS.  Create
 *  an XML-RPC server instance, define the commands and start the thread.
 */
static void
dts_shServerMethods (DTS * dts)
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
    xr_addServerMethod ("startQueue", dts_startQueue, NULL);
    xr_addServerMethod ("pauseQueue", dts_pauseQueue, NULL);
    xr_addServerMethod ("flushQueue", dts_flushQueue, NULL);
    xr_addServerMethod ("restartQueue", dts_restartQueue, NULL);
    xr_addServerMethod ("addToQueue", dts_addToQueue, NULL);
    xr_addServerMethod ("removeFromQueue", dts_removeFromQueue, NULL);

		/****  Low-Level I/O Methods  ****/
    xr_addServerMethod ("read", dts_Read, NULL);
    xr_addServerMethod ("write", dts_Write, NULL);
    xr_addServerMethod ("prealloc", dts_Prealloc, NULL);
    xr_addServerMethod ("stat", dts_Stat, NULL);

		/****  Transfer Methods  ****/
    xr_addServerMethod ("xferPushFile", dts_xferPushFile, NULL);
    xr_addServerMethod ("xferPullFile", dts_xferPullFile, NULL);
    xr_addServerMethod ("receiveFile", dts_xferReceiveFile, NULL);
    xr_addServerMethod ("sendFile", dts_xferSendFile, NULL);

		/****  Async Handler Methods  ****/
    xr_addServerMethod ("psHandler", dts_psHandler, NULL);


    /* Start the server thread.
     */
    xr_startServerThread ();
}


/**
 *  DTS_SHCOMBINEARGS -- Combine an argv vector into a single commandline.
 */
static char *
dts_shCombineArgs (int argc, char *argv[])
{
    register int i;
    static char cmd[4096];

    memset (cmd, 0, 4096);
    for (i = 0; i < argc; i++) {
	strcat (cmd, argv[i]);
	if (i < (argc - 1))
	    strcat (cmd, " ");
    }

    return (cmd);
}


/**
 *  DTS_SHTPING -- Multi-threaded PING.
 */
#define SH_MAX_THREADS		1024

static int
dts_shTPing (char *host, int N)
{
    register int i, j, end;
    int   stat, rc;
    pthread_t tid[SH_MAX_THREADS];
    pthread_attr_t th_attr;

    if (N > SH_MAX_THREADS) {
	fprintf (stderr, "Too many threads, max = %d\n", SH_MAX_THREADS);
	return (-1);
    }

    /* Initialize the service processing thread attribute.
     */
    pthread_attr_init (&th_attr);
    pthread_attr_setdetachstate (&th_attr, PTHREAD_CREATE_JOINABLE);


    /* Spawn the threads.
     */
    for (i = 0; i < N; i++) {
	for (j = 0; j < 16 && i < N; j++, i++) {
	    if ((rc = pthread_create (&tid[j], &th_attr,
				     (void *) dts_shPing, (void *) NULL))) {
		fprintf (stderr,
			"ERROR: pthread_create() fails, code: %d, thread %d\n",
			rc, i);
		exit(1);
	    }
	}
	end = j;

/* FIXME */

	/* Collect the threads.  We must re-join to avoid memory leaks.
	 */
	for (j = 0; j < end; j++) {
	    if ((rc = pthread_join (tid[j], (void **) &stat))) {
		fprintf (stderr, "pthread_join() failure\n");
		break;
	    }
	}
    }

    return (0);
}


/*  DTS_SHPING -- Utility function to ping a host.
 */
static void
dts_shPing (void *data)
{
    (void) dts_hostPing (host);
}


/*  DTS_CFGATTR -- Get a configuration attribute value as a string.
 */
static char *
dts_cfgAttr (char *outstr, char *attr)
{
    char *str_start, *str_data_start, *str_stop;
    char mystr[SZ_LINE], *mystrp;


    if ((str_start = strstr (outstr, attr))) {

	if ((str_stop = strstr (str_start, "\n"))) {
	    if ((str_data_start = strchr (str_start, (int) ':') + 1)) {
		/* Get rid of whitespace.
		*/
		while (isspace (*str_data_start) && str_data_start != str_stop)
		     str_data_start++;
		strncpy (mystr, str_data_start, str_stop - str_data_start);
		mystr[str_stop - str_data_start] = '\0';
		mystrp = &mystr[0];
		return mystrp;
	    } else
		return NULL;
	} else
	    printf ("Failed to find new line for: %s\n", attr);

	return NULL;
    } else
	printf ("Failed to find %s inside of our parsing string. \n", attr);

    return NULL;
}


/**
 *  USAGE -- Print usage information
 */
static void dts_shUsage(void)
{
    fprintf (stderr, "\
Usage:\n\
    dtsh [options] <cmd> <arg> [<arg> ...]\n\
\n\
Options:\n\
    -t [name]			    command target flag\n\
    -n				    no-op flag\n\
    -q				    quiet - suppress output flag\n\
    -f				    run as command interpreter\n\
\n\
    -d				    debug flag\n\
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
    nping <N> 			    ping a host <N> times\n\
    tping <N> 			    ping a host <N> times, threaded\n\
    prealloc <path> <nbytes>	    preallocate file space\n\
    pwd				    print current working directory\n\
    rename <old> <new>		    rename a file\n\
    rm <pattern>		    delete files matching pattern\n\
    stat <path>			    print fille stat() of file\n\
    touch <path>		    touch a file's access time\n\n\
\n\
    push <path>		    	    push file to remote server\n\
    pull <path>		    	    pull file from remote server\n\
    give <path>		    	    give file to remote node (local server)\n\
    take <path>		    	    take file from remote node (local server)\n\
\n\
Queue Commands:\n\
\n\
\n\
Admin Commands:\n\
    abort			    shutdown the DTS (not nice)\n\
    shutdown			    shutdown the DTS (nice)\n\
    cfg			      	    get DTS config n target\n\
    init			    initialize the DTS\n\
    restart			    restart (stop/start) the DTS\n\
    set <key> <value>    	    set a DTS value\n\
    get <key>		    	    get a DTS value\n\
\n\
");
}
