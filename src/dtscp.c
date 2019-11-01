/**
**  DTSCP -- Utility app to copy files manually between DTS nodes.
**
**  Usage:
**      dtscp [options] <from> <to>
**
**  Options:
**
**      -a <alias>		      set contact alias
**      -f <file>		      process arguments in <file>         (NYI)
**      -g			      use GIVE/TAKE mode for transfer     (DEF)
**      -i			      run interactive shell               (NYI)
**      -p			      use PUSH/PULL mode for transfer
**      -q <qname>		      transfer to queue's deliveryDir
**      -t [name]		      target host name/IP
**
**  Common DTS Options
**
**      --debug <N>             set debug level
**      --help                  print task help
**      --noop                  no-op flag, print steps taken only
**      --verbose               verbose flag
**      --version               print task version
**
**      -c <file>               set DTSCP config file to be used
**      -q <queue>              submit to specified queue
**      -t <host>               name/IP address of target DTS host
**      -w <path>               set directory to DTSCP working directory
**
**      -H <port>               set hi transfer port
**      -L <port>               set low transfer port
**      -N <NThreads>           set number of transfer threads
**      -P <port>               set communications port
**
**      <path>                  full path to file to queue
**
**  @file       dtscp.c
**  @author     Travis Semple, Mike Fitzpatrick, NOAO
**  @date	June 2013
*/

//#include <ftw.h>
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
#include <libgen.h>

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


#define  APP_NAME	"dtscp"
#define  APP_VERSION	"1.0"

#define	 SZ_SBUF	4096
#define	 MAX_ARGS	32

char *srchost 	= NULL;
char *host 	= NULL;		/* target host          */
char *cmd 	= NULL;		/* command              */
char *path 	= NULL;		/* path name            */
char *config 	= NULL;		/* config file name     */
char *alias 	= NULL;		/* node alias name      */
long  fsize 	= 0;		/* file size            */
int   mode 	= 0;		/* access mode          */

/*  Task Options
*/
int debug 	= 0;		/* debug flag           */
int verbose 	= 0;		/* verbose flag         */
int quiet 	= 0;		/* quiet flag           */
int noop 	= 0;		/* no-op flag           */
int exec 	= 0;		/* exec deliveryCmd     */
int poke 	= 0;		/* poke flag            */
int doShell 	= 0;		/* command shell flag   */
int move_flag 	= 0;
int doTime 	= 0;		/* time the command     */
int try_pull 	= 0;		/* GIVE/TAKE mode       */
int push 	= 1;		/* local->remote?       */
int remoteDest 	= 0;		/* remote destination?  */
int rate 	= 0;		/* transfer rate        */
int method 	= TM_PSOCK;	/* transfer method      */

int res 	= OK;
int execStat 	= ERR;
int port 	= 0;
int cmdPort 	= DTSCP_PORT;
int loPort 	= DTSCP_PORT + 50;
int hiPort 	= DTSCP_PORT + 50 + DEF_MAX_PORTS;
int nThreads 	= 1;		/* DEF_NTHREADS		*/
int s_mfile 	= 0;


// was qnames
char *qname 	= NULL;		/* queue name           */
char *ddir 	= NULL;		/* delivery directory   */
char *cdir 	= NULL;		/* copy directory       */

FILE *in 	= (FILE *) NULL;
DTS *dts 	= (DTS *) NULL;

char *clientUrl;
char cmdbuf[SZ_SBUF];
char localIP[SZ_PATH];
char to[SZ_LINE], cwd[SZ_LINE];
char from[SZ_LINE];


/**
 *  struct that holds our source files hosts ips 
 */
typedef struct {
    char  file[SZ_LINE];
    char  host[SZ_LINE];
    char  ip[SZ_LINE];
    char  dtscp_src;
    char  copymode;
} srcStruct;


typedef struct {
    char  file[SZ_LINE];
    char  host[SZ_LINE];
    char  ip[SZ_LINE];
    char  dtscp_dst;
    char  copymode;
} destStruct;

destStruct dst;
srcStruct  src[50];		// FIXME - limits file size

int 	toPort, fromPort;

/* Might need multiple froms.
*/
char  from_file[SZ_LINE];
char  fromHost[SZ_LINE], toHost[SZ_LINE];
char  fromIP[SZ_LINE], toIP[SZ_LINE];
char  fromPath[SZ_LINE], toPath[SZ_LINE], savedtoPath[SZ_LINE];
char  execBuf[SZ_LINE], execCmd[SZ_LINE];
char  queuePath[SZ_LINE];

char *subArg[MAX_ARGS];
int numArgs = 0;
char *xArgs[MAX_ARGS];
int nXArgs = 0;

xferStat xfs;				/* transfer stats            */
Control  control;


static void  dts_cpUnlink (char *cmd, char *path);
static void  dts_cpResolvefromConfig (DTS * dts, srcStruct * mysrc,
				     destStruct * mydst);
static void  dts_cpListFromFile (char *from_file, srcStruct * mysrc,
				int *add_f);
static void  dts_cpBuildCmd (srcStruct * mysrc, char *fPath,
			    destStruct * mydst, char *tPath, char *qPath);
static void  dts_cpCheckForRemoteWildcard (srcStruct * mysrc, int *lastent,
					  int i, char *lclIP);
static int   dts_cpPatchArgv (char *host, int dir, int argc, char *argv[]);
static int   dts_cpProcCmd (char *cmdbuf);
static int   dts_cpProcess (int argc, char *argv[]);
static void  dts_cpFreeXArgv (void);
static void  dts_cpUsage (void);
static void  dts_cpServerMethods (DTS * dts);

static void  dts_qInitControl (char *qhost, char *fname);
static char *dts_cpFname (char *fname);


/**
 *  Task main().
 */
int 
main (int argc, char **argv)
{
    register int i=0, j=0, k=0, len=0;
    register int L = 0, astart = 1, interactive = 0, aseen = 0;
    char  msg[SZ_LINE], cmdline[SZ_LINE], *ip = NULL;
    char  fpathdir = 0, fpathlocal = 0, tpathdir = 0, tpathlocal = 0;
    char  buf[SZ_LINE];


    memset (queuePath, 0, SZ_LINE); 		/*  initialize  	*/
    memset (msg, 0, SZ_LINE);
    memset (localIP, 0, SZ_PATH);
    memset (cmdbuf, 0, SZ_SBUF);
    memset (cmdline, 0, SZ_LINE);
    memset (from, 0, SZ_LINE);
    memset (to, 0, SZ_LINE);
    memset (fromHost, 0, SZ_LINE);
    memset (toHost, 0, SZ_LINE);
    memset (fromPath, 0, SZ_LINE);
    memset (toPath, 0, SZ_LINE);
    memset (execBuf, 0, SZ_LINE);
    memset (execCmd, 0, SZ_LINE);
    memset (cwd, 0, SZ_LINE);


    /*  Process command-line arguments.
     */
    if (argc >= 3 && strstr (argv[1], "-F")) {

	/* Only allow '-f" as an arg, ignore anything else but save the
	 * non-flag arguments for later substitution.
	 *
	 *  Save any substitution arguments from the script commandline.
	 *  Note hat in this case the arg vector looks like
	 *
	 *      argv[0]         command interpreter (i.e. 'dtscp')
	 *      argv[1]         arguments on the #! line
	 *      argv[2]         path to the script file executed
	 *      argv[3]         script command line arg 1
	 *         :
	 *      argv[N]         script command line args N
	 */
	subArg[numArgs] = calloc (1, SZ_LINE);		/* init subArgs      */
	strcpy (subArg[numArgs++], argv[0]);

	for (i = 3; i < argc; i++) {
	    subArg[numArgs] = calloc (1, SZ_LINE);	/* copy arg          */
	    strcpy (subArg[numArgs++], argv[i]);
	}
	doShell++;

    } else {

	for (i = 1; i < argc; i++) {
	    if (!aseen && (argv[i][0] == '-' && !(isdigit (argv[i][1])))) {
		len = strlen (argv[i]);
		for (j = 1; j < len; j++) {
		    switch (argv[i][j]) {
                    case 'B':
			use_builtin_config = 1;
			break;

		    case 'e':
			exec++;
			if (argv[i + 1][0] != '-')
			    strcpy (execCmd, argv[++i]);
			break;

		    case 'f':
			strcpy (from_file, argv[++i]);
			break;
		    case 'm':
			move_flag = 1;
			break;
		    case 'i':
			interactive++;
			doShell++;
			break;
		    case 'n':
			noop++;
			break;
		    case 'p':
			try_pull = 1;
			break;
		    case 'u':
			method = TM_UDT;
			nThreads = 1;
			break;
		    case 'R':
			rate = atoi (argv[++i]);
			break;

			/*  FIXME -- Not yet implemented  */
		    case 'a':
			alias = argv[++i];
			break;
		    case 't':
			host = argv[++i];
			break;

			/* DTS Common Options.
			 */
		    case 'c':
			config = argv[++i];
			break;
		    case 'q':
			qname = argv[++i];
			break;

		    case 'P':
			port = atoi (argv[++i]);
			break;
		    case 'L':
			loPort = atoi (argv[++i]);
			break;
		    case 'H':
			hiPort = atoi (argv[++i]);
			break;
		    case 'N':
			nThreads = atoi (argv[++i]);
			break;

		    case '-':
			len = strlen (argv[i]);
			for (j = 2; j < len; j++) {
			    switch (argv[i][j]) {
			    case 'd':
				debug++;
				j = len;
				break;
			    case 'h':
				dts_cpUsage ();
				j = len;
				return (0);
			    case 'n':
				noop++;
				j = len;
				break;
			    case 'R':
				rate = atoi (argv[++i]);
				j = len;
				break;
			    case 'v':
				if (strcmp (&(argv[i][j + 1]), "ersion") == 0) {
				    printf ("Build Version: %s\n", build_date);
				    return (0);
				} else
				    verbose++;
				j = len;
				break;

			    default:
				fprintf (stderr, "Invalid option '%s'\n",
					 argv[i]);
				exit (1);
			    }
			}
			break;

		    case 'v':
			if (strcmp (&(argv[i][j + 1]), "ersion") == 0) {
			    printf ("Build Version: %s\n", build_date);
			    return (0);
			} else
			    verbose++;
			break;

		    default:
			fprintf (stderr, "Unknown option '%c'\n", argv[i][j]);
			break;
		    }
		}

	    } else if (argv[i][0] == '+') {
		len = strlen (argv[i]);
		for (j = 1; j < len; j++) {
		    switch (argv[i][j]) {
		    case 'q':
			quiet++;
			break;
		    case 'd':
			debug++;
			break;
		    case 'v':
			quiet++, verbose = 0;
			break;
		    }
		    j = len;	/* skip remainder       */
		}

	    } else {
		/*  The only non-flag arguments are command arguments.
		 */
		if (strncmp ("cmd", argv[i], 3) == 0) {
		    if (!aseen)
			astart = i;
		    aseen++;
		    strcat (cmdline, argv[i]);
		    if (i < (argc - 1))
			strcat (cmdline, " ");

		} else if ((to[0] == '\0') && (i == argc - 1)) {
		    /*  Our last parameter is always our To.
		     */
		    strcpy (to, argv[argc - 1]);	

		} else if (!(i == argc - 1))	{
		    /*  Make sure it isn't the last parameter.
		     */
		    strcpy (&src[k].file[0], argv[i]);	
		    k++;
		    s_mfile++;
		}
	    }
	}
    }


    /* Can we contact the DTS server at all? 

       if (dts_hostPing (host) != OK) {
           fprintf (stderr, 
		"Error: Cannot contact DTSCP destination host '%s'\n", host);
           return (ERR);
       } 
       if (dts_hostPing (srchost) != OK) {
           fprintf (stderr, 
		"Error: Cannot contact DTSCP source host '%s'\n", host);
           return (ERR);
       } 
    */

    if (argc == 1)
	interactive++, doShell++;

    /*  Simple idiot checking.
     */
    if (exec && !execCmd[0]) {
	fprintf (stderr, "No queue name specified for exec command\n");
	return (1);
    }

    /*  Initialize the DTS interface.  Use dummy args since we aren't setting
     *  up to be a DTS server process, but this allows us to load the config
     *  file and initialize the 'dts' struct with local information.
     */
    strcpy (cwd, dts_localCwd ());
    strcpy (localIP, dts_getLocalIP ());
    dts = dtsInit (config, cmdPort, localIP, "./", 0);
    dts->debug = debug;
    dts->verbose = verbose;


    /*  Initialize the DTS server methods we'll need.  Note, this isn't a 
     *  complete server command set, only those we need to support the 
     *  commands we can send (e.g. data transfer).
     */
    dts_cpServerMethods (dts);

    /*  Reads from a source file list (from_file provides the path) and adds 
     *  to the array of src structs which we will loop through later. 
     *
     *  This loads up our toPath and toHost, because they will only ever be 
     *  one target.  This was assumed before for from, which caused a huge
     *  rework of the code.
     */
    if ((ip = strchr (to, (int) ':'))) {
	strcpy (toPath, ip + 1);
	strncpy (dst.host, to, ip - to);
    } else {
	strcpy (toPath, to);
	strcpy (dst.host, localIP);
	dst.dtscp_dst = 1;
    }

    int add_files = s_mfile;
    int *addfile = &add_files;
    if (strlen (from_file))
	dts_cpListFromFile (from_file, src, addfile);
    s_mfile = add_files;	/*  Add to our total source multiple files, 
				 *  because we just obtained a few more.
				 */

    /*  We need to get the config information for the files that have been 
     *  loaded from a file.
     */

    /*  Loop through the multiple files and copy the remotehost, resolve the 
     *  remote host using ResolvefromConfig() which resolves the port, ip 
     *  address and host from the /.dts_config file. If the ip doesn't get
     *  resolved then resolve using resolveHost.
     * 
     *  Start on the struct array after add_files because we already loaded
     *  in a few of the files from a filelist. 
     */
    for (i = 0; i < s_mfile; i++) {
	if (strchr (src[i].file, (int) ':') != NULL) {
	    /* If we have a remote host
	     ** Copy over the host in src_files, but save the host in src_hosts.
	     */
	    memcpy (src[i].host, src[i].file,
		    strchr (src[i].file, (int) ':') - &src[i].file[0]);
	    memcpy (src[i].file, strchr (src[i].file, (int) ':') + 1,
		    strlen (src[i].file));
	    src[i].dtscp_src = 0;

	} else {
	    /*  If host isn't specified it is classified as LOCAL_DTSCP.
	     *  If you wanted to use the host then you'd put your node name
	     *  For example using DTSCP on a computer running DTSD
	     *  Say you run DTSCP on Denali, if you want to send using DTSCP
	     *  You'd type in /foo/file as a cmd arg but if you wanted to 
	     *  send using the DTSD running on the machine you'd use the 
	     *  alias for the machine denali:/foo/file
	     */
	    strcpy (src[i].host, localIP);
	    src[i].dtscp_src = 1;
	}

	/*  Now resolve the hostnames and ports from the DTS config 
	 *  and using the correct port if we are a DTSCP client 
	 */
	dts_cpResolvefromConfig (dts, &src[i], &dst);


	/*  If we couldn't resolve them from the config, resolve using 
	 *  dts_resolveHost. 
	 */
	if (strlen (dst.ip) == 0)
	    strcpy (dst.ip, dts_resolveHost (dst.host));

	if (strlen (src[i].ip) == 0)
	    strcpy (src[i].ip, dts_resolveHost (src[i].host));

    }

    /*  Check our command to see if it is a remotenode:*.file
     *  This way we can get a directory listing of the remote server
     *  and know which files we need to send a request for.
     *  If this was local *.file bash would fill this information 
     *  in for us which was done above in the argument handling for main.
     *  Since our hosts have been resolved we'll need to replace 
     *  the current *.file line and expand src_files to include the new
     *  files that have to be transfered.
     */
    char *cmode;
    for (i = 0; i < s_mfile; i++) {
	/*  If a user inputs img\*  using bash it will be inputted as img*.
         */
	if (strchr (src[i].file, (int) '*') != NULL
	    && strstr (src[i].file, "*.") == NULL)
	    /* change to img/  */
	    src[i].file[strchr (src[i].file, (int) '*') - src[i].file] = '/';	

	else if ((strstr (src[i].file, "*.") != NULL) && 
	    (strstr (src[i].file, "/*.") == NULL)) {	

	    /*  If a user inputs img\*.fits using bash it will be inputted 
	     *  as 'img*.fits'.  Expand our string 1 character -> to fit 
	     *  in a "/".
	     */
	    memset (buf, 0, SZ_LINE);
	    strcpy (buf, src[i].file);
	    strcpy (strstr (src[i].file, "*.") + 1, strstr (buf, "*."));
	    src[i].file[strstr (src[i].file, "*.") - src[i].file - 1] = '/';
	    //  Now will be 'img/*.fits'  instead of 'img*.fits'
	}


	//  Just in case someone puts in "\*" instead of "/*"  
	if (strstr (src[i].file, "\\*") != NULL)
	    src[i].file[strstr (src[i].file, "\\*") - src[i].file] = '/';

	//  Replace all "/*" that don't have a setup like this /*. or /*.fits
	if ((strstr (src[i].file, "/*") != NULL))
	    src[i].file[strstr (src[i].file, "/*") - src[i].file] = '\0';

	//  Replace  "*" 
	if (src[i].file[0] == '*' && src[i].file[1] != '.')
	    src[i].file[0] = '/';

	src[L].copymode = 0;

	if (!src[L].dtscp_src) {
	    host = src[L].host;
	    cmode = dts_hostGet (host, "dts", "copyMode");
	    if (strcasecmp ("normal", cmode) != 0)
		src[L].copymode = 1;
	    host = dst.host;			/* Set host back to dst.host */
	}

	/*  Put in copy mode into the fromPath.
	 */
	if (!src[L].copymode && !qname && !src[L].dtscp_src) {
	    strcpy (buf, src[L].file);
	    if (buf[0] == '/')
		sprintf (src[L].file, "/copy%s", &buf[0]);
	    else
		sprintf (src[L].file, "/copy/%s", &buf[0]);
	}
    }


    int lastentryline = s_mfile;
    int *lastentry = &lastentryline;

    for (i = 0; i < s_mfile; i++)
	dts_cpCheckForRemoteWildcard (&src[i], lastentry, i, localIP);
    s_mfile = lastentryline;

    /*   Set any debug or verbose params in the server as well.
     */

    host = dst.host;			/* target host          */
    if (!host) {
	fprintf (stderr, "Error:  No target host specified.\n");
	exit (1);
    }
    if (verbose)
	dts_hostSet (host, "dts", "verbose", "true");
    if (debug)
	dts_hostSet (host, "dts", "debug", "true");


    if (!doShell) {

	/*  Build the one command string we'll use.
	 */
	cmode = dts_hostGet (host, "dts", "copyMode");

	if ((qname) && (strcasecmp ("normal", cmode) == 0)) {
	    ddir = dts_hostGetQueueDir (host, qname);
	    if (exec)
		cdir = dts_hostGetCopyDir (host);
	} else {
	    if (strcasecmp ("normal", cmode) == 0) {
		ddir = "copy";
		dts->copyMode = 0;
	    } else {
		ddir = "";
		dts->copyMode = 1;
	    }
	}

	dst.copymode = 0;			/*  initialize to 0	*/
	host = dst.host;
	cmode = dts_hostGet (host, "dts", "copyMode");
	if (strcasecmp ("normal", cmode) != 0)
	    dst.copymode = 1;

	/*  Loop through multiple files that need to be sent.
	 *  We use the strings fromPath and toPath to send the information.
	 */
	strcpy (savedtoPath, toPath);
	for (L = 0; L < s_mfile; L++) {
	    /*  Multiple source hosts can be used so let us loop through 
	     *  them all. 
	     */
	    strcpy (toPath, savedtoPath);

	    /*  If the file is on the source machine from dtscp
	     *  e.g.   % dtscp ./foo/ tucana:
	     *  If it's local -> remote
	     */
	    if (src[L].dtscp_src) {
		if ((realpath (src[L].file, NULL) == NULL)) {
		    fprintf (stderr, "Could not find file: %s \n", src[L].file);
		    continue;
		} else {
		    /*  Now read into src_files
		     */
		    strcpy (&src[L].file[0], realpath (src[L].file, NULL));	
		    sprintf (fromPath, "%s", src[L].file);
		}

	    } else {
	        /*  If it's remote -> remote or remote -> local
	        */
		strcpy (fromPath, &src[L].file[0]);
	    }

	    if (fromPath[0] != '/')
		sprintf (fromPath, "/%s", src[L].file);

	    /*  Checking to see if we are copying directories.
	     *  For example someone might want to move a file -> file
	     *  or file -> directory, directory -> directory but never 
	     *  a directory -> file.
	     */
	    if ((strcmp (src[L].ip, localIP) == 0) && src[L].dtscp_src) {
		/*  If the path is a directory, correct in the command string.
		 */
		if (dts_isDir (fromPath) && 
		    strrchr (fromPath, (int) '/') != NULL) {
		        /*  Make sure there is a / on the end.
		         */
		    if ((strrchr (fromPath, (int) '/') - fromPath) !=
			(strlen (fromPath) - 1))
			strcpy (fromPath + strlen (fromPath), "/");
		    fpathdir = 1;
		}
		fpathlocal = 1;

	    } else {
		/*  If the path is a directory correct in the command string.
		 */
		if (dts_hostIsDir (src[L].host, fromPath) && 
		    strrchr (fromPath, (int) '/') != NULL) {
		        /*  Make sure there is a / on the end.
		         */
		        if ((strrchr (fromPath, (int) '/') - fromPath) !=
			    (strlen (fromPath) - 1))
			        strcpy (fromPath + strlen (fromPath), "/");
		        fpathdir = 1;
		}
		fpathlocal = 0;
	    }


	    if (strcmp (dst.ip, localIP) == 0) {
		/*  If the path is a directory, correct in the command string.
		 */
		if (dts_isDir(toPath) && (strrchr(toPath, (int) '/') != NULL)) {
		    /*  Make sure there is a / on the end.
		     */
		    if ((strrchr (toPath, (int) '/') - toPath) !=
			(strlen (toPath) - 1))
			    strcpy (toPath + strlen (toPath), "/");
		    tpathdir = 1;
		}
		tpathlocal = 1;
	    } else {
		/*  It might not exist so dts_hostIsDir could return 0.
		 */
		if (dts_hostIsDir (dst.host, toPath) && 
		   (strrchr (toPath, (int) '/') != NULL)) {  // Check for a '/'
		    /*  Make sure there is a / on the end.
		     */
		    if ((strrchr (toPath, (int) '/') - toPath) !=
			(strlen (toPath) - 1))
			    strcpy (toPath + strlen (toPath), "/");
		    tpathdir = 1;
		}
		tpathlocal = 0;
	    }

	    if (s_mfile > 1)		/* multiple files?		*/
		tpathdir = 1;		/* Saving to a directory.	*/

	    /* If we are receiving on DTSCP */
	    if (dst.dtscp_dst) {
		/*  If our source is a folder dest must also be folder.
		 */
		if (fpathdir == 1) {
		    memset (buf, 0, SZ_LINE);
		    if (toPath != basename (toPath))
			sprintf (buf, "%s/%s", cwd, toPath);
		    else
			sprintf (buf, "%s/", cwd);

		    strcpy (toPath, buf);
		    if ((strrchr (toPath, (int) '/') - toPath) !=
			(strlen (toPath) - 1))
			strcpy (toPath + strlen (toPath), "/");

		} else if (tpathdir) {
		    /*  or to directory that needs to be resolved. 
		     */
		    memset (buf, 0, SZ_LINE);
		    strcpy (buf, toPath);
		    realpath (buf, toPath);

		} else if ((realpath (toPath, NULL) == NULL)) {
		    /*  or its a file */
		    fprintf (stderr, "Could not find file: %s \n", toPath);

		} else {
		    /*  Now read into src_files
		     */
		    strcpy (toPath, realpath (toPath, NULL));	
		}
	    }

	    /*  If multiple files.	
	     */
	    if ((s_mfile > 1) && (fpathdir == 0)) {
		if ((strrchr (toPath, (int) '/') - toPath) !=
		    (strlen (toPath) - 1))
		    strcpy (toPath + strlen (toPath), "/");

		memset (buf, 0, SZ_LINE);
		strcpy (buf, fromPath);

		/*  Append file name,  treat topath is a directory. 	
		*/
		sprintf (toPath, "%s%s", toPath, basename (buf));

	    } else if (fpathdir == 0) {
		memset (buf, 0, SZ_LINE);
		strcpy (buf, fromPath);

		/* Append file name.  	*/
		sprintf (toPath, "%s/%s", toPath, basename (buf));	
		if (toPath[0] == '.' && strlen (toPath) > 2)
		    strcpy (toPath, &toPath[1]);   // case dirname returns '.'
	    }

	    if (fpathdir == 1) {
		/*  At least 1 path was a folder so we are treating the TO 
		 *  as a folder path. Because it's stupid to transfer a
		 *  folder to a file (impossible)  make sure there is a 
		 *  trailing '/'.
		 */
		if ((strrchr (toPath, (int) '/') - toPath) !=
		    (strlen (toPath) - 1))
		    strcpy (toPath + strlen (toPath), "/");

		if (src[L].dtscp_src) {
		    /*  This is for local to remote. We need to append the
		     *  top level folder /home/semple/topfolder/ to our 
		     *  toPath.
		     */
		    memset (buf, 0, SZ_LINE);
		    strcpy (buf, fromPath);
		    sprintf (toPath, "%s%s/", toPath, basename (buf));
		}
	    }

	    /*  If no toPath, and it isn't a directory, load in the base
	     *  filename.
	     */
	    if ((strlen (toPath) == 0) && (fpathdir == 0)) {
		memset (buf, 0, SZ_LINE);
		strcpy (buf, fromPath);
		sprintf (toPath, "/%s", basename (buf));
	    }

	    /*  Check if we are sending to a queue.  If so, do all the 
	     *  control setup to send to a queue.  Check copymode isn't 
	     *  being used.
	     */
	    if ((qname) && (!dst.copymode)) {
		char temp_fromPath[SZ_LINE];


		memset (temp_fromPath, 0, SZ_LINE);
		strcpy (temp_fromPath, fromPath);

#ifdef HOST_ACCESS
		if ((dts_hostAccess (src[L].host, temp_fromPath, R_OK) != 0)
		    && !src[L].dtscp_src) {
		    printf ("Failed to access %s\n", temp_fromPath);
		    return (ERR);

		}
#endif
		if (dts_hostInitTransfer (dst.host, qname, 
		    temp_fromPath, queuePath) != OK) {
			if (debug)
		            fprintf (stderr, "Queue init transfer failure.\n");
		        return (ERR);
		}

		if (src[L].dtscp_src) {
		    dts_qInitControl (dst.host, temp_fromPath);
		    if (debug) fprintf (stderr, "called Qinitcontrol\n");
		}

		/* FIXME -- Support for initControl for remote to remote
		 * or Remote to X is on hold
		 * We need a remote CRC method for this to work correctly.
		 */


	    } else if ((qname) && dst.copymode) {
		fprintf (stderr,
		    "Cannot use queues when --copymode flag is used "
		    "on the destination. \n");
		return (ERR);
	    }

	    /*  Qstuff hacks off our ending slash so add if recursive.
	     *  Make sure frompath doesn't have any starting //.
	     */
	    while (1) {
		if (strstr (fromPath, "//") == fromPath)
		    strcpy (fromPath, &fromPath[1]);
		else
		    break;
	    }
	    if (!strlen (toPath))
		strcpy (toPath, "/");


	    /*  APPEND OUR /copy/ to our directory here if copymode isn't on.
	     */
	    if (!dst.copymode && !qname && !dst.dtscp_dst) {
		memset (buf, 0, SZ_LINE);
		strcpy (buf, toPath);
		if (buf[0] == '/')
		    sprintf (toPath, "/copy%s", buf);
		else
		    sprintf (toPath, "/copy/%s", buf);
	    }

	    if (debug) {
		fprintf (stderr, "fromHost='%s'   toHost='%s'\n", 
		    src[L].host, dst.host);
		fprintf (stderr, "fromPath='%s'   toPath='%s'\n", 
		    fromPath, toPath);
		fprintf (stderr, "   alias='%s' --> '%s'\n", alias, host);
		if (s_mfile - 1)
		    fprintf (stderr, "Number of source extra files: %d\n",
			     s_mfile);
		if (qname)
		    fprintf (stderr, "To queue name: %s\n", qname);
	    }


	    /*  Build the command from the parameters we are given. 
	     */
	    dts_cpBuildCmd (&src[L], fromPath, &dst, toPath, queuePath);
	    if ((res = dts_cpProcCmd (cmdbuf)) == OK) {
		if (exec) {
		    sprintf (execBuf, "%s %s/%s", execCmd, cdir, toPath);
		    if (remoteDest) {
			execStat = dts_hostExecCmd (host, ddir, execBuf);
			if (execStat != OK) {
			    if (verbose) 
			        fprintf (stderr,
				     "Error executing remote command\n");
			    res = execStat;
			}
		    } else
			system (execBuf);
		}
	    }

	}

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
		len = strlen (cmdline);		/* kill newline              */
		cmdline[len - 1] = '\0';
		dts_cpProcCmd (cmdline);	/* process command           */
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
 *  DTS_CPBUILDCMD -- Build the command string to process.
 */
static void
dts_cpBuildCmd (srcStruct * mysrc, char *fPath, destStruct * mydst,
		char *tPath, char *qPath)
{
    char src[SZ_LINE], dest[SZ_LINE], thost[SZ_LINE];


    memset (src, 0, SZ_LINE);
    memset (dest, 0, SZ_LINE);
    memset (thost, 0, SZ_LINE);
    memset (cmdbuf, 0, SZ_SBUF);


    sprintf (src, "%s:%s", mysrc->host, fPath);
    sprintf (dest, "%s:%s", mydst->host, tPath);

    xArgs[0] = calloc (1, SZ_LINE);
    xArgs[1] = calloc (1, SZ_LINE);

    if (!qname) {
	strcpy (xArgs[0], src);
	strcpy (xArgs[1], dest);
    } else {
	// Check if submitting directory to queue?
	sprintf (xArgs[0], "%s:%s", mysrc->host, fPath);
	sprintf (xArgs[1], "%s:%s%s", mydst->host, qPath, tPath);
    }
    nXArgs = 2;


    if ((strcmp (mysrc->ip, localIP) == 0) && (mysrc->dtscp_src))
	sprintf (cmdbuf, "%s %s %s", (!try_pull ? "push" : "give"), src, dest);
    else
	sprintf (cmdbuf, "%s %s %s", (!try_pull ? "pull" : "take"), src, dest);


    if (strcmp (mysrc->ip, localIP) == 0) {
	if (!try_pull)
	    host = mydst->host;		// push
	else
	    host = mysrc->host;		// give
    } else {
	if (!try_pull)
	    host = mydst->host;		// pull
	else
	    host = mysrc->host;		// take
    }

    if (debug)
	fprintf (stderr, "cmd = '%s'\n", cmdbuf);
}


/**
 *  DTS_CPPROCCMD -- Process a DTS shell command line, e.g. as when reading
 *  from a script file.  Break the command into argc/argv before processing.
 */
static int
dts_cpProcCmd (char *cmdbuf)
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

	memset (buf, 0, SZ_LINE); 		/* pull out the token 	*/
	quote = nq = (*ip == '"');
	if (quote)
	    ip++;

	while (*ip && isspace (*ip))
	    ip++;

	for (op = buf; *ip;) {
	    if (!quote && isspace (*ip))
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
	    /* Argument substitution.
	     */
	    anum = atoi (&buf[1]);
	    memset (buf, 0, SZ_LINE);
	    strcpy (buf, subArg[anum]);
	}

	argv[argc] = calloc (1, SZ_LINE);
	strcpy (argv[argc++], buf);
    }

    status = dts_cpProcess (argc, argv);

    for (i = 0; i < argc; i++)
	if (argv[i])
	    free ((char *) argv[i]);

    return (status);
}


/**
 *  DTS_CPPROCESS -- Process a DTS shell command.  In this version, we
 *  use the "standard" argc/argv to pass in the command and arguments.
 *  Argument substitution has already been done by the caller.
 */
static int
dts_cpProcess (int argc, char *argv[])
{
    long size, astart = 1;
    struct timeval s_time, e_time;
    xferStat xfs;


    /**
     *    Process the command.
     */
    cmd = argv[0];
    astart = 1;

    gettimeofday (&s_time, NULL);


    if (cmd[0] == '#' || cmd[0] == '\n') {		/* comment line      */
	return (OK);

    } else if (strcasecmp (cmd, "sleep") == 0) {	/* quit              */
	sleep (atoi (argv[1]));

    } else if (strcasecmp (cmd, "port") == 0) {		/* port              */
	dtsFree (dts);
	dts = dtsInit (config, (cmdPort = atoi (argv[1])), localIP, "./", 0);

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
    } else if (strcasecmp (cmd, "prealloc") == 0) {	/* PREALLOC   */
	path = argv[astart++];
	size = atoi (argv[astart]);
	res = (noop ? OK : dts_hostPrealloc (host, path, size));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));

    } else if (strcasecmp (cmd, "fget") == 0) {	/* FGET       */
	int nb;
	char *local;

	path = argv[astart++];
	local = argv[astart++];
	nb = atoi (argv[astart]);
	res = (noop ? OK : dts_hostFGet (host, path, local, nb));
	if (!quiet)
	    printf ("(%d) %s\n", res, (res == OK ? "OK" : "ERR"));



    /*************************************************************************
    **  File Transfer Commands
    *************************************************************************/

    } else if (strcasecmp (cmd, "give") == 0) {	/* GIVE       */
	nXArgs = dts_cpPatchArgv (host, XFER_TO, 2, &xArgs[0]);
	if (debug)
	    fprintf (stderr, "GIVE %s %s\n", xArgs[0], xArgs[1]);
	res = (noop ? OK : dts_hostTo (host, cmdPort, method, rate,
				       loPort, hiPort,
				       nThreads, XFER_PUSH,
				       nXArgs, &xArgs[0], &xfs));
	if (!quiet)
	    printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		    (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		    xfs.tput_MB, ((double) xfs.fsize / MBYTE), cmd);

	if (qname) 		/*  if we sent to a queue, close it */
	    dts_hostEndTransfer (dst.host, qname, queuePath);

	if ((res == OK) && (move_flag))
	    dts_cpUnlink (cmd, fromPath);	//uses Xargs


    } else if (strcasecmp (cmd, "push") == 0) {	/* PUSH       */
	nXArgs = dts_cpPatchArgv (host, XFER_TO, 2, &xArgs[0]);
	if (debug)
	    fprintf (stderr, "PUSH %s %s\n", xArgs[0], xArgs[1]);
	res = (noop ? OK : dts_hostTo (host, cmdPort, method, rate,
				       loPort, hiPort,
				       nThreads, XFER_PULL,
				       nXArgs, &xArgs[0], &xfs));
	if (!quiet)
	    printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		    (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		    xfs.tput_MB, ((double) xfs.fsize / MBYTE), cmd);

	if ((res == OK) && (move_flag))
	    dts_cpUnlink (cmd, fromPath);	//uses Xargs

	if (qname) 		/*  if we sent to a queue, close it */
	    dts_hostEndTransfer (dst.host, qname, queuePath);

    } else if (strcasecmp (cmd, "take") == 0) {	/* TAKE       */
	nXArgs = dts_cpPatchArgv (host, XFER_FROM, 2, &xArgs[0]);
	if (debug)
	    fprintf (stderr, "TAKE %s %s\n", xArgs[0], xArgs[1]);
	res = (noop ? OK : dts_hostFrom (host, cmdPort, method, rate, 
					 loPort, hiPort, nThreads, XFER_PULL,
					 nXArgs, &xArgs[0], &xfs));
	if (!quiet)
	    printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		    (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		    xfs.tput_MB, ((double) xfs.fsize / MBYTE), cmd);

	if (qname) 		/*  if we sent to a queue, close it */
	    dts_hostEndTransfer (dst.host, qname, queuePath);

	if ((res == OK) && (move_flag))
	    dts_cpUnlink (cmd, fromPath);	//uses Xargs


    } else if (strcasecmp (cmd, "pull") == 0) {	/* PULL       */
	nXArgs = dts_cpPatchArgv (host, XFER_FROM, 2, &xArgs[0]);
	if (debug)
	    fprintf (stderr, "PULL %s %s\n", xArgs[0], xArgs[1]);
	res = (noop ? OK : dts_hostFrom (host, cmdPort, method, rate,
					 loPort, hiPort, nThreads, XFER_PUSH,
					 nXArgs, &xArgs[0], &xfs));
	if (!quiet)
	    printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes [%s]\n",
		    (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
		    xfs.tput_MB, ((double) xfs.fsize / MBYTE), cmd);

	if (qname) 		/*  if we sent to a queue, close it */
	    dts_hostEndTransfer (dst.host, qname, queuePath);

	if ((res == OK) && (move_flag))
	    dts_cpUnlink (cmd, fromPath);	//uses Xargs


    } else {
	/*  Command Not Found
	 */
	if (!quiet)
	    fprintf (stderr, "Command '%s' is not supported.\n", cmd);
	return (1);
    }


    /*  Get timing information.
     */
    gettimeofday (&e_time, NULL);
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
    dts_cpFreeXArgv ();
    return (res);
}


/**
 *  DTS_CPPATCHARGV --  Given the commandline arguments for the task, 
 *  convert these to forms used by the DTS.
 */
static int
dts_cpPatchArgv (char *host, int dir, int argc, char *argv[])
{
    register int i;
    int have_src = 0, have_dest = 0;
    char *src = NULL, *dest = NULL, first;
    char *cwd, buf[SZ_PATH];


    memset (buf, 0, SZ_PATH);	/* get current working directory  */
    cwd = getcwd (buf, (size_t) SZ_PATH);
    nXArgs = argc;

    for (i = 0; i < argc; i++) {

	first = argv[i][0];
	if (first == '-' || (isdigit (first) && !strchr (argv[i], (int) ':'))) {
	    xArgs[i] = calloc (1, strlen (argv[i] + 1));
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
		    sprintf (xArgs[i], "%s:%s", host, dts_pathFname (fname));
		dest = xArgs[i];
		have_dest++;

	    } else {
		if (strchr (fname, (char) ':'))
		    strcpy ((src = xArgs[i]), fname);
		else if (dir == XFER_TO) {	/*  push/give  */
		    sprintf (xArgs[i], "%s:%s/%s", dts_getLocalIP (), cwd,
			     fname);
		    src = dts_pathFname (fname);
		} else if (dir == XFER_FROM) {	/*  pull/take  */
		    sprintf (xArgs[i], "%s:/%s", dts_resolveHost (host), fname);
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
 *  DTS_CPFREEXARGV --  Free the patched argument vector.
 */
static void
dts_cpFreeXArgv ()
{
    register int i;

    for (i = 0; i < nXArgs; i++)
	free ((void *) xArgs[i]);
}


/** 
 *  DTS_CPRESOLVEFROMCONFIG -- Resolves hostnames from the DTS config, 
 *  setting the port appropriately depending on if DTSCP is the local or 
 *  remote target.
 *
 *  WARNING USES FROMPORT which is external.
 */
static void 
dts_cpResolvefromConfig (DTS * dts, srcStruct *src, destStruct *dst)
{
    register int i;

    for (i = 0; i < dts->nclients; i++) {
	if ((strcmp (src->host, dts->clients[i]->clientName) == 0) ||
	    (strcmp (src->host, dts->clients[i]->clientIP) == 0)) {

	    if (src->dtscp_src)
		fromPort = DTSCP_PORT;
	    else
		fromPort = dts->clients[i]->clientPort;

	    strcpy (src->ip, dts->clients[i]->clientIP);
	    strcpy (src->host, dts->clients[i]->clientHost);
	    if (strchr (src->host, (int) ':') == NULL)
		sprintf (src->host, "%s:%d", src->host, fromPort);
	    break;
	}
    }

    for (i = 0; i < dts->nclients; i++) {
	if ((strcmp (dst->host, dts->clients[i]->clientName) == 0) ||
	    (strcmp (dst->host, dts->clients[i]->clientIP) == 0)) {

	    if (dst->dtscp_dst)
		toPort = DTSCP_PORT;
	    else
		toPort = dts->clients[i]->clientPort;

	    strcpy (dst->host, dts->clients[i]->clientHost);
	    strcpy (dst->ip, dts->clients[i]->clientIP);
	    if (strchr (dst->host, (int) ':') == NULL)
		sprintf (dst->host, "%s:%d", dst->host, toPort);
	    break;
	}
    }
}


/**
 *  DTS_CPSERVERMETHODS -- Initialize the server-side of the DTS.  Create
 *  an XML-RPC server instance, define the commands and start the thread.
 */
static void
dts_cpServerMethods (DTS * dts)
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
    xr_addServerMethod ("ping", dts_Ping, NULL);
    xr_addServerMethod ("diskFree", dts_DiskFree, NULL);
    xr_addServerMethod ("diskUsed", dts_DiskUsed, NULL);

    /* Start the server thread.
     */
    xr_startServerThread ();
}


/**
 *  DTS_CPCHECKFORREMOTEWILDCARD -- 
 */
static void 
dts_cpCheckForRemoteWildcard (srcStruct *mysrc, int *lastent, int i, 
	char *lclIP)	
{
    int L = 0;
    static char tmpr[SZ_LINE];		/* has to be static or segfault */
    char *sres = (char *) NULL;
    int islong = 0;
    char extension[SZ_LINE];

	
    memset (tmpr, 0, SZ_LINE);

    /* If we get something like *.
     */
    if ((strstr (mysrc[i].file, (const char *) "*.") != NULL)
	&& (strlen (strstr (mysrc[i].file, (const char *) "*.") + 1) ==
	    strlen (mysrc[i].file))) {

	char str[SZ_LINE], *s = str, *t = NULL;


	//  Copy the path before *. e.g.: /foo/bar/*. -> /foo/bar/
	strncpy (tmpr, mysrc[i].file,
		 (strlen (mysrc[i].file) -
		  strlen (strstr (mysrc[i].file, (const char *) "*"))));
	sres = dts_hostDir (mysrc[i].host, tmpr, islong);

	memset (str, 0, SZ_LINE);
	strcpy (str, sres);

	while ((t = strtok (s, "\n")) != NULL) {
	    s = NULL;
	    if (debug)
		printf ("%s\n", t);

	    /*  If it contains a '.'
	     */
	    if (strrchr (t, (int) '.') != NULL) {
		if (L == 0) {
		    strcpy (&mysrc[i].
			    file[(strstr (mysrc[i].file, (const char *) "*")) -
				 mysrc[i].file], t);
		} else {
		    strcpy (mysrc[*lastent].host, mysrc[i].host);
		    strcpy (mysrc[*lastent].ip, mysrc[i].ip);
		    mysrc[*lastent].dtscp_src = mysrc[i].dtscp_src;
		    mysrc[*lastent].copymode = mysrc[i].copymode;
		    sprintf (mysrc[*lastent].file, "%s%s", tmpr, t);

		    (*lastent)++;	//increment source multiple files
		}
		L++;		//next element in dirfiles.
	    }
	}
	if (L == 0) {
	    fprintf (stderr, "No files found.\n");
	    return;
	}

    } else if ((strstr (mysrc[i].file, (const char *) "*.") != NULL)) {

	char str[SZ_LINE], *s = str, *t = NULL;

	/*  If our fromhost isn't local. and IF our fromPath has *. inside 
	 *  of it.  If we get *.<something>
	 */

	memset (extension, 0, SZ_LINE);

	/*  Copy information after *.  ex *.fits -> fits
	 */
	strcpy (extension, (strstr (mysrc[i].file, (const char *) "*.") + 2));
	printf ("extension: %s\n", extension);

	//  Copy the path before *. e.g.: /foo/bar/*.fits -> /foo/bar/
	strncpy (tmpr, mysrc[i].file,
		 (strlen (mysrc[i].file) -
		  strlen (strstr (mysrc[i].file, (const char *) "*."))));
	sres = dts_hostDir (mysrc[i].host, tmpr, islong);

	/*  Sort through sres string looking for files that fit our *. extn.
	 *  sres prints out the nodename\n<dir>\nlol.foo\n<dir> etc. at the
	 *  root in a directory: <dir>\n<file>\n
	 */

	memset (str, 0, SZ_LINE);
	strcpy (str, sres);
	while ((t = strtok (s, "\n")) != NULL) {
	    s = NULL;
	    if (debug)
		printf ("%s\n", t);

	    /*  If it contains a . and has the same matching extension.
	     */
	    if ((strrchr (t, (int) '.') != NULL)
		&& (strcmp (extension, strrchr (t, (int) '.') + 1)) == 0) {
		if (L == 0) {
		    strcpy (&mysrc[i].
			    file[(strstr (mysrc[i].file, (const char *) "*.")) -
				 mysrc[i].file], t);
		    //memset (&src_files, 0, SZ_LINE);
		} else {
		    strcpy (mysrc[*lastent].host, mysrc[i].host);
		    strcpy (mysrc[*lastent].ip, mysrc[i].ip);
		    mysrc[*lastent].dtscp_src = mysrc[i].dtscp_src;
		    sprintf (mysrc[*lastent].file, "%s%s", tmpr, t);

		    (*lastent)++;	//increment source multiple files
		}
		L++;		//next element in dirfiles.
	    }
	}
	if (L == 0) {
	    printf ("No files found that matched the extension.\n");
	    return;
	}
    }
}


/**
 *  DTS_CPLISTFROMFILE -- Load the source struct from a file.
 */
static void
dts_cpListFromFile (char *from_file, srcStruct *my_src, int *add_f)
{
    char line[SZ_LINE];		/* or other suitable maximum line size */
    int lin_num = (*add_f);


    if (from_file && strlen (from_file)) {
	FILE *file = (FILE *) NULL;

	if (verbose)
	    fprintf (stderr, "Loaded from filename: %s\n", from_file);
	if ((file = fopen (from_file, "r")) != NULL) {

	    /*  Loop through the file.
	     */
	    while (fgets (line, sizeof line, file) != NULL) {	

		if ((line != NULL) || (strlen (line) > 1))
		    line[strlen (line) - 1] = '\0';

		/*  Check for host specification, e.g. tucana:/home/data etc.
		 */
		if ((strstr (line, ":")) != NULL) {
		    strcpy (my_src[lin_num].file, line);

		    (*add_f)++;
		    ++lin_num;

		} else if (realpath (line, NULL) == NULL) {
		    printf ("Could not find file: %s \n", line);
		    continue;

		} else {
		    /*  Now read into src_files.
		     */
		    strcpy (my_src[lin_num].file, realpath (line, NULL));	
		    if (debug)
			fprintf (stderr, "srcfiles: %s\n",
				 my_src[lin_num].file);

		    (*add_f)++;	//increment multiple file sender    
		    ++lin_num;
		}
	    }
	    fclose (file);

	} else
	    perror (from_file);		/* why didn't the file open? 	*/
    }
}


/**
 *  DTS_CPUNLINK -- Unlink local or remote files and folders.
 */
static void 
dts_cpUnlink (char *cmd, char *path)
{
    char error = 0;
    /* TODO PROTECT queue directories  */


    if ((strcasecmp (cmd, "give") == 0) || (strcasecmp (cmd, "push") == 0)) {
	/*  Don't allow just "/" 
	 */
	if ((strlen (path) > 1) && (remove (path) != 0)) {	
	    if (dts_localDelete (path, 1) != 0)
		error = 1;
	}

    } else if ((strcasecmp (cmd, "pull") == 0)
	    || (strcasecmp (cmd, "take") == 0)) {

	char host[SZ_LINE];

	memset (host, 0, SZ_LINE);
	if (((strlen (path) > 1)) &&
	    !(strstr (fromPath, "/copy/") != NULL && strlen (fromPath) == 6) &&
	    !(strstr (fromPath, "/spool/") != NULL && strlen (fromPath) == 7)) {

	    /*  Copy the source host.
	     */
	    strncpy (host, xArgs[0], 
		strchr (xArgs[0], (int) ':') - xArgs[0]);	

	    if (dts_hostDel (host, fromPath, DEF_PASSWD, 1) == 0)
		fprintf (stderr, "Path: %s unlinked. \n", path);
	    else
		error = 1;
	} else
	    error = 1;
    }

    if (verbose || debug) {
        if (error == 0)
	    fprintf (stderr, "Path: %s unlinked. \n", path);
        else
	    fprintf (stderr, "ERROR Path: %s  couldn't be unlinked. \n", path);
    }
}


/**
 *   DTS_QINITCONTROL -- Initialize the control structure.
 */
static void 
dts_qInitControl (char *qhost, char *fname)
{
    char *md5 = NULL, opath[SZ_PATH];


    /*  FIXME -- This needs fixing so it works with remote.
     */

    memset (opath, 0, SZ_PATH);
    sprintf (opath, "%s!%s", dts_getLocalHost (), fname);

    /*  Initialize the queue control file on the target machine.
     */
    strcpy (control.queueHost, dts_getLocalHost ());
    strcpy (control.queueName, qname);
    strcpy (control.filename, dts_cpFname (fname));
    strcpy (control.xferName, dts_pathFname (fname));
    strcpy (control.deliveryName, dts_pathFname (fname));
    strcpy (control.srcPath, dts_pathDir (fname));
    strcpy (control.igstPath, opath);

    if ((md5 = dts_fileMD5 (fname))) {
	strcpy (control.md5, md5);
	free ((void *) md5);
    } else
	strcpy (control.md5, " \0");

    control.epoch = time (NULL);
    control.isDir = dts_isDir (fname);

    if (control.isDir == 1)
	control.sum32 = control.crc32 = 0;
    else
	control.sum32 = dts_fileCRCChecksum (fname, &control.crc32);
    control.fsize = dts_du (fname);

    /*  Call the method.
     */
    dts_hostSetQueueControl (qhost, queuePath, &control);
}


/**
 *  DTS_CPFNAME -- Get the FILENAME keyword value, else return the input
 *  filename.
 */
static char *
dts_cpFname (char *fname)
{
    static char buf[16738], value[SZ_FNAME], *ip = NULL, *op = value;
    int fd = 0;


    if ((fd = dts_fileOpen (fname, R_OK)) >= 0) {
	memset (buf, 0, 16738);
	if (dts_fileRead (fd, buf, 16738) > 0) {
	    memset (value, 0, SZ_FNAME);
	    if (strncasecmp (buf, "SIMPLE  ", 8) == 0) {
	        if ((ip = strstr (buf, "FILENAME"))) {
		    for (ip += 11; *ip && *ip != '\''; ip++)
		        *op++ = *ip;
	        } else {
		    strcpy (value, fname);
	        }
	    }
	    dts_fileClose (fd);
	    return (value);
	}
    }

    return (fname);
}


/**
 *  DTS_CPUSAGE -- Print task usage information
 */
static void
dts_cpUsage (void)
{
    fprintf (stderr,
	     "Usage:\n"
	     "     dtscp [options] <src> <to>\n" "\n" "Options:\n" "\n" "\n");
}
