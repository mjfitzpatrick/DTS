/**
 *  DTSCP -- Copy a file from one DTS node to another.  
 *
 *  The interface is similar to 'scp' except that instead of providing a 
 *  host name, we use the DTS name as defined in the config file.  Third 
 *  party transfers (i.e. host A tells B to move a file to C) are permitted,
 *  however all hosts must be running a DTS daemon locally and share a 
 *  common config file, (or at least be reachable by a FQDN or host IP.
 *  	The DTS must have read permission on any source files and write 
 *  permission on any destination directories.  Src or Dest paths that begin
 *  with a '/' are assumed to be absolute paths on that host machine, else
 *  they are relative to the DTS root dir of that machine.
 *
 *  Example Usage:
 *
 *    1) Copy local files to a remote DTS at a specific path.
 *
 *	  % dtscp *.fits ncsa:/pipe/stage/071209/
 *	  % dtscp image.fits 140.252.26.30:/pipe/stage/071209/
 *
 *    2) Copy a directory from the CTIO mountain cache to the current directory.
 *
 *	  % dtscp -r ctio_mc:/pipe/stage/071209/ .
 *
 *  Options:
 *
 *	-m push		Transfer using PUSH model (src opens server sockets)
 *	-m pull		Transfer using PULL model (dest opens server sockets)

 *	-P <port>	Number of threads to open (default: 8)
 *	-N <N>		Number of threads to open (default: 8)
 *
 *	-l(og)		log transfer
 *	-p		preserve modification/access times & mode
 *	-r		recursive copy
 *
 *	-d(ebug)	Debug output
 *	-v(erbose)	Verbose output
 *	-q(uiet)	Verbose output
 *	-n(oop)		No-op flag
 */


#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>

#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "dts.h"
#include "dtsPSock.h"

#define	 min(a,b)	"(a<b?a:b)"
#define	 max(a,b)	"(a>b?a:b)"

#define APP_NAME	"dtscp"
#define APP_VERSION	"1.0"

#define XFER_METHOD	"psock"			/* only supported opt 	*/



char   	*fname 		= NULL;			/* file name		*/
long     fsize    	= 0;			/* file size		*/
char	*src_host 	= "127.0.0.1";		/* localhost		*/
char	*dest_host 	= "127.0.0.1";		/* localhost		*/

int	nsrcs		= 0;			/* number of sources	*/
    
/* Options
*/    
int	do_checksums	= 0;			/* checksum stripes	*/
int	mode     	= XFER_PUSH;		/* transfer mode	*/
int	nthreads 	= DEF_NTHREADS; 	/* No. of xfer threads	*/
int	port     	= DEF_XFER_PORT;	/* transfer port	*/
int	recursive    	= 0;			/* recursive flag	*/
int	debug    	= 0;			/* debug flag		*/
int	verbose  	= 0;			/* verbose flag		*/
int	quiet  		= 0;			/* quiet flag		*/
int	noop  		= 0;			/* no-op flag		*/
char   *method		= "xferPush";		/* transfer method	*/
char   *config		= NULL;			/* config file		*/

DTS    *dts		= (DTS *) NULL;
char   *clientUrl;


/**
 *  An 'xFile' is a src file to be transferred or a destination.
 */
typedef struct {
    char  host[SZ_PATH];	/* host name		*/
    char  hostIP[SZ_PATH];	/* host IP string	*/
    char  path[SZ_PATH];	/* path name		*/
    char  file[SZ_FNAME];	/* file name		*/

    int	  isDir;		/* directory		*/
    long  fsize;		/* file size		*/

    void  *next;		/* next file in list	*/
} xFile, *xFileP;


/* Private declarations.
*/
static  xFileP  dtParseArg (char *s, int is_src);

static  char   *dt_getCmdUrl (int mode, xFile *src, xFile *dest);

static  int     dt_initClientParams (char *url, xFile *src, xFile *dest);
static  int     dt_executeTransfer (int client, int *sec, int *usec);

static  void    dt_destValidate (xFile *dest, long totsize);
static  void    dt_srcValidate (xFile *src, long fsize);


/*  Usage:
**
**    dtscp [-P <N>] [-m push|-pull] [-N <N>] [-v] [-q] [-d] <src> <dest>
*/
int 
main (int argc, char **argv)
{
    register int i, j, len, fileno = 0;
    xFile  *src   = (xFile *) NULL,		/* source file list	*/
	   *dest  = (xFile *) NULL,		/* source file list	*/
	   *sp    = (xFile *) NULL,		/* src file 		*/
	   *np    = (xFile *) NULL,		/* next ptr 		*/
	   *slast = (xFile *) NULL;		/* last src file ptr	*/
    int	    sec = 0, usec = 0, client  = -1;
    long    totsize = 0L;




    /* Process command-line arguments.
    */
    for (i=1; i < argc; i++) {
        if (argv[i][0] == '-' && !(isdigit(argv[i][1]))) {
	    len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                switch ( argv[i][j] ) {
                case 'c':   config = argv[++i]; 	break;
                case 'd':   debug++; 			break;
                case 'n':   noop++; 			break;
                case 'q':   quiet++; 			break;
                case 'r':   recursive++; 		break;
                case 'v':   verbose++; 			break;

                case 'N':
		    nthreads = atoi(argv[++i]);
		    break;
                case 'P':
		    port = atoi(argv[++i]);
		    break;
                case 'm':  
		    if (strncasecmp (argv[i], "push", 4) == 0) {
			mode = XFER_PUSH;
			method = "xferPush";
		    } else if (strncasecmp (argv[i], "pull", 4) == 0) {
			mode = XFER_PULL;
			method = "xferPull";
		    } else {
			perror ("Invalid flag not [push|pull]");
			exit (1);
		    }
		    break;

                case 'C':
		    do_checksums = !DO_CHECKSUMS;
		    break;
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
	    if ( i == (argc - 1) ) {
		/* Last arg. 
		*/
		if (dest) {
		    fprintf (stderr, "Error: Dest already set !?!\n");
		    exit (1);
		} else
	            dest = dtParseArg (argv[i], FALSE);

	    } else {
		/* Add to the source list.
		*/
	        sp = dtParseArg (argv[i], TRUE);
		if (!src) {
		    src = slast = sp;
		} else {
		    if (strcmp (sp->hostIP, src->hostIP)) {
			/* Ensure all sources are on the same host.  We may 
			** change this later, but it's simple restriction
			** for this version.
			*/
			fprintf (stderr, 
			    "ERROR: Source hosts must be unique, '%s' fails.\n",
			    sp->host);
			exit (1);
		    } else {
		        slast->next = sp;
		        slast = sp;
		    }
		}
		totsize += sp->fsize;
		nsrcs++;
	    }
	}
    }

    /* Total size is done in 1K blocks.  We use this number to check on the
    ** destination machine for enough transfer space, and use the entire
    ** transfer request to avoid sync problems if there isn't enough room.
    ** Note that individual files are also checked prior to each transfer.
    */
    totsize = totsize / 1024.;


    /* Initialize the DTS interface.  Use NULL args since we aren't setting
    ** up to be a DTS server process, but this allows us to load the config
    ** file.
    */
    dts = dtsInit (config, 0, "127.0.0.1", "./", 0);


    /*  Do the sanity checks on the destination machine.
     *
     *  1) Ping the machine to check for a live DTS.
     *  2) Check for enough space for the complete transfer.
     */
    dt_destValidate (dest, totsize);

        
    if (debug) {
	fprintf (stderr, "DEST: '%s' '%s' '%s'\n", 
	    dest->host, dest->hostIP, dest->path);
	for (i=0,sp=src; sp; sp=sp->next,i++)
	    fprintf(stderr, "SRC[%d]:  '%s' '%s' '%s'\n", i,
		sp->host, sp->hostIP, sp->path);
	fprintf (stderr, "mode = %s\n", (mode==XFER_PUSH ? "push" : "pull"));
	fprintf (stderr, "port = %d\n", port);
	fprintf (stderr, "nthreads = %d\n", nthreads);
    }




    /*  Loop over all of the source files and transfer.  Sources are
    **  specified individually with host info and the transfer command
    **  target depends on whether we use a PUSH or PULL model, so create
    **  the client url for each transfer.
    */
    for (sp=src; sp; sp=sp->next) {
    
	/* Get the client URL string.
	*/
	clientUrl = dt_getCmdUrl (mode, sp, dest);

	if (debug)
	    fprintf (stderr, "dtscp: clientURL = '%s'\n", clientUrl);


        /*  Sanity checking for the source side of transfer.
        */
        dt_srcValidate (sp, sp->fsize);



        /*  Initialize the client command and execute the transfer.
        */
        client = dt_initClientParams (clientUrl, sp, dest);
	if (noop == 0 && dt_executeTransfer (client, &sec, &usec) != OK) {
	    fprintf (stderr, "Transfer Failed: %s:%s\n", sp->host, sp->file);
	    continue;
	}

        /* Print the summary output.
        */
	if (debug) {
            printf ("%s:%s -> %s:%s\n",
		sp->host, sp->file, dest->host, dest->file);

	} else if (verbose) {
            printf ("%s:  (%ld bytes %d.%d sec)\n    src = '%s'  dest = '%s'",
	        sp->file, sp->fsize, sec, usec, sp->host, dest->host);
            printf ("    %g Mb/s   %g MB/s   %d.%d sec\n",
                transferMb (sp->fsize, sec, usec), 
		transferMB (sp->fsize, sec, usec), sec, usec);

	} else if (! quiet) {
            printf ("%s:  (%ld bytes in %d.%d sec -> %g MB/s)\n",
	        sp->file, sp->fsize, sec, usec, 
		transferMB(sp->fsize,sec,usec));
	}

	fileno++;
    }

    /* Clean up.
    */
    free ((void *) dest);
    for (sp=src; sp; sp=np) {
	np = sp->next;
        free ((void *) sp);
    }

    return 0;
}


/**
 *  DTPARSEARG --  Parse a DTSCP argument.
 * 
 *  @fn		xFile *dtParseArg (char *s, int is_src)
 *
 *  @param  xf	    file path name
 *  @param  is_src  is a source designation?
 *  @brief	    Get the host and path specification from designation.
 */
static xFile *
dtParseArg (char *s, int is_src)
{
    char   *ip = s, *sp, *hp;
    char   *localHost = (char *) dts_getLocalHost ();
    char   *localIP = (char *) dts_getLocalIP ();
    int     len = (int) max (strlen(localIP), strlen(s));
    xFile  *xf = (xFile *) NULL;


    /* Allocate an xFile.
    */
    xf = (xFile *) calloc (1, sizeof (xFile));


    if (*ip == '.' && strlen (s) == 1) {
	(void ) strcpy (xf->host,   localHost);	/* special case of the cwd */
	(void ) strcpy (xf->hostIP, localIP);
	(void ) getcwd (xf->path,   len);
	(void ) getcwd (xf->file,   len);	/* FIXME */

    } else {
	if ( (sp = strchr (s, (int)':')) ) {
	    *sp = '\0';
	    strcpy (xf->host, s);
	    strcpy (xf->path, sp+1);
	    strcpy (xf->file, sp+1);		/* FIXME */
	} else {
	    strcpy (xf->host,   localHost);	/* special case of the cwd */
	    strcpy (xf->hostIP, localIP);
	    strcpy (xf->path, s);
	    strcpy (xf->file, s);		/* FIXME */
	}
    }

    /* Now resolve the host name to an IP address.  For a local host, we've
    ** done this already, so it's only the DTS node names or FQDN machine
    ** names we need to resolve.
    */
    hp = xf->host;
    if (! isdigit ((int) *hp) ) {
	char   hostIP[SZ_PATH];

	memset (hostIP, 0, SZ_PATH);

	if (dts_nameToHost (dts, xf->host)) {	/* host in config file 	*/
	    strcpy (xf->host, dts_nameToHost (dts, xf->host));
	    strcpy (xf->hostIP,  dts_nameToIP (dts, xf->host));

	} else {

	    if ((ip = dts_resolveHost (xf->host)))
	        strcpy (hostIP, ip);
	    if (!hostIP[0] && verbose) {
		fprintf (stderr, 
		    "Warning: Cannot resolve '%s', using 'localhost'\n", hp);
	        strcpy (hostIP, (char *) "127.0.0.1");
	    }
	    strcpy (xf->hostIP, hostIP);	/* save the new host name */
	}
    } else
	strcpy (xf->hostIP, hp);


    /* Get the file/direcory size.
    */
    xf->isDir = dts_isDir (xf->path);
    if ( !is_src)
	xf->fsize = -1L;			/* dest doesn't get sized */
    else
	xf->fsize = dts_du (xf->path);


    return ( (xFile *) xf);
}


/**
 *  DT_GETCMDURL -- Generate a URL for the command target.
 */
static char *
dt_getCmdUrl (int mode, xFile *src, xFile *dest)
{
    static char url[SZ_URL];

    memset (url, 0, SZ_URL); 
    sprintf (url, "http://%s:%d/RPC2", 
        ((mode == XFER_PUSH) ? src->hostIP : dest->hostIP), DTS_PORT);

    return (url);
}


/**
 *  DT_INITCLIENTPARAMS -- Initialize the calling params.
 */
static int
dt_initClientParams (char *url, xFile *src, xFile *dest)
{
    int client;
    char  s_url[SZ_URL], d_url[SZ_URL];


    /* Initialize.
    */
    memset (s_url, 0, SZ_URL); 
    sprintf (s_url, "http://%s:%d/RPC2", src->hostIP, DTS_PORT);
    memset (d_url, 0, SZ_URL); 
    sprintf (d_url, "http://%s:%d/RPC2", dest->hostIP, DTS_PORT);

    /* Initialize the XML-RPC client.
    */
    client = xr_initClient (url, APP_NAME, APP_VERSION);
    xr_initParam (client);

    /* Set the calling params.
    */
    xr_setStringInParam (client, APP_NAME);	/* whoAmI (for logging) */
    xr_setStringInParam (client, XFER_METHOD);	/* transfer method	*/
    xr_setStringInParam (client, src->file);	/* file name		*/
    xr_setIntInParam    (client, src->fsize);	/* file size		*/
    xr_setIntInParam    (client, nthreads);	/* no. transfer threads	*/
    xr_setIntInParam    (client, port);		/* base transfer port	*/
    xr_setStringInParam (client, src->hostIP);	/* source machine	*/
    xr_setStringInParam (client, dest->hostIP);	/* dest machine		*/
    xr_setStringInParam (client, s_url);	/* src RPC server	*/
    xr_setStringInParam (client, d_url);	/* dest RPC server	*/

    return (client);
}


/**
 *  DT_EXECUTETRANSFER -- Execute the transfer command.
 */
static int
dt_executeTransfer (int client, int *sec, int *usec)
{
    int  res, anum;


    xr_callSync (client, method); 		/* execute the method 	*/

    xr_getArrayFromResult (client, &anum); 	/* get the result 	*/
        xr_getIntFromArray (anum, 0, sec);
        xr_getIntFromArray (anum, 1, usec);
        xr_getIntFromArray (anum, 2, &res);

    /*  FIXME -- This segfaults.
    */
    xr_clientCleanup (client);

    return (res);
}


/**
 *  DT_SRCVALIDATE -- Validate source side of transfer.
 */
static void
dt_srcValidate (xFile *src, long fsize)
{
    long   sz = 0L;


    /*  Do the sanity checks on the destination machine.
     *
     *  1) Ping the src machine to check for a live DTS.
     */
    if (dts_hostPing (src->hostIP) == ERR) {
	fprintf (stderr, "ERROR: Cannot contact DTS host machine '%s'\n", 
	    src->host);
	exit (1);
    }

    /*  2) Check for enough space for the complete transfer.
    */
    sz = dts_hostDiskFree (src->hostIP, src->path);
    if (fsize > (sz * 1024)) {
	fprintf (stderr, "ERROR: No room available for '%s'\n", src->file);
	exit (1);
    }
}



/**
 *  DT_DESTVALIDATE -- Validate destination side of transfer.
 */
static void
dt_destValidate (xFile *dest, long totsize)
{
    long   sz = 0L;


    /*  Do the sanity checks on the destination machine.
     *
     *  1) Ping the dest machine to check for a live DTS.
     */
    if (dts_hostPing (dest->hostIP) == ERR) {
	fprintf (stderr, "ERROR: Cannot contact DTS host machine '%s'\n", 
	    dest->host);
	exit (1);
    }

    /*  2) Check for enough space for the complete transfer.
    */
    if (totsize > (sz = dts_hostDiskFree (dest->hostIP, dest->path))) {
	fprintf (stderr, 
	    "ERROR: Not enough space available on '%s'\n", dest->host);
	fprintf (stderr, 
	    "ERROR:     %ld Kb required,  %ld Kb available\n", totsize, sz);
	exit (1);
    }
}
