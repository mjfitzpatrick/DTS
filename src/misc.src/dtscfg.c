/**
**  DTSCFG -- Utility app to scan a DTS or DTSQ config file to output infor-
**  mation in a more readable form, or as code to include in binaries that
**  may not have access to the config file.
**
**  Usage:
**      dtscfg [options] [<config_file> ....]
**
**  Options:
**      -h			      print help
**
**      -a [alias]		      get config file from named alias
**      -i			      write as an include file
**      -l			      list clients
**      -n			      print clients belonging to network
**      -q			      print clients having named queue
**      -o [file]		      set output filename
**      -t [target]		      get config file from named target
**
**      -d			      debug flag
**      -v			      verbose output flag
**
**  @file       dtscfg.c
**  @author     Mike Fitzpatrick, NOAO
**  @date	May 2015
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
#include "dtsClient.h"
#include "build.h"


#define  APP_NAME	"dtscfg"
#define  APP_VERSION	"1.0"

#define	 SZ_SBUF	4096
#define	 MAX_ARGS	32


char *host 	= "localhost";			/* target host              */
char *lhost 	= "";				/* logical host             */
char *config 	= NULL;				/* config file name         */
char *outfile 	= NULL;				/* output file name         */
char *target 	= NULL;				/* target machine name      */
char *alias 	= NULL;				/* target alias name        */

/* Options
*/
int debug 	= 0;				/* debug flag               */
int verbose 	= 0;				/* verbose flag             */

int doInc 	= 0;				/* print as include file    */
int doList 	= 0;				/* list results             */
int doNetwork 	= 0;				/* print network names      */
int doQueue 	= 0;				/* print queue names        */

int res 	= OK;
int cmdPort	= 0;

FILE *in 	= (FILE *) NULL;
FILE *out 	= (FILE *) NULL;

DTS  *dts 	= (DTS *) NULL;

char  cmdbuf[SZ_SBUF];
char  localIP[SZ_PATH];

char *cfgFiles[MAX_ARGS];
int   numCfgFiles = 0;

static void  dts_cfgUsage (void);


/**
 *  Task entry point.
 */
int
main (int argc, char *argv[])
{
    register int i, j, len;
    char msg[SZ_LINE], cmdline[SZ_LINE];


    memset(msg, 0, SZ_LINE);
    memset(localIP, 0, SZ_PATH);
    memset(cmdbuf, 0, SZ_SBUF);
    memset(cmdline, 0, SZ_LINE);


    /* Process command-line arguments.
     */
    for (i = 1; i < argc; i++) {
        if ( (argv[i][0] == '-' && !(isdigit(argv[i][1]))) ) {
	    len = strlen(argv[i]);
	    for (j = 1; j < len; j++) {
		switch (argv[i][j]) {
		case 'h':
		    dts_cfgUsage();
		    return (0);
                case 'B':   use_builtin_config = 1; 	  break;

						/* Task-specific flags	*/
		case 'a':  alias = strdup (argv[++i]); 	  break;
		case 'd':  debug++; 			  break;
		case 'i':  doInc++; 			  break;
		case 'l':  doList++; 			  break;
		case 'n':  doNetwork++;			  break;
		case 'q':  doQueue++; 			  break;
		case 't':  target = strdup (argv[++i]);   break;

		case 'o':
		    outfile = strdup (argv[i]);
		    if ((out = fopen (argv[i], "w+")) == (FILE *) NULL) {
			fprintf (stderr, 
				"Error opening output file '%s'\n", argv[i]);
			return (ERR);
		    }
		    break;
		 				/* Standard task flags	*/
		case 'v':
		    if (strcmp (&(argv[i][j + 1]), "ersion") == 0) {
			printf ("Build Version: %s\n", build_date);
			return (0);
		    } else
			verbose++;
		    break;

		default:
		    fprintf (stderr, "Unknown option '%c'\n", argv[i][j]);
		    return (ERR);
		}
	    }

	} else {
	    /*  The only non-flag arguments are config file names.
	     */
	    cfgFiles[numCfgFiles++] = strdup (argv[i]);
	}
    }

    if (argc == 1)
	doInc++;  	/* use the $HOME/.dts_config as the input source */

    if (debug) {
        printf ("doInc = %d\ndoList = %d\n", doInc, doList);
        printf ("donEtwork = %d\ndoQueue = %d\n", doNetwork, doQueue);
        printf ("target = '%s'\nalias = '%s'\n", target, alias);
        for (i=0; i < numCfgFiles; i++)
	    printf ("%2d: file = '%s'\n", i, cfgFiles[i]);
    }


    /*  Initialize the DTS interface.  Use dummy args since we aren't setting
     *  up to be a DTS server process, but this allows us to load the config
     *  file and initialize the 'dts' struct with local information.
     */
    strcpy (localIP, dts_getLocalIP ());
    dts = dtsInit (config, cmdPort, localIP, "./", 0);





    /*  If we were given an alias name, lookup the host.  For this to work
     *  we need a local config file to bootstrap.
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
    if (target)
        host = target;


    /*  First test:  Can we contact the DTS server at all?
     */
    if (target && dts_hostPing (host) != OK) {
	fprintf (stderr, "Error: Cannot contact DTS host '%s'\n", host);
	return (ERR);
    }




    /* Clean up.  On success, return zero to $status.
     */
    if (alias)
	free (alias);
    if (target)
	free (target);
    if (outfile && out != stdout) {
	fclose (out);
	free (outfile);
    }
    for (i=0; i < numCfgFiles; i++)
	free (cfgFiles[i]);

    dtsFree (dts);
    return (res);
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
static void dts_cfgUsage(void)
{
    fprintf (stderr, "\
Usage:\n\
    dtsh [options] <cmd> <arg> [<arg> ...]\n\
\n\
Options:\n\
    -a [alias]		      get config file from named alias\n\
    -i			      write as an include file\n\
    -l			      list clients\n\
    -n			      print clients belonging to network\n\
    -q			      print clients having named queue\n\
    -o [file]		      set output filename\n\
    -t [target]		      get config file from named target\n\
\n\
    -d			      debug flag\n\
    -v			      verbose output flag\n\
\n\
");
}
