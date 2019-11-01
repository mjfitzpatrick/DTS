/**
**  DTSLOG -- Command-line tool to log a message to the DTS.
**
**  @file       dtslog.c
**  @author     Mike Fitzpatrick, NOAO
**  @date	July 2010
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>

#include "dts.h"
#include "dtsdb.h"
#include "dtsMethods.h"
#include "dtsClient.h"
#include "build.h"

#define  _DTS_SRC_

#define	SZ_LOGMSG		1024

#define	MAX_TESTS		4096
#define	MAX_THREADS		 512


DTS   *dts 		= (DTS *) NULL;	/* main DTS structure       	*/

int    interactive	= 0;		/* interactive mode		*/
int    debug		= 0;		/* debug flag			*/
int    trace		= 0;		/* trace flag			*/
int    verbose		= 0;		/* verbose flag			*/
int    test		= 0;		/* test mode			*/
int    Nthreads		= 0;		/* No. parallel threads		*/

char   monitor[SZ_FNAME];		/* monitor connection		*/
char   msg[SZ_LOGMSG];			/* message string		*/

int    dts_monitor	= -1;		/* DTS monitor handle		*/


void    dtsFree (DTS *dts);

typedef struct {
    char      qname[SZ_FNAME];			/* queue name		*/
    int       threadNum;			/* thread number	*/
    dtsQueue  *dtsq;				/* dtsQueue struct	*/
} qmArg, *qmArgP;


static void dtslog_Usage (void);
void log_test (void *data);


/**
 *  DTS Logger Entry point.  
 */
int 
main (int argc, char **argv)
{
    register int i, j, rc, len, port = DTS_PORT;
    char     ch, *host = NULL, *root = NULL, *config = NULL;
    char     line[SZ_LOGMSG];
    void     *stat;

    /*
    dtsClient *client = (dtsClient *) NULL;
    */


    /* Initialize.
    */
    memset (msg,     0, SZ_LOGMSG);
    memset (line,    0, SZ_LOGMSG);
    memset (monitor, 0, SZ_FNAME);
    host = dts_getLocalIP ();

    /* Process commandline arguments.
    */
    for (i=1; i < argc; i++) {
        if (argv[i][0] == '-' && !(isdigit(argv[i][1]))) {
            len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                ch = argv[i][j];

                switch (ch) {
		case 'h':   dtslog_Usage ();			exit (0);
                case 'B':   use_builtin_config = 1; 		break;

		case 'v':
                    if (strcmp (&(argv[i][j+1]), "ersion") == 0) {
                        printf ("Build Version: %s\n", build_date);
                        return (0);
                    } else
                        verbose++;
                    break;

		case 'i':   interactive++;     			break;
		case 't':   test++;     			break;
		case 'N':   Nthreads = atoi (argv[++i]);     	break;

		case 'c':   config = argv[++i];     		break;
		case 'm':   strcpy (monitor, argv[++i]);	break;

                default:
                    fprintf (stderr, "Unknown option '%c'\n", ch);
                    break;
                }
            }

        } else if (argv[i][0] == '+') {
            len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                switch ((ch = argv[i][j])) {
		case 'd':   debug++; 				break;
		}
                j = len;        	/* skip remainder       */
	    }
	} else {
	    strcat (msg, argv[i]);
	    strcat (msg, " ");
	}
    }

    if (verbose)
	fprintf (stderr, "test: %d  Nthreads: %d\n", test, Nthreads);


    /*  Initialize.  Passing in the port/host/svc let's us override the
    **  local setting or else pretend to be another server.
    */
    dts = dtsInit (config, port, "localhost", root, 0);
    dts->debug    = debug;
    dts->verbose  = verbose;

    /*  Connect to the logging monitor or else open a local logfile.
    */
    strcpy (dts->monitor, monitor);
    if (dts->monitor[0]) {
        if (index (dts->monitor, (int)':'))
            sprintf (dts->mon_url, "http://%s/RPC2", dts->monitor);
        else
            sprintf (dts->mon_url, "http://%s:%d/RPC2",
                dts->monitor, DTSMON_PORT);
    } else
        sprintf (dts->mon_url, "http://%s:%d/RPC2", DTSMON_HOST, DTSMON_PORT);

    xr_setClient (DTSMON_CLIENT, dts->mon_url);
    dts->mon_fd = 1;


    if (test) {
	if (Nthreads > 0) {
	    pthread_t	tid[MAX_THREADS];
	    pthread_attr_t  attr;

    	    pthread_attr_init (&attr);

	    for (i=0; i < Nthreads; i++) {
	        if ((rc = pthread_create (&tid[i], &attr, (void *) log_test,
		    (void *) NULL))) {
	                fprintf (stderr, "ERROR: cannot create thread %d\n", i);
	                return (1);
	        }
	    }

            for (i=0; i < Nthreads; i++) {
                if ((rc = pthread_join (tid[i], (void **)&stat)))
                    ;
            }

	} else
	    log_test (NULL); 

    } else if (interactive) {
	/*  Interactively log whatever the user types.
	 */
        while (fgets (msg, SZ_LOGMSG, stdin))
            dtsLog (dts, msg);

    } else {
	/* Log a single message and quit.
	 */
        dtsLog (dts, msg);
    }

    /* Clean up.
    */
    dtsFree (dts);
    return (0);
}


void log_test (void *data)
{
    int	   i;
    char   msg[SZ_LOGMSG];

    for (i=0; i < MAX_TESTS; i++) {
	memset (msg, 0, SZ_LOGMSG);
	sprintf (msg, "Test tid=%d  count=%d", (int) pthread_self(), i);

	dtsLog (dts, msg);
    }
}


/******************************************************************************
 *  USAGE -- Print DTSTAT usage information
 */
static void
dtslog_Usage (void)
{
  fprintf (stderr, "\
    Usage:\n\
  	dtslog [options] [message]\n\
  \n\
    Options:\n\
  	-h 		print help\n\
        -v		verbose\n\
  \n\
   	-c <cfg>	specify the config file to read\n\
        -m [host:port]	specify monitor\n\
  \n\
  \n");
}
