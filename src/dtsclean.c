/** 
**  DTCLEAN -- Clean the source files or DTS working directory.
**
**  Usage:
**	dtclean [options]
**
**  Options:
**	-h 		print help
**
** 	-c <cfg>	specify the config file to use
**	-p <port>	specify server port
**
**      -t		trace
**      -v		verbose
**
**  @file	dtclean.c
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
#include "dtsdb.h"
#include "dtsMethods.h"
#include "dtsClient.h"

#define  _DTS_SRC_

#define	DTSD_PAUSE	5		/* Pause between retries (sec)	*/



DTS   *dts 		= (DTS *) NULL;	/* main DTS structure       	*/

int    debug		= 0;		/* debug flag			*/
int    trace		= 0;		/* trace flag			*/
int    verbose		= 0;		/* verbose flag			*/
int    errs		= 0;		/* error msg flag		*/
int    init		= 0;		/* initialize server root    	*/

int    dts_monitor	= -1;		/* DTS monitor handle		*/
int    normal_exit	= 0;		/* did we exit normally?	*/


void    dtsFree (DTS *dts);

static  void dtcl_Usage (void);

typedef struct {
    char      qname[SZ_FNAME];			/* queue name		*/
    int       threadNum;			/* thread number	*/
    dtsQueue  *dtsq;				/* dtsQueue struct	*/
} qmArg, *qmArgP;


static void dtcl_Usage (void);


/**
 *  DTS Daemon Entry point.  
 */
int 
main (int argc, char **argv)
{
    register int i, j, len, port = DTS_PORT, res=OK, noop=0;
    char     ch, *host = NULL, *root = NULL, *config = NULL;
    char    *qname = NULL,  *target = NULL;

    dtsClient *client = (dtsClient *) NULL;
    nodeStat   nstat;


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
		case 'h':   dtcl_Usage ();		exit (0);
		case 'v':   verbose++; 			break;
                case 'B':   use_builtin_config = 1; 	break;

		case 'c':   config = argv[++i];     	break;
		case 'p':   port = atoi (argv[++i]); 	break;

                default:
                    fprintf (stderr, "Unknown option '%c'\n", ch);
                    break;
                }
                j = len;        		/* skip remainder       */
            }

        } else if (argv[i][0] == '+') {
            len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                switch ((ch = argv[i][j])) {
		case 'd':   debug++; 			break;
		}
                j = len;        /* skip remainder       */
	    }
	}
    }


    /*  Initialize.  Passing in the port/host/svc let's us override the
    **  local setting or else pretend to be another server.
    */
    dts = dtsInit (config, port, "localhost", root, init);

    dts->debug    = debug;
    dts->verbose  = verbose;


    /*  Initialize the DTS server methods.
    dts_initCleanMethods (dts);
     */


    /*
     */
    for (i=0; i < dts->nclients; i++) {
	client = dts->clients[i];

	/*  Move on if we're looking for a specific target host or queue
	 *  name. At this stage we check the queues on a particular client
	 *  to see if it exists, we print the status in a second stage below.
	 */
	if (target && strcasecmp (target, client->clientName))
	    continue;
        if (qname) {
	    int found = 0;
            for (j=0; j < dts->nqueues; j++) {
		if (strcasecmp (qname, dts->queues[j]->name) == 0) {
		    found++;
		    break;
		}
            }
	    if (! found)
		continue;
	}

	printf ("\nNode: %-10.10s\t\t\t ", client->clientName);

	/*  Ping the host to check for aliveness.
	 */
	res = (noop ? OK : dts_hostPing ((host = client->clientHost)));
        printf ("\t\t\tStatus: %s\n", (res == OK ? "Alive" : "Offline"));
	if (res != OK)
	    continue;

	res = (noop ? OK : dts_hostNodeStat (host, qname, errs, &nstat));
	if (res != OK) {
            fprintf (stderr, "Status Request Fails\n");
	    continue;
	}

	/*  Print the node status information.
	dtcl_printStat (client, qname, nstat);
	 */
    }
    printf ("\n");


    /* Clean up.
    */
    dtsFree (dts);
    return (0);
}


/**
 *  USAGE -- Print DTCLEAN usage information.
 */
static void
dtcl_Usage (void)
{
  fprintf (stderr, "\
    Usage:\n\
  	dtclean [options]\n\
  \n\
    Options:\n\
  	-h 		print help\n\
        -v		verbose\n\
  \n\
   	-c <cfg>	specify the config file to read\n\
  	-p <port>	specify the local server port  [NOT USED]\n\
  \n\
        -e [<N>]	print last <N> error messages\n\
        -q		specify queue\n\
        -t		specify target machine\n\
  \n\
  \n");
}
