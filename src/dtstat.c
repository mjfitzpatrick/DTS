/** 
**  DTSTAT -- Print a status summary of the DTS network.
**
**  Usage:
**	dtstat [options]
**
**  Options:
**	-h 		print help
**
** 	-c <cfg>	specify the config file to use
**	-p <port>	specify server port
**
**      -e [<N>]	number of errors to print
**      -q <name>	queue name
**      -s		print one-line summary only
**      -t		target machine
**
**      -v		verbose
**
**  @file	dtstat.c
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
#include "build.h"

#define  _DTS_SRC_

#define	DTSD_PAUSE	5		/* Pause between retries (sec)	*/



DTS   *dts 		= (DTS *) NULL;	/* main DTS structure       	*/

int    debug		= 0;		/* debug flag			*/
int    trace		= 0;		/* trace flag			*/
int    verbose		= 0;		/* verbose flag			*/
int    errs		= 0;		/* error msg flag		*/
int    init		= 0;		/* initialize server root    	*/
int    start		= 0;		/* attempt to start server	*/
int    colors		= 1;		/* use colored output?		*/
int    loop		= 0;		/* refresh periodically		*/
int    refresh		= 5;		/* refresh time			*/
int    summary		= 0;		/* print one-line summary	*/
int    web		= 0;		/* web output			*/

int    dts_monitor	= -1;		/* DTS monitor handle		*/
int    normal_exit	= 0;		/* did we exit normally?	*/


void    dtsFree (DTS *dts);

typedef struct {
    char      qname[SZ_FNAME];			/* queue name		*/
    int       threadNum;			/* thread number	*/
    dtsQueue  *dtsq;				/* dtsQueue struct	*/
} qmArg, *qmArgP;


static void dtss_Usage (void);
static void dtss_printStat (dtsClient *client, char *qname, int alive,
		nodeStat nstat);
static void dtss_printSummary (dtsClient *client, char *qname, int alive, 
		nodeStat ns);

static int dts_initStatMethods (DTS *dts);



/**
 *  DTSTAT Entry point.  
 */
int 
main (int argc, char **argv)
{
    register int i, j, len, port = DTS_PORT, stat=OK, alive=OK, noop=0;
    char     ch, *host = NULL, *root = NULL, *config = NULL;
    char    *qname = NULL,  *target = NULL, *network = NULL;
    char     qhost[SZ_PATH];

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
		case 'h':   dtss_Usage ();		exit (0);
                case 'B':   
		    use_builtin_config = 1;
		    config = def_dtsConfig;
		    break;

		case 'v':
                    if (strcmp (&(argv[i][j+1]), "ersion") == 0) {
                        printf ("Build Version: %s\n", build_date);
                        return (0);
                    } else
                        verbose++;
                    break;

		case 'c':   config = argv[++i];     	break;
		case 'n':   network = argv[++i];     	break;
		case 'p':   port = atoi (argv[++i]); 	break;

		case 'e':   
		    errs = 10;			/* set default 		*/
		    if (i < (argc-1) && argv[i+1][0] != '-')
			errs = atoi (argv[++i]);
		    break;
		case 'q':   qname = argv[++i];		break;
		case 'S':   start++; 			break;
		case 'C':   colors=0; 			break;
		case 'R':   refresh = atoi (argv[++i]);	break;
		case 'l':   loop++; 			break;
		case 's':   summary++; 			break;
		case 't':   target = argv[++i];		break;
		case 'w':   web++, colors=0;		break;

                default:
                    fprintf (stderr, "Unknown option '%c'\n", ch);
                    break;
                }
                /* j = len; */
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
     */
    dts_initStatMethods (dts);

    /*  Loop over each of the clients in the config file.
     */
    if (loop && refresh > 0)
	if (colors) printf("%c[2J", 0x1B);

    //if (web) printf ("<html><body><pre>\n");

repeat_:
    for (i=0; i < dts->nclients; i++) {
	client = dts->clients[i];

	/*  Move on if we're looking for a specific target host or queue
	 *  name. At this stage we check the queues on a particular client
	 *  to see if it exists, we print the status in a second stage below.
	 */
	if (target && strcasecmp (target, client->clientName))
	    continue;
	if (network && (strcasecmp(network, "default") && dts->nnetworks > 0)) {
	    if (strcasecmp (network, client->clientNet))
	        continue;
	}
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


	/*  Ping the host to check for aliveness.
	 */
	host = client->clientHost;
	memset (qhost, 0, SZ_PATH);
	sprintf (qhost, "%s:%d", host, client->clientPort);

	if (start)
	    alive = (noop ? OK : dts_hostPing (qhost));
	else
	    alive = (noop ? OK : dts_hostPoke (qhost));


	/*  Get node status information.
	 */
	memset (&nstat, 0, sizeof(nstat));
	if (alive == OK) {
	    stat = (noop ? OK : dts_hostNodeStat (qhost, qname, errs, &nstat));
	    if (stat != OK) {
                fprintf (stderr, "Status Request Fails\n");
	        continue;
	    }
	}

	/*  Print the node status information.
	 */
	if (summary)
	    dtss_printSummary (client, qname, alive, nstat);
	else
	    dtss_printStat (client, qname, alive, nstat);
    }
    printf ("\n");

    if (loop && refresh > 0) {
	sleep (refresh);
	/*if (colors) printf("%c[2J", 0x1B); */
	if (colors) printf("%c[0;0H", 0x1B);
	goto repeat_;
    }

    //if (web) printf ("</pre></body></html>\n");

    /* Clean up.
    */
    dtsFree (dts);
    return (0);
}



/******************************************************************************
**  DTS_INITSTATMETHODS -- Initialize the server-side of the DTS.  Create
**  an XML-RPC server instance, define the commands and start the thread.
*/
static int
dts_initStatMethods (DTS *dts)
{
    if (dts->trace)
        fprintf (stderr, "Initializing Stat Server Methods:\n");

    /*  Create the server and launch.  Note we never return from this
    **  and make no calls to other servers.
    xr_createServer ("/RPC2", dts->serverPort, NULL);
    */

    /*  Define the server methods.
    xr_addServerMethod ("init",            dts_initDTS,         NULL);
    */

    return (OK);
}


/******************************************************************************
 *  DTSS_PRINTSUMMARY -- Print the node status struct as a summary line.
 */

#define fG	30
#define BG	 0

void origin(void) 	{ if (colors) printf("%c[0;0H", 0x1B); 		   }
void erase(void) 	{ if (colors) printf("%c[2J", 0x1B); 		   }
void label(int v) 	{ if (colors) printf("%c[%d;33m", 0x1B, BG);       }

void bold(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?1:BG));    }
void underline(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?4:BG));    }
void blink(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?5:BG));    
			  if (web)    printf("<%cblink>", (v?' ':'/'));    
			}
void reverse(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?7:BG));    }
void concealed(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?8:BG));    }
  
void black(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?30:BG));   }
void red(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?31:BG));   }
void green(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?32:BG));   }
void yellow(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?33:BG));   }
void blue(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?34:BG));   }
void magenta(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?35:BG));   }
void cyan(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?36:BG));   }
void white(int v) 	{ if (colors) printf("%c[%dm", 0x1B, (v?37:BG));   }

void bblack(int v) 	{ if (colors) printf("%c[1;%dm", 0x1B, (v?20:BG)); }
void bred(int v) 	{ if (colors) printf("%c[1;%dm", 0x1B, (v?31:BG)); }
void bgreen(int v) 	{ if (colors) printf("%c[1;%dm", 0x1B, (v?32:BG)); }
void byellow(int v) 	{ if (colors) printf("%c[1;%dm", 0x1B, (v?33:BG)); }
void bblue(int v) 	{ if (colors) printf("%c[1;%dm", 0x1B, (v?34:BG)); }
void bmagenta(int v) 	{ if (colors) printf("%c[1;%dm", 0x1B, (v?35:BG)); }
void bcyan(int v) 	{ if (colors) printf("%c[1;%dm", 0x1B, (v?36:BG)); }
void bwhite(int v) 	{ if (colors) printf("%c[1;%dm", 0x1B, (v?37:BG)); }

void rblack(int v) 	{ if (colors) printf("%c[7;%dm", 0x1B, (v?30:BG)); }
void rred(int v) 	{ if (colors) printf("%c[7;%dm", 0x1B, (v?31:BG)); }
void rgreen(int v) 	{ if (colors) printf("%c[7;%dm", 0x1B, (v?32:BG)); }
void ryellow(int v) 	{ if (colors) printf("%c[7;%dm", 0x1B, (v?33:BG)); }
void rblue(int v) 	{ if (colors) printf("%c[7;%dm", 0x1B, (v?34:BG)); }
void rmagenta(int v) 	{ if (colors) printf("%c[7;%dm", 0x1B, (v?35:BG)); }
void rcyan(int v) 	{ if (colors) printf("%c[7;%dm", 0x1B, (v?36:BG)); }
void rwhite(int v) 	{ if (colors) printf("%c[7;%dm", 0x1B, (v?37:BG)); }

void ublack(int v) 	{ if (colors) printf("%c[4;%dm", 0x1B, (v?20:BG)); }
void ured(int v) 	{ if (colors) printf("%c[4;%dm", 0x1B, (v?31:BG)); }
void ugreen(int v) 	{ if (colors) printf("%c[4;%dm", 0x1B, (v?32:BG)); }
void uyellow(int v) 	{ if (colors) printf("%c[4;%dm", 0x1B, (v?33:BG)); }
void ublue(int v) 	{ if (colors) printf("%c[4;%dm", 0x1B, (v?34:BG)); }
void umagenta(int v) 	{ if (colors) printf("%c[4;%dm", 0x1B, (v?35:BG)); }
void ucyan(int v) 	{ if (colors) printf("%c[4;%dm", 0x1B, (v?36:BG)); }
void uwhite(int v) 	{ if (colors) printf("%c[4;%dm", 0x1B, (v?37:BG)); }


static void
dtss_printSummary (dtsClient *client, char *qname, int alive, nodeStat ns)
{
    register int i=0, qact=0;
	    
    bold (0);
    printf ("%-12.12s ", client->clientName);
    bold (0);

    if (alive != OK) {
        bcyan (1);
	printf ("Free: \t\tQueues:\t\t\tErrs: \t");
	bred (1);
	if (web) printf ("<font color='#FF0000'>");
	printf ("    Offline\n");
	if (web) printf ("</font>");
	bred (0);
        bcyan (0);
	return;
    }

    for (i=0; i < ns.nqueues; i++) {
	qStat  *q = &ns.queue[i];
	qact  += (q->pending ? 1 : 0);
    }

    if (colors) {
        bcyan (1); 
	printf ("Free:  "); 
	bcyan (0);
        printf ("%7.2f\t", ((float) ns.dfree / 1024.));

	bcyan (1);
        printf ("Queues: ");
	bcyan (0);
        printf ("%2d", qact);
	bcyan (1); printf ("  of "); bcyan (0);
        printf ("%2d\t", ns.nqueues);

	bcyan (1);
        printf ("Errs: \t");
	bcyan (0);

	bcyan (ns.nerrors);
        printf ("%d", ns.nerrors);
	bcyan (0);

	bgreen (1);
        printf ("   %s\n", (start ? "Started" : "Alive"));
	bgreen (0);

    } else {
        printf ("Free: %5.2f\t", ((float) ns.dfree / 1024.));
        printf ("Queues: %2d of %2d\tErrs: %d\t", qact, ns.nqueues, ns.nerrors);
        printf ("    %s\n", (start ? "Started" : "Alive"));
    }
}


/******************************************************************************
 *  DTSS_PRINTSTAT -- Print the node status struct.
 */
static void
dtss_printStat (dtsClient *client, char *qname, int alive, nodeStat ns)
{
    register int i=0;
    char  *srcHost, *ip, *s;
    extern char *dts_cfgQNodeStr ();
    int	   min = 0;
    float  sec = 0.0;


    ryellow(1);
    rwhite(1);
    if (web) {
        printf ("<hr>");
    } else {
        printf ("                                        ");
        printf ("                                        \n");
        underline (0);
    }

    byellow (1);
    printf ("Node: ");
    byellow (0);
    bold (1);
    printf ("%-10.10s", client->clientName);
    //printf ("\t    %-s\t\t    ", dts_localTime());
    printf ("\t    %-s UTC\t        ", dts_UTTime());
    bold (0);
    byellow (1);
    printf ("Status: ");
    byellow (0);
    if ( (alive == OK || start) ) {
	bgreen (1);
        if (web) printf ("<font color='#00FF00'>");
    } else {
	bred (1);
        if (web) printf ("<font color='#FF0000'>");
    }
    if (alive != OK) blink (1);
    s = (alive == OK ?  (start ? "Started" : "Alive") : "Offline");
    if (alive != OK) blink (1);
    printf ("%s\n", s);
    if (alive != OK) blink (0);
    if (web) printf ("</font>");
    bwhite (0);

    byellow (1);
    printf ("  Host:");
    byellow (0);
    printf ("\t\t%s:%d:%s\n", client->clientHost, client->clientPort,
	(ns.root[0] ? ns.root : client->clientDir)); 

    byellow (1);
    printf ("  Port Range:");
    byellow (0);
    printf ("\t%d - %d\t", 
	client->clientLoPort, client->clientHiPort);

    if (alive != OK) { 		/* keep it brief if we're offline	*/
	printf ("\n\n");
	return ;
    }


    /*  Print queue summaries.
     */
    byellow (1);
    printf ("  Disk Used:");
    byellow (0);
    printf ("   %.2f GB  ", ((float) ns.dused / 1024.));
    byellow (1);
    printf ("Free:  ");
    byellow (0);
    if (((float) ns.dfree / 1024.) < 500) {
        if (((float) ns.dfree / 1024.) < 100)
            blink (1);
        bred (1);
    } else
        bold (1);
    printf ("%.2f", ((float) ns.dfree / 1024.));
    bold (0);
    printf (" GB\n\n");

    printf ("  ");
    cyan (1);
    underline (1);
    printf ("%-9.9s  %-3s %-8s %-6s %-4s  %-5s %-8s %-7s  %-8s %-4s\n",
      	"QName", "Typ", "Src", "Proc", "Pend", "Mbps", "Time", "Tot(GB)", 
	"Stat", "Nerr");
    bold (0);


    if (web) printf ("<font color='#FFFFFF'>");
    for (i=0; i < ns.nqueues; i++) {
      qStat  *q = &ns.queue[i];

      min = ((int) q->time) / 60;
      sec = (q->time - (min*60)) / 60.;

      if ((ip = strchr (q->src, (int)'.')))
	*ip = '\0';
      srcHost = q->src;

      bold (1);
      printf ( "  %-9.9s", q->name);
      bold (0);

/*
      printf (
	"   %c  %-8s %-6d %-4d  %-5.1f %02d:%04.1f  %7.1f  %-7s %4d\n",
	  toupper (q->type[0]), 
	  (q->type[0] == 'i' ? "ingest" : srcHost ),
	  q->current, q->pending, q->rate, min, (sec * 60),
	  q->total, 
	  (q->type[0] == 'e' ? "  N/A" : 
	      (alive == OK ? q->status : "offline")), ns.nerrors );
*/
      printf ( "   %c  %-7.7s %-6d ", toupper (q->type[0]), 
	  (q->type[0] == 'i' ? "ingest" : srcHost ), q->current);
      if (q->pending) rgreen (1);
      printf ("%-4d", q->pending);
      if (q->pending) rgreen (0);
      printf ("  %-5.1f %02d:%04.1f  %7.1f  %-7s %4d\n",
	  q->rate, min, (sec * 60), q->total, 
	  (q->type[0] == 'e' ? "  N/A" : 
	      (alive == OK ? q->status : "offline")), ns.nerrors );
				/*  FIXME -- above should be errors per queue */

if (!web) {
      printf ("      ");
      label (1);
      printf ("File:");
      bold (0);

      printf ("   ");
      bgreen (1);
      printf ("<< %-27.27s ",
	(q->infile[0] && strcmp("none",q->infile) ? q->infile : "in"));
      printf ("%27.27s >> ", 
	(q->outfile[0] && strcmp("none",q->outfile) ? q->outfile : "out"));
      bgreen (0);

      printf ("   \n");
}
    }
    if (web) printf ("</font>");

    /*  Print error summaries.
    printf ("\n  Errors:  %d\n", ns.nerrors);
    for (i=0; i < ns.nerrors; i++)
	printf ("    [%2d] %s\n", i, ns.emsgs[i]);
     */
    printf ("\n");

ryellow(1);
rwhite(1);
printf ("                                        ");
printf ("                                        \r");
bold (0);
}



/******************************************************************************
 *  USAGE -- Print DTSTAT usage information
 */
static void
dtss_Usage (void)
{
  fprintf (stderr, "\
    Usage:\n\
  	dtstat [options]\n\
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
