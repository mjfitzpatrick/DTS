#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>

#include <sys/errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "dts.h"
#include "dtsPSock.h"

#define	 min(a,b)	"(a < b ? a : b)"
#define	 max(a,b)	"(a > b ? a : b)"


int	do_checksums	= 0;
char   	*fname 		= NULL;
char	*src_host 	= "140.252.1.86";	/* tucana		*/
char	*dest_host 	= "140.252.26.30";	/* denali		*/
    

    
/*  Usage:
**
** 	 xfer [-s <src>] [-d <dest>] [-push|-pull] [-n <N>] [-v] <fname>
*/


int 
main(int argc, char **argv)
{
    register int i, j, len;
    char     ch, s_url[128], d_url[128];
    long     fsize    = 0;
    int	     sec      = 0, 
	     usec     = 0,
	     res      = 0,
	     anum     = 0;
    int      mode     = XFER_PUSH, 
	     nthreads = DEF_NTHREADS, 
	     port     = DEF_XFER_PORT,
	     client   = 0,
	     debug    = 0,
	     verbose  = 0;


    /* Process commandline arguments.
    */
    for (i=1; i < argc; i++) {
        if (argv[i][0] == '-' && !(isdigit(argv[i][1]))) {
	    len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                ch = argv[i][j];

                switch (ch) {
                case 'd':
		    dest_host = argv[++i];
		    break;
                case 'f':
		    fname = argv[++i];
		    if (access (fname, F_OK) == 0)
		        fsize = dts_nameSize (fname);
		    break;
                case 'n':
		    nthreads = atoi(argv[++i]);
		    break;
                case 's':
		    src_host = argv[++i];
		    break;
                case 'p':  
		    if (strcasecmp (argv[i], "-push") == 0) {
			mode = XFER_PUSH;
		    } else if (strcasecmp (argv[i], "-pull") == 0) {
			mode = XFER_PULL;
		    } else {
			perror ("Invalid mode flag [push|pull]");
			exit (1);
		    }
		    break;
                case 'v':
		    verbose = 1;
		    break;

                case 'F':
		    do_checksums = !DO_CHECKSUMS;
		    break;
		default:
		    fprintf (stderr, "Unknown option '%c'\n", ch);
		    break;
		}
		j = len;		/* skip remainder of flag 	*/
	    }

	} else {
	    fname = argv[i];
	    fsize = dts_nameSize (fname);
	}
    }


    /* Create a client URL string.
    */
    memset (s_url, 0, 128); 
    sprintf (s_url, "http://%s:%d/RPC2", src_host, DTS_PORT);
    memset (d_url, 0, 128); 
    sprintf (d_url, "http://%s:%d/RPC2", dest_host, DTS_PORT);

    if (debug) {
	fprintf (stderr, " s_url = %s\n", s_url);
	fprintf (stderr, " d_url = %s\n", d_url);
	fprintf (stderr, "  mode = %s\n", (mode==XFER_PUSH ? "push" : "pull") );
        fprintf (stderr, "   url = %s\n", (mode==XFER_PUSH ? s_url : d_url) );
    }


    /*  If mode is PUSH, we send a command to the DTSD on the source machine,
    **  otherwise the command is sent to the dest machine. 
    */
    client = xr_initClient (((mode==XFER_PUSH) ? s_url : d_url), "xfer","v1.0");

    xr_initParam (client);

    /* Set the calling params.
    */
    xr_setStringInParam (client, "xferTest");
    xr_setStringInParam (client, "psock");
    xr_setStringInParam (client, fname);
    xr_setIntInParam    (client, fsize);
    xr_setIntInParam    (client, nthreads);
    xr_setIntInParam    (client, port);
    xr_setStringInParam (client, src_host);
    xr_setStringInParam (client, dest_host);
    xr_setStringInParam (client, s_url);
    xr_setStringInParam (client, d_url);

    xr_callSync (client, (mode == XFER_PUSH ? "xferPush" : "xferPull") );

    xr_getArrayFromResult (client, &anum);
        xr_getIntFromArray (anum, 0, &sec);
        xr_getIntFromArray (anum, 1, &usec);
        xr_getIntFromArray (anum, 2, &res);

    /*  FIXME -- This segfaults.
    */
    xr_clientCleanup (client);



    /* Print the summary output.
    */
    printf ("File: '%s' (%ld bytes %d.%d sec)\n    src = '%s'  dest = '%s'",
	fname, fsize, sec, usec, src_host, dest_host);
    printf ("  mode = '%s'\n", (mode==XFER_PUSH ? "push" : "pull") );
    printf ("    %g Mb/s   %g MB/s   %d.%d sec\n",
        transferMb(fsize,sec,usec), transferMB(fsize,sec,usec), sec, usec);
    printf ("    status = %s\n", (res ? "Error" : "OK") );


    return 0;
}
