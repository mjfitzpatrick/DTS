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



typedef struct {
    char *fname;
    long fsize;
} tdata, *tdataP;



int	do_checksums	= 0;
int	whoAmI		= XFER_SOURCE;
char   	*fname 		= "test.dat";
char	*host  		= "localhost";
    

    

/* Usage:
** run as data source: xfer -s (without arguments)
** run as data sink:   xfer -d
*/


int main(int argc, char **argv)
{
    register int i, j, len;
    char   ch, 
	   *fname   = NULL;
    int    type     = XFER_PUSH;
    long   fsize    = 0;
    int    mode     = XFER_PUSH, 
	   nthreads = DEF_NTHREADS, 
	   port     = DEF_XFER_PORT,
	   verbose  = 0;

    void (*func)(void *data) = NULL;		/* function to execute 	*/



    /* Process commandline arguments.
    */
    for (i=1; i < argc; i++) {
        if (argv[i][0] == '-' && !(isdigit(argv[i][1]))) {
	    len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                ch = argv[i][j];

                switch (ch) {
                case 'f':
		    fname = argv[++i];
		    if (access (fname, F_OK) == 0)
		        fsize = dts_nameSize (fname);
		    break;
                case 'h':  host = argv[++i]; 		  break;
                case 'n':  nthreads = atoi (argv[++i]);   break;
                case 'p':  port = atoi (argv[++i]); 	  break;
                case 'd':  whoAmI = XFER_DEST; 		  break;
                case 's':  whoAmI = XFER_SOURCE;	  break;

                case 'm':  
                case 't':  
		    type = mode = ((strcmp(argv[++i],"pull") == 0) ? 
			XFER_PULL : XFER_PUSH);
		    break;
                case 'F':  do_checksums = !DO_CHECKSUMS;  break;
                case 'S':  fsize = atol(argv[++i]);	  break;
                case 'v':  verbose = 1; 		  break;
		default:
		    fprintf (stderr, "Unknown option '%c'\n", ch);
		    break;
		}
		j = len;	/* skip remainder	*/
	    }

	} else {
	    fname = argv[i];
	    fsize = dts_nameSize (fname);
	}
    }

    if (nthreads > MAX_THREADS)
	nthreads = MAX_THREADS;


    /* Without an argument, we operate as the 'server' and send data
    ** to the listening client.
    */
    if (whoAmI == XFER_SOURCE) {
	func = psSendFile;

	fprintf (stderr, "Waiting for %d connections ....\n", nthreads);

    } else if (whoAmI == XFER_DEST) {
	func = psReceiveFile;

	fprintf (stderr, "Making %d connections ....\n", nthreads);

	/*
	*/
	if (access (fname, F_OK) != 0) {
	    fprintf (stderr, "Pre-allocating file...\n");
            dts_preAlloc (fname, fsize);
	}
    }


    /*  Spawn the function which does all the work.
    */
    psSpawnWorker (func, nthreads, fname, fsize, mode, port, host, verbose);

    return 0;
}
