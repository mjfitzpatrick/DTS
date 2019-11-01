/*
**  DTSPSOCK.H -- Structures and constants for parallel socket transfers.
**
*/

#define SERVADDR 	"127.0.0.1" 	/* default server address 	*/

#define DEF_XFER_PORT 	3001        	/* default transport socket	*/
#define DEF_MAX_PORTS 	64        	/* max open transport sockets	*/
#define DEF_NTHREADS	10		/* default threads to create	*/
#define MAX_THREADS	512		/* max transfer threads to run	*/
#define MAXTRIES	128		/* max times to resend		*/

#define DO_CHECKSUMS	0		/* debug flags			*/

#define PS_SOURCE	0		/* data source			*/
#define	PS_DEST		1		/* data sink			*/

#define	PS_PUSH		XFER_PUSH	/* push transfer model		*/
#define	PS_PULL		XFER_PULL	/* pull transfer model		*/


#define SZ_XFER_BUFFER	(1024 * 1025 * 4) /* transfer buffer size	*/

#define TCP_WINDOW_MAX	196608 		/* TCP window max size		*/
#define TCP_WINDOW_SZ	262144
#define SZ_XFER_CHUNK	(1024 *  1024 * 2)




#ifdef USE_PHDR
/*  Packet header.  A data "stripe" is preceeded by a header packet 
**  containing information about the stripe.  This is also used when
**  doing checksums after transferring each stripe.
*/
typedef struct {
    unsigned short  sum16;		/* 16-bit checksum		*/
    unsigned int    sum32;		/* 32-bit checksum		*/
    int    	    chunkSize;		/* transfer 'chunk' size	*/
    long   	    offset;		/* file offset of stripe	*/
    long   	    maxbytes;		/* max bytes to transfer	*/
} phdr, *phdrP;

	
/*  Data structure used to pass information into parallel socket worker
**  thread.
*/
typedef struct {
    char     fname[256];		/* file name			*/
    char     dir[256];			/* working directory		*/
    long     fsize;			/* file size			*/

    char     host[256];			/* remote host name		*/
    int      port;			/* remote port number		*/
    int      mode;			/* push or pull mode		*/

    int      tnum;			/* processing thread number	*/
    long     nbytes;			/* number of bytes to transfer	*/
    long     start;			/* start byte number		*/
    long     end;			/* ending byte number		*/
} psArg, *psArgP;
#endif


/*
pthread_mutex_t svc_mutex = PTHREAD_MUTEX_INITIALIZER;
*/

int     udtSpawnThreads (void *worker, int nthreads, char *dir, char *fname,
                        long fsize, int mode, int rate, int port, char *host,
                        int verbose, pthread_t *tids);
int    *udtCollectThreads (int nthreads, pthread_t *tids);


void 	udtSendFile (void *data);
void 	udtReceiveFile (void *data);

void 	udtComputeStripe (long fsize, int nthreads, int tnum,
    		long *chsize, long *start, long *end);

int 	udtSendStripe (int s, unsigned char *dbuf, long offset, int tnum,
    		long maxbytes);
unsigned char *udtReceiveStripe (int s, long offset, int tnum);


#ifdef _UDT_SRC_


#endif




