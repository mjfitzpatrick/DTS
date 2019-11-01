/**
 *  @file       dtsPSock.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/10/09
 *
 *  @brief  DTS parallel socket transfer routines.
 *
 *
 */


#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>
#include <ctype.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>

#include <sys/errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#define	_PTCP_SRC_

#include "dts.h"
#include "dtsPSock.h"



/**
 *   Mutex lock for thread startup to protect file I/O.
 */ 
pthread_mutex_t svc_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 *   FIXME -- CS_STRIPE and CS_PACKET not working......
 */
int	psock_checksum_policy	= CS_CHUNK;
int	err_return		= -1;
int	thread_sem		= 0;
    
extern struct timeval io_tv;
extern int   queue_delay;
extern int   first_write;	/* keep track of whether thread has written   */
extern DTS  *dts;

void dts_printPHdr (char *s, phdr *h);



/**
 *  PSSPAWNTHREADS -- Spawn a worker thread for the transfer.  All we do here
 *  is start a thread to run the function passed in.  This may be used to
 *  either read or write the data.
 *
 *  @brief  Spawn a worker thread for the transfer.
 *  @fn     int psSpawnThreads (void *worker, int nthreads, char *dir, 
 *		char *fname, long fsize, int mode, int port, char *host, 
 *		int verbose, pthread_t *tids)
 *
 *  @param  worker	worker function 
 *  @param  nthreads	number of threads to create
 *  @param  dir		working directory
 *  @param  fname	file name
 *  @param  fsize	file size
 *  @param  mode	transfer mode (push or pull)
 *  @param  port	client base port number
 *  @param  host	client host name
 *  @param  verbose	verbose output flag
 *  @param  tids	thread id array
 *
 *  @return		status code
 */
int
psSpawnThreads (void *worker, int nthreads, char *dir, char *fname, long fsize, 
	int mode, int port, char *host, int verbose, pthread_t  *tids)
{
    int    t, rc;
    long   start, end, stripeSize;
#ifdef STATIC_ARG
    static psArg  arg[MAX_THREADS];
#else
    psArg  *arg = (psArg *) NULL, *argP = (psArg *) NULL;
#endif
    pthread_attr_t  attr;                       /* thread attributes    */


    /* Initialize the service processing thread attribute.
    */
    pthread_attr_init (&attr);
    pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_JOINABLE);


    /* Do the actual file transfer.
    */
#ifdef STATIC_ARG
    memset (arg, 0, (MAX_THREADS * sizeof(psArg)) );
#endif
    for (t=0; t < nthreads; t++) {

	psComputeStripe (fsize, nthreads, t, &stripeSize, &start, &end);

#ifdef STATIC_ARG
	argp = &arg[t];
#else
        argP = (psArg *) calloc (1, sizeof(psArg));
        arg = argP;
#endif
	argP->tnum   = t;		/* setup thread argument	*/
	argP->port   = port + t;
	argP->mode   = mode;
	strcpy (argP->host, host);
	strcpy (argP->fname, fname);
	strcpy (argP->dir, dir);
	argP->fsize  = fsize;
	argP->nbytes = stripeSize;
	argP->start  = start;
	argP->end    = end;

	if (verbose)
	    dtsErrLog (NULL, "Spawning thread %2d ...%10ld to %10ld\n", 
		t, argP->start, argP->end);
	    
        if ((rc = pthread_create (&tids[t], &attr, (void *)worker, 
		(void *)argP))) {
                    dtsErrLog (NULL, 
			"ERROR: pthread_create() fails, code: %d\n", rc);
                    return (ERR);
        }
    }

    pthread_attr_destroy (&attr);
    return (OK);
}


/**
 *  PSCOLLECTTHREADS -- Collect the worker threads for the transfer.  Our
 *  onl job  here is start a thread to rejoin the previously createed
 *  threads.
 *
 *  @brief  Collect worker threads for the transfer.
 *  @fn     int psCollectThreads (int nthreads, pthread_t *tids)
 *
 *  @param  nthreads	number of threads to create
 *  @param  tids	thread IDs
 *  @return		status code
 *
 */

#define USE_PTHREAD_JOIN

int *
psCollectThreads (int nthreads, pthread_t *tids)
{
#ifdef USE_PTHREAD_JOIN
    int  nerrs = 0;
    void *s;
#endif
    int  t, rc;
    //static int stat[MAX_THREADS];
    int *stat = calloc (MAX_THREADS, sizeof (int));


    memset (stat, 0, (sizeof(int) * MAX_THREADS));
    /* Free attribute and wait for the threads to complete.
    */
#ifdef USE_PTHREAD_JOIN
    for (t=0; t < nthreads; t++) {
        if ((rc = pthread_join (tids[t], (void **)&s)) ) {
            stat[t] =  *((int *) s);
	    nerrs++;
        }
    }
#else

#endif

    return ( nerrs ? stat : NULL );
}



/** 
 *  psSendFile -- Send a file to a remote DTS.
 *
 *  This function can be called to send a portion of a file to a remote host.
 *  Arguments are passed in through a generic 'data' pointer to the psArg 
 *  struct defined for this 'stripe' of the data.
 *  
 *  In this procedure, we act as a server, i.e. we open the specified tcp/ip
 *  socket and wait for a client connection before beginning any transfer.
 *
 *
 *  @brief  Send a file to a remote DTS.
 *  @fn     psSendFile (void *data)
 *
 *  @param  data	caller thread data
 *  @return		nothing
 *
 */
void
psSendFile (void *data)
{
    int   fd, ps=0, ps2=0, sock = 0, lock, status = OK, nread;
    char  *fp = NULL;
    unsigned char *buf = NULL;
    psArg  *arg = data;

    struct timeval tv1, tv2;
    int    sndsz = TCP_WINDOW_SZ, rcvsz = TCP_WINDOW_SZ;
    socklen_t ssz = sizeof(int);
    extern DTS *dts;


    /* We are running in a thread, so change the working directory.
    */
    if (strcmp (arg->dir, "./") != 0) {
        char *pdir  = dts_sandboxPath (arg->dir);

	if (access (pdir, F_OK) != 0)
	    dts_makePath (pdir, TRUE);
        chdir (pdir);
	free ((void *) pdir);
    }

    if (arg->mode == XFER_PUSH) {
        ps = dts_openServerSocket (arg->port);		  /* open as "server" */
	if (ps < 0) {
            dtsErrLog (NULL, 
		"psSendFile: cannot open server socket %d\n", arg->port);
	    return;
	}

        dts_semDecr (thread_sem);

        /* Accept a connection on the port.
        */
        if ((ps2 = accept (ps, NULL, NULL)) < 0) {
            dtsErrLog (NULL, "errno: %s", errno, strerror(errno));
            dtsError ("psSendFile accept() fails");
	}
	sock = ps2;

    } else if (arg->mode == XFER_PULL) {
        ps = dts_openClientSocket (arg->host, arg->port, 1);
	if (ps < 0) {
            dtsErrLog (NULL, 
		"psSendFile: cannot open client socket to %s:%d ps=%d\n",
		arg->host, arg->port, ps);
	    return;
	}
	sock = ps;
    }


    if (PTCP_DEBUG) {
        getsockopt (sock, SOL_SOCKET, SO_SNDBUF, &sndsz, &ssz);
        getsockopt (sock, SOL_SOCKET, SO_RCVBUF, &rcvsz, &ssz);
        dtsErrLog (NULL, "sock:  snd=%d  rcv=%d\n", sndsz, rcvsz);
    }


    /*  Disable the Nagle algorithm to make persistant connections faster.
    if (psock_checksum_policy == CS_NONE && (!DTS_NAGLE)) {
    */
    if ((!DTS_NAGLE)) {
	int ret, flag = 1;

	ret = setsockopt (sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, 
	    sizeof(flag));
	fcntl (sock, F_SETFL, O_NDELAY);
    }
                

    /*  Create a lock on the thread so we have exclusive access to the
    **  file.  Read the chunk we're going to send into memory and release
    **  the lock.
    */
    dts_tstart (&tv1);
    lock = pthread_mutex_lock (&svc_mutex);

    if ((fd = dts_fileOpen ((fp=dts_sandboxPath(arg->fname)),
	O_RDONLY|O_NONBLOCK )) > 0) {

        lseek (fd, (off_t) arg->start, SEEK_SET);
	if (arg->nbytes) {
            buf = calloc (1, arg->nbytes + 1);
	} else {
	    dtsErrLog (NULL, "psSendFile: cannot alloc %d bytes, quitting\n", 
    	        arg->nbytes);
	    free ((void *) fp);
            lock = pthread_mutex_unlock (&svc_mutex);
            pthread_exit (&err_return);
	}

        nread = dts_fileRead (fd, buf, arg->nbytes);	/* read stripe	*/
        dts_fileClose (fd);
	free ((void *) fp);

    } else { 
	/* Cannot open file.
	*/
	dtsErrLog (NULL, "psSendFile:  cannot open '%s' (%s), quitting\n", 
    	    dts_sandboxPath(arg->fname), arg->fname);
	free ((void *) fp);
        lock = pthread_mutex_unlock (&svc_mutex);
        pthread_exit (&err_return);
    }

    lock = pthread_mutex_unlock (&svc_mutex);


    if (PTCP_VERB) {
        dtsTimeLog ("psSend: file read time: %.4g sec\n", tv1);
        dtsErrLog (NULL, 
	    "Send t %d, fsize=%10d  offset=%10d  stripe=%10d port=%d fd=%d\n",
	    arg->tnum, (int)arg->fsize, (int)arg->start, (int)arg->nbytes, 
	    arg->port, sock);
    }
        
    /* Initialize the I/O time counters.
    */
    gettimeofday (&tv1, NULL);

    /* Send the file stripe.
    */
    if (buf) {
        status = psSendStripe (sock, buf, arg->start, arg->tnum, arg->nbytes);
	if (dts->debug > 2)
	    fprintf (stderr, "Stripe %d:  status=%d\n", arg->tnum, status);
    }
    if (PTCP_VERB)
        dtsTimeLog ("psSend: stripe send time: %.4g sec\n", tv1);


    /* Update the I/O time counters.
    */
    gettimeofday (&tv2, NULL);
    io_tv.tv_sec += tv2.tv_sec - tv1.tv_sec;
    io_tv.tv_usec += tv2.tv_usec - tv1.tv_usec;
    if (io_tv.tv_usec >= 1000000) {
        io_tv.tv_usec -= 1000000; io_tv.tv_sec++;
    } else if (io_tv.tv_usec < 0) {
        io_tv.tv_usec += 1000000; io_tv.tv_sec--;
    }


    /* Free the data buffer in memory, close our part of the socket.
    */
    if (buf) {
	if (buf[arg->nbytes] != 0)
            dtsError ("psSendFile: Buffer overflow");
        free ((char *) buf);
    }

    if (ps2 && (close (ps2) < 0) )
        dtsError ("psSendFile: Socket (ps2) shutdown fails");
    if (ps && (close (ps) < 0) )
        dtsError ("psSendFile: Socket (ps) shutdown fails");

#ifndef STATIC_ARG
    if (arg) free ((void *) arg);
#endif
    pthread_exit (NULL);
}


/** 
 *  psReceiveFile -- Receive a file from a remote DTS.
 *
 *  This function can be called to read a portion of a file from a remote 
 *  host.  Arguments are passed in through a generic 'data' pointer to the 
 *  psArg struct defined for this 'stripe' of the data.
 *  
 *  In this procedure, we act as a client in the connection, i.e. the
 *  transfer won't begin until we connect to a remote server sending the 
 *  data.
 *
 *
 *  @brief  Read a file from a remote DTS.
 *  @fn     psReceiveFile (void *data)
 *
 *  @param  data	caller thread data
 *  @return		nothing
 *
 */
void
psReceiveFile (void *data)
{
    int    ps=0, ps2=0, sock = 0, fd = 0, lock, nwrote;
    unsigned char *dbuf;
    psArg *arg = data;
    struct timeval tv1, tv2;


    /* We are running in a thread, so change the working directory.
    */
    if (strcmp (arg->dir, "./") != 0) {
        char *pdir  = dts_sandboxPath (arg->dir);

	if (access (pdir, F_OK) != 0)
	    dts_makePath (pdir, TRUE);
        chdir (pdir);
	free ((void *) pdir);
    }


    if (arg->mode == XFER_PUSH) {
        ps = dts_openClientSocket (arg->host, arg->port, 3);
        if (ps < 0) {
            dtsErrLog (NULL, 
		"psReceiveFile: cannot open client socket to %s:%d\n",
                arg->host, arg->port);
            return;
        }
	sock = ps;

    } else if (arg->mode == XFER_PULL) {
        ps = dts_openServerSocket (arg->port);		  /* open as "server" */
        if (ps < 0) {
            dtsErrLog (NULL, "psReceiveFile: cannot open server socket %d\n",
		arg->port);
            return;
        }

	dts_semDecr (thread_sem);

        /* Accept a connection on the port.
        */
        if ((ps2 = accept (ps, NULL, NULL)) < 0) {
            dtsErrLog (NULL, "errno: %s", errno, strerror(errno));
            dtsError ("psReceiveFile accept() fails");
	}
	sock = ps2;
    }


    /*  Disable the Nagle algorithm to make persistant connections faster.
    if (psock_checksum_policy == CS_NONE && (!DTS_NAGLE)) {
    */
    if (!DTS_NAGLE) {
	int ret, flag = 1;

	ret = setsockopt (sock, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, 
	    sizeof(flag));
	fcntl (sock, F_SETFL, O_NDELAY);
    }
                
                
    if (PTCP_VERB) {
        dtsErrLog (NULL, 
	    "Recv t %d, fsize=%10d  offset=%10d  chunk=%10d  %d/%d %d\n",
	    arg->tnum, (int)arg->fsize, (int)arg->start, (int)arg->nbytes,
	    arg->port, sock, fd);
    }


    /* Initialize the transfer timing.
    */
    gettimeofday(&tv1, NULL);
    memset (&tv2, 0, sizeof (struct timeval));
    gettimeofday (&tv1, NULL);


    /* Receive the file stripe.
    */
    dbuf = psReceiveStripe (sock, arg->start, arg->tnum);

    if (dts->debug > 2)
        dtsLog (dts, "psReceive: file recv time: %.4g sec\n", dts_tstop (tv1));

    /* Check for a special NULL name to indicate we don't want to save
     * the data.
     */
    if (strcmp (arg->fname, "DTSNull") != 0) {

        /*  Create a lock on the thread so we have exclusive access to the 
        **  output file.  Once we write our data to the file, free the data
        **  buffer and unlock the thread.
        */
        lock = pthread_mutex_lock (&svc_mutex);

        if (access (arg->fname, W_OK) != 0) {
            struct timeval t1 = {0, 0};

            /* File doesn't exist, pre-allocate it.
	    */
	    dts_tstart (&t1);
	    if (dts_preAlloc (arg->fname, arg->fsize) != OK) {
	        if (dbuf) free ((void *) dbuf);
                lock = pthread_mutex_unlock (&svc_mutex);
                pthread_exit (&err_return);
	    }
            if (dts->debug > 2)
                dtsLog (dts, "psReceive: file prealloc time: %.4g sec\n",
                    dts_tstop (t1));

        } else {
	    /* File exists, check the size, decide whether we want to 
	    ** allow overwrite, etc. ....
	    */
	    ;
        }


        if (first_write) {
            /*  First write gets rid of existing file.
            */ 
            fd = dts_fileOpen ( arg->fname, O_WRONLY|O_CREAT|O_TRUNC);
            first_write = 0;
        } else
            fd = dts_fileOpen ( arg->fname, O_WRONLY|O_CREAT|O_NONBLOCK);

        if ( (fd) > 0 ) {
	    struct timeval t1 = {0, 0};
	    extern DTS *dts;

	    dts_tstart (&t1);
            lseek (fd, (off_t) arg->start, SEEK_SET); /* write data stripe */

	    if (dts->debug > 2)
		dtsLog (dts, "psRcvFile:  tnum=%d  off=%d  nbytes=%d\n",
		    arg->tnum, arg->start, arg->nbytes);
            nwrote = dts_fileWrite (fd, dbuf, arg->nbytes);
	    fsync (fd);				/* sync to disk		   */
            
	    if (TIME_DEBUG)
		dtsTimeLog ("  disk i/o:  %.4g sec\n", t1);
	    if (dbuf) 
		free ((void *) dbuf);

        } else {
	    /* Cannot open file.
	    */
	    dtsError ("psReceiveFile cannot open file");
	    if (dbuf) free ((void *) dbuf);
            lock = pthread_mutex_unlock (&svc_mutex);
            pthread_exit (&err_return);
        }

        lock = pthread_mutex_unlock (&svc_mutex);
    }


    /* Update I/O transfer counter.
    */
    gettimeofday(&tv2, NULL);
    io_tv.tv_sec += tv2.tv_sec - tv1.tv_sec;
    io_tv.tv_usec += tv2.tv_usec - tv1.tv_usec;
    if (io_tv.tv_usec >= 1000000) {
        io_tv.tv_usec -= 1000000; io_tv.tv_sec++;
    } else if (io_tv.tv_usec < 0) {
        io_tv.tv_usec += 1000000; io_tv.tv_sec--;
    }


    if (ps2 && (close (ps2) < 0) )
        dtsError ("psSendFile: Socket (ps2) shutdown fails");
    if (ps && (close (ps) < 0) )
        dtsError ("psSendFile: Socket (ps) shutdown fails");

#ifndef STATIC_ARG
    if (arg) free ((void *) arg);
#endif

    pthread_exit (NULL);
}


/** 
 *  psSendStripe -- Do the actual transfer of the data stripe to the
 *  client connection.  A 'stripe' of data is actually transferred in 
 *  much smaller 'chunks' which can be tuned to be optimal for the 
 *  given connection.  The checksum policy allows us to perform a checksum
 *  of the data either for the entire stripe, or for each individual chunk.
 *  The former is generally more efficient as it involves fewer round-trips
 *  to the client (i.e. send the checksum and wait for verification before
 *  sending next chunk).  File-level checksum policy is enforced by our
 *  parent.
 *
 *  @brief  Do actual transfer of data stripe to the socket
 *  @fn     int psSendStripe (int sock, unsigned char *dbuf, long offset, 
 *		int tnum, long maxbytes)
 *
 *  @param  sock	socket descriptor
 *  @param  dbuf	data buffer
 *  @param  offset	file offset for this stripe
 *  @param  tnum	thread number
 *  @param  maxbytes	max bytes to transfer
 *
 *  @return		number of chunks sent
 *
 */
int
psSendStripe (int sock, unsigned char *dbuf, long offset, int tnum, 
	long maxbytes)
{
    register int npack = 0;
    long     nb, nleft, nwrote = 0, nresend = 0, count = 0;
    long     nbytes = 1, ntry = 0, chunkSize;
    phdr     ip, op;
    unsigned char *sbuf = NULL;
    char     tfmt[SZ_FNAME];
    struct timeval t1 = {0, 0};


    if (dts_debugLevel() > 3)
	fprintf (stderr, "psSendStripe begins\n");

    /*  Initialize.
    */
    chunkSize = min (maxbytes,SZ_XFER_CHUNK);		/*  32KB chunk	*/
    sbuf = calloc (1, chunkSize + 1);
    sbuf[chunkSize] = 0xFF;

    memset (&ip, 0, sizeof (ip));
    memset (&op, 0, sizeof (op));

    /*  Send the chunk size and file offset as the first packet to the 
    **  receiver.
    */
    ip.offset    = offset;
    ip.chunkSize = min(chunkSize,maxbytes);
    ip.maxbytes  = max(chunkSize,maxbytes);
    if ((nb = dts_sockWrite (sock, &ip, sizeof(ip))) < 0)
        dtsError ("dts_sockWrite() fails to init stripe");

    if (TIME_DEBUG) {
	memset (tfmt, 0, SZ_FNAME);
	sprintf (tfmt, "psSendStripe[%d]: ", tnum);
	strcat (tfmt, " network transfer = %.4g sec\n");
	dts_tstart (&t1);
    }

    /* Send the data.
    */
    nleft = maxbytes;
    while (nwrote < maxbytes) {

        if (!dbuf) {
	    dtsErrLog (NULL, "psSendStripe ERR: NULL dbuf n=%ld\n", nwrote);
	    return (ERR);
        }

	ntry = 0;
	count++;
	if (dbuf)
	    memcpy (sbuf, (void *)&dbuf[nwrote], chunkSize);
	nbytes = chunkSize;

resend:
        if (psock_checksum_policy == CS_CHUNK) {
            memset (&ip, 0, sizeof (ip));
	    ip.chunkSize = chunkSize;
	    ip.maxbytes  = maxbytes;
	    ip.sum32     = addcheck32 (sbuf, nbytes);

	    /* Send the packet header with the size and checksums.
	    */
            if (dts_sockWrite (sock, &ip, sizeof(ip)) < 0)
                dtsError ("dts_sockWrite() chunk checksum failure");
        }

	/* Send the data chunk.
	*/
        if (TIME_DEBUG) {
            if (psock_checksum_policy == CS_CHUNK) {
		if (npack > 0 && (npack % 100) == 0) {
	            dtsErrLog (NULL, "Thread[%2d] send packet %4d nbytes %ld\n", 
	                tnum, npack, nbytes);
		}
            }
        }
        if ((nb = dts_sockWrite (sock, sbuf, nbytes)) < 0)
            dtsError ("dts_sockWrite() data chunk failure");

	/* Get confirmation from the client.
	*/
        if (psock_checksum_policy == CS_CHUNK) {
            memset (&op, 0, sizeof (op));
            if (dts_sockRead (sock, &op, sizeof(op)) < 0)
                dtsError ("dts_sockRead() fails to read chunk checksum");

	    /*  Check the 32-bit checksum as a simple error.
	     */
	    if (ip.sum32 != op.sum32) {
	        dtsErrLog (NULL, "Resending[%d:%d] packet %6d  %10u != %10u\n", 
		    tnum, sock, npack, ip.sum32, op.sum32);
	        nresend++;
	        if (ntry++ > MAXTRIES) {
		    dtsError ("maxtries resend overflow");
		    break;
	        }
	        goto resend;
	    }
        }
	npack++;

	nwrote += nb;
	nleft  -= nb;
        memset (sbuf, 0, chunkSize);
	if (nleft > 0 && nleft < chunkSize) {
	    chunkSize = nleft;
	    sbuf[chunkSize] = 0xFF;
	}
        if (nwrote >= maxbytes)
	    break;
    }

    /*  Log debug timing info.
     */
    if (TIME_DEBUG)
	dtsTimeLog (tfmt, t1);
      
    if (PTCP_VERB)
	dtsErrLog (NULL, "Thread[%2d] %ld bytes in %d chunks, %ld resends\n", 
	    tnum, nwrote, npack, nresend);
    if (psock_checksum_policy == CS_CHUNK && nresend > 0)
	dtsErrLog (NULL, "Thread[%2d:%d] %ld bytes %d chunks, %ld resends\n", 
	    tnum, sock, nwrote, npack, nresend);


    /*  Terminate the data transfer by sending a header packet with a negative 
    **  chunk size.
    */
    ip.sum16     = 0;
    ip.sum32     = 0;
    ip.chunkSize = -1;
    ip.maxbytes  = -1;

#ifdef LAST_PACKET
    if (TIME_DEBUG) 
	dts_tstart (&t1);
    if ((nb = dts_sockWrite (sock, &ip, sizeof(ip))) < 0)
        dtsError ("dts_sockWrite() fails to terminate stripe");
    if (TIME_DEBUG) 
	dtsTimeLog ("final header: %g", t1);
#endif

    /* Clean up.
    */
    if (sbuf) {
	if (sbuf[chunkSize] != 0xFF)
            dtsErrLog (NULL, "psSendStripe: Buf overflow %d\n",sbuf[chunkSize]);
	free ((char *) sbuf);
    }

    if (dts_debugLevel() > 3)
	fprintf (stderr, "psSendStripe done\n");

    return (npack);			/* return number of chunks sent	*/
}


/** 
 *  psReceiveStripe -- Do the actual transfer of the data stripe to the
 *  client connection.  A 'stripe' of data is actually transferred in 
 *  much smaller 'chunks' which can be tuned to be optimal for the 
 *  given connection.  The checksum policy allows us to perform a checksum
 *  of the data either for the entire stripe, or for each individual chunk.
 *  The former is generally more efficient as it involves fewer round-trips
 *  to the client (i.e. send the checksum and wait for verification before
 *  sending next chunk).  File-level checksum policy is enforced by our
 *  parent.
 *
 *  @brief  Read data stripe from the socket connection
 *  @fn     uchar *psReceiveStripe (int sock, long offset, int tnum)
 *
 *  @param  sock	socket descriptor
 *  @param  offset	file offset for this stripe
 *  @param  tnum	thread (i.e. stripe) number
 *
 *  @return		a pointer to the data read
 *
 */
unsigned char *
psReceiveStripe (int sock, long offset, int tnum)
{
    register int npack = 0;
    long     nread, chunkSize, maxbytes, nr = 0, nleft = 0;
    phdr     ip, op;
    unsigned char  *rcvbuf = NULL, *dbuf = NULL;
    char     tfmt[SZ_FNAME];
    struct timeval t1 = {0, 0};
	

    if (dts_debugLevel() > 2)
	fprintf (stderr, "psReceiveStripe begins\n");


    /* Get the chunk size and file offset.
    */
    memset (&ip, 0, sizeof (ip));
    if ((nread = dts_sockRead (sock, &ip, sizeof(ip))) < 0)
        dtsError ("dts_sockRead() fails to init chunk/offset");

    chunkSize = min (ip.maxbytes,SZ_XFER_CHUNK);	/*  32KB chunk	*/

    if (PTCP_VERB)
        dtsErrLog (NULL, "recv T[%2d]  sz=%d  off=%d  max=%d\n", tnum,
            (int)ip.chunkSize, (int)ip.offset, (int)ip.maxbytes);
        
    dbuf = calloc (1, ip.maxbytes + 1);
    dbuf[ip.maxbytes] = 0xFF;
    if (psock_checksum_policy != CS_NONE) {
        rcvbuf = calloc (1, ip.maxbytes + 1);
        rcvbuf[ip.maxbytes] = 0xFF;
    }
    maxbytes = ip.maxbytes;

    if (TIME_DEBUG) {
	memset (tfmt, 0, SZ_FNAME);
	sprintf (tfmt, "psReceiveStripe[%d]: ", tnum);
	strcat (tfmt, " net i/o:  %.4g sec\n");
	dts_tstart (&t1);
    }

    /* Start reading the data.
    */
    nleft = maxbytes;
    while (nleft > 0) {

        if (psock_checksum_policy == CS_CHUNK) {
	    /* Get the header giving the chunk size and checksums.
	    */
	    memset (&ip, 0, sizeof (ip));
            if (dts_sockRead (sock, &ip, sizeof(ip)) < 0)
                dtsError ("dts_sockRead() fails header checksum");
	    if (ip.chunkSize < 0)
	        break;
        }

	/* Read to the local data buffer.
	*/
        if ((nread = dts_sockRead (sock, &dbuf[nr], chunkSize)) < 0)
            dtsError ("dts_sockRead() fails");
	if (psock_checksum_policy != CS_NONE)
	    memcpy (rcvbuf, &dbuf[nr], chunkSize);

        if (psock_checksum_policy == CS_CHUNK) {
	    if (TIME_DEBUG && npack % 100 == 0)
	        dtsErrLog (NULL, 
		    "Thread[%2d] recv  packet %4d nread %ld  nleft %ld\n", 
		    tnum, npack, nread, nleft);

	    /* Compute the local checksum for the block.
	    */
	    memset (&op, 0, sizeof (op));
	    op.chunkSize = nread;
	    op.sum32     = addcheck32 (rcvbuf, nread);
        }

	/* Write a valid chunk to the output file.
	*/
	if (psock_checksum_policy == CS_NONE || 
	   (psock_checksum_policy == CS_CHUNK && ip.sum32 == op.sum32)) {
	    nleft -= nread;
	    nr    += nread;
	    if (nleft < chunkSize)
		chunkSize = nleft;
	}

	if (psock_checksum_policy == CS_CHUNK) {
	    /* Reply to the server with values based on what we got.
	    */
	    if (ip.sum32 != op.sum32) {
               dtsErrLog (NULL, 
		    "Requesting resend[%d:%d] packet %6d  %10u != %10u\n",
                    tnum, sock, npack, ip.sum32, op.sum32);
	    }
            if (dts_sockWrite (sock, &op, sizeof(op)) < 0)
                dtsError ("psReceiveStripe send() fails");
        }

	npack++;
    }

    /* Log timing info.
     */
    if (TIME_DEBUG)
	dtsTimeLog (tfmt, t1);
      
    if (PTCP_VERB)
	dtsErrLog (NULL, "Thread %d read %d bytes\n", tnum, (int) nr);

    if (rcvbuf) {
	if (rcvbuf[maxbytes] != 0xFF)
            dtsError ("psReceiveStripe: Buffer overflow");
	free ((void *) rcvbuf);
    }
    if (dbuf[maxbytes] != 0xFF)
        dtsError ("psReceiveStripe: Data buffer overflow");

    if (dts_debugLevel() > 2)
	fprintf (stderr, "psReceiveStripe done\n");
    return (dbuf);
}



/**
 *  PSCOMPUTESTRIPE -- Compute the parameters of a data stripe given the 
 *  file size and number of worker threads.
 *
 *  @brief	Compute the various parameters of a 'stripe'
 *  @fn 	psComputeStripe (long fsize, int nthreads, int tnum, 
 *			long *chsize, long *start, long *end)
 *
 *  @param  fsize	file size
 *  @param  nthreads	numbers of threads being processed
 *  @param  tnum	this thread number
 *  @param  chsize	chunk size
 *  @param  start	starting byte number of stripe (output)
 *  @param  end		ending byte number of stripe (output)
 *
 *  @return		nothing
 *
 */
void
psComputeStripe (long fsize, int nthreads, int tnum,
    long *chsize, long *start, long *end)
{
    long size = fsize, remain = 0;

    if (nthreads == 1) {
        *start  = 0;
        *end    = fsize;
        *chsize = fsize;

    } else {

        size = (int) (fsize / nthreads);
        remain = (int) (fsize  - ( nthreads * size));

        if (tnum < remain) {
            size++;
            *start  = size * tnum;
        } else {
            *start  = ((size + 1) * remain) + ((tnum - remain) * size);
        }
        *end    = (*start) + size - 1;
        *chsize = size;
    }
    
    if (PTCP_DEBUG)
        dtsErrLog (NULL, "size = %ld  remain = %ld  ==> %ld %5ld to %ld\n",
            size, remain, *chsize, *start, *end);
}


/**
 *  DTS_PRINTHDR -- Debug Utility.
 */
void
dts_printPHdr (char *s, phdr *h)
{
    fprintf (stderr, "%s: sum16=%ud sum32=%ud chunk=%d offset=%ld max=%ld\n", s,
	(unsigned short) h->sum16, (unsigned int) h->sum32,
	h->chunkSize, (long) h->offset, (long) h->maxbytes);
    fflush (stderr);
}
