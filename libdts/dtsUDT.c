/**
 *  DTSUDT.C -- DTS UDT transfer routines.
 *
 *  @file       dtsUDT.c
 *  @author     Mike Fitzpatrick, Travis Semple, NOAO
 *  @date       3/10/13
 *
 *  @brief  DTS UDT transfer routines.
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
#include "dtsUDT.h"
#include "dtsPSock.h"
#include "udtc.h"


#define  UDT_DEBUG	0

/**
 *   Mutex lock for thread startup to protect file I/O.
 */ 
pthread_mutex_t udt_mutex = PTHREAD_MUTEX_INITIALIZER;

/**
 *   FIXME -- CS_STRIPE and CS_PACKET not working......
 */
int	udt_checksum_policy	= CS_NONE;

    
extern struct timeval io_tv;
extern int    queue_delay;
extern int    thread_sem;
extern DTS   *dts;
extern int    first_write;

static int    err_return	= -1;



/**
 *  UDTSPAWNTHREADS -- Spawn a worker thread for the transfer.  All we do here
 *  is start a thread to run the function passed in.  This may be used to
 *  either read or write the data.
 *
 *
 *  @brief  Spawn a worker thread for the transfer.
 *  @fn     int udtSpawnThreads (void *worker, int nthreads, char *dir, 
 *		char *fname, long fsize, int mode, int rate, int port, 
 *		char *host, int verbose, pthread_t *tids)
 *
 *  @param  worker	worker function 
 *  @param  nthreads	number of threads to create
 *  @param  dir		working directory
 *  @param  fname	file name
 *  @param  fsize	file size
 *  @param  mode	transfer mode (push or pull)
 *  @param  rate	transfer rate (Mbps)
 *  @param  port	client base port number
 *  @param  host	client host name
 *  @param  verbose	verbose output flag
 *  @param  tids	thread id array
 *
 *  @return		status code
 *
 */
int
udtSpawnThreads (void *worker, int nthreads, char *dir, char *fname, 
	long fsize, int mode, int rate, int port, char *host, int verbose, 
	pthread_t  *tids)
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
	argP->rate   = rate;
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
 *  UDTCOLLECTTHREADS -- Collect the worker threads for the transfer.  Our
 *  onl job  here is start a thread to rejoin the previously createed
 *  threads.
 *
 *  @brief  Collect worker threads for the transfer.
 *  @fn     int udtCollectThreads (int nthreads, pthread_t *tids)
 *
 *  @param  nthreads	number of threads to create
 *  @param  tids	thread IDs
 *  @return		status code
 *
 */

#define USE_PTHREAD_JOIN

int *
udtCollectThreads (int nthreads, pthread_t *tids)
{
#ifdef USE_PTHREAD_JOIN
    int  nerrs = 0;
    void *s;
#endif
    int  t, rc;
    //static int stat[MAX_THREADS];
    int *stat = calloc (MAX_THREADS, sizeof (int));

    memset (stat, 0, MAX_THREADS);
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
    ;
#endif

    return ( nerrs ? stat : NULL );
}


/** 
 *  UDTSENDFILE -- Send a file to a remote DTS using UDT.
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
 *  @fn     udtSendFile (void *data)
 *
 *  @param  data	caller thread data
 *  @return		nothing
 *
 */
void
udtSendFile (void *data)
{
    int  fd, ps=0, ps2=0, sock = 0, lock, status = OK, udt_rate = 0, nread;
    char *fp = NULL;
    unsigned char *buf = NULL;
    psArg  *arg = data;

    struct timeval tv1, tv2;
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
	/*  FIXME -- openUDTServerSocket returns a UDTSOCKET which is 
	 *  actually an int.
	 */
	/* Open as "server" */
        ps = dts_openUDTServerSocket (arg->port, arg->rate);  
	if (ps < 0) {
            dtsErrLog (NULL, 
		"udtSendFile: cannot open server socket %d\n", arg->port);
	    return;
	}

        dts_semDecr (thread_sem);

        /* Accept a connection on the port.
        */
        if ((ps2 = udt_accept (ps, NULL, NULL)) < 0) {
            dtsErrLog (NULL, "errno: %s", errno, strerror(errno));
            dtsError ("udtSendFile accept() fails");
	}

	/*  Get handle to our accepted socket and pass in rate.
	 */
#ifdef DEFAULT_UDT_RATE
        udt_rate = ((arg->rate > 0) ? arg->rate : DEF_UDT_RATE);
#else
        udt_rate = arg->rate;
#endif
	if (udt_rate > 0) {
            if (udt_getsockopt_cc (ps2, udt_rate) == 0) {
                if (UDT_DEBUG)
	            dtsLog (dts, "udtSendFile: transfer rate set to %d Mbps\n", 
    	                udt_rate);
            } else
                dtsErrLog (NULL, "udtSendFile: UDT transfer rate not set\n");
	}

	sock = ps2;

    } else if (arg->mode == XFER_PULL) {
        ps = dts_openUDTClientSocket (arg->host, arg->port, 1);
	if (ps < 0) {
            dtsErrLog (NULL, 
		"udtSendFile: cannot open client socket to %s:%d ps=%d\n",
		arg->host, arg->port, ps);
	    return;
	}
	sock = ps;
    }


    /*  Create a lock on the thread so we have exclusive access to the
    **  file.  Read the chunk we're going to send into memory and release
    **  the lock.
    */
    dts_tstart (&tv1);
    lock = pthread_mutex_lock (&udt_mutex);

    if ((fd = dts_fileOpen ( (fp=dts_sandboxPath(arg->fname)),O_RDONLY )) > 0) {

        lseek (fd, (off_t) arg->start, SEEK_SET);
	if (arg->nbytes) {
            buf = calloc (1, arg->nbytes + 1);
	} else {
	    dtsLog (dts, "udtSendFile: cannot alloc %d bytes, quitting\n", 
    	        arg->nbytes);
	    free ((void *) fp);
            lock = pthread_mutex_unlock (&udt_mutex);
            pthread_exit (&err_return);
	}

        nread = dts_fileRead (fd, buf, arg->nbytes);	/* read stripe	*/
        dts_fileClose (fd);
	free ((void *) fp);

    } else { 
	/* Cannot open file.
	*/
	dtsLog (dts, "udtSendFile:  cannot open '%s' (%s), quitting\n", 
    	    dts_sandboxPath(arg->fname), arg->fname);
	free ((void *) fp);
        lock = pthread_mutex_unlock (&udt_mutex);
        pthread_exit (&err_return);
    }

    lock = pthread_mutex_unlock (&udt_mutex);
    if (dts->debug > 1)
        dtsTimeLog ("udtSend: file read time: %.4g sec\n", tv1);


    if (PTCP_VERB) {
        dtsErrLog (NULL, 
	    "Send t %d, fsize=%10d  offset=%10d  stripe=%10d port=%d fd=%d\n",
	    arg->tnum, (int)arg->fsize, (int)arg->start, (int)arg->nbytes, 
	    arg->port, sock);
    }
        
    /* Initialize the I/O time counters.
    */
    gettimeofday (&tv1, NULL);

    /* Send the file / stripe.
    */
    if (buf) {
        status = udtSendStripe (sock, buf, arg->start, arg->tnum, arg->nbytes);
	if (dts->debug > 2)
	    fprintf (stderr, "Stripe %d:  status=%d\n", arg->tnum, status);
    }
    if (dts->debug > 1)
        dtsTimeLog ("udtSend: stripe send time: %.4g sec\n", tv1);


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
            dtsError ("udtSendFile: Buffer overflow");
        free ((char *) buf);
    }

    if (ps2 && (udt_close (ps2) < 0) )
        dtsError ("udtSendFile: Socket (ps2) shutdown fails");
    if (ps && (udt_close (ps) < 0) )
        dtsError ("udtSendFile: Socket (ps) shutdown fails");

#ifndef STATIC_ARG
    if (arg) free ((void *) arg);
#endif
    udt_cleanup ();
    pthread_exit (NULL);
}


/** 
 *  UDTRECEIVEFILE -- Receive a file from a remote DTS.
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
 *  @fn     udtReceiveFile (void *data)
 *
 *  @param  data	caller thread data
 *  @return		nothing
 *
 */
void
udtReceiveFile (void *data)
{
    int    ps=0, ps2=0, sock = 0, fd = 0, lock, nwrote, udt_rate = 0;
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
        ps = dts_openUDTClientSocket (arg->host, arg->port, 3);
        if (ps < 0) {
            dtsErrLog (NULL, 
		"udtReceiveFile: cannot open client socket to %s:%d\n",
                arg->host, arg->port);
            return;
        }
	sock = ps;

    } else if (arg->mode == XFER_PULL) {
	/* Open as "server" */
        ps = dts_openUDTServerSocket (arg->port, arg->rate);
        if (ps < 0) {
            dtsErrLog (NULL, "udtReceiveFile: cannot open server socket %d\n",
		arg->port);
            return;
        }

	dts_semDecr (thread_sem);

        /* Accept a connection on the port.
        */
        if ((ps2 = udt_accept (ps, NULL, NULL)) < 0) {
            dtsErrLog (NULL, "errno: %s", errno, strerror(errno));
            dtsError ("udtReceiveFile accept() fails");
	}

	/*  Get handle to our accepted socket and pass in rate.
	 */
#ifdef DEFAULT_UDT_RATE
        udt_rate = ((arg->rate > 0) ? arg->rate : DEF_UDT_RATE);
#else
        udt_rate = arg->rate;
#endif
	if (udt_rate > 0) {
            if (udt_getsockopt_cc (ps2, udt_rate) == 0 && dts->debug ) {
                if (UDT_DEBUG)
	            dtsLog (dts, 
			"udtReceiveFile: transfer rate set to %d Mbps\n", 
    	            	udt_rate);
            } else
                dtsErrLog (NULL, "udtReceiveFile: UDT transfer rate not set\n");
	}

	sock = ps2;
    }


    if (PTCP_VERB) {
        dtsErrLog (NULL, 
	    "Recv t %d, fsize=%10d  offset=%10d  chunk=%10d  %d/%d %d\n",
	    arg->tnum, (int)arg->fsize, (int)arg->start, (int)arg->nbytes,
	    arg->port, sock, fd);
    }


    /* Initialize the transfer timing.
    */
    memset (&tv1, 0, sizeof (struct timeval));
    memset (&tv2, 0, sizeof (struct timeval));
    gettimeofday (&tv1, NULL);


    /* Receive the file stripe.
    */
    dbuf = udtReceiveStripe (sock, arg->start, arg->tnum);

    if (dts->debug > 2)
        dtsLog (dts, "udtReceive: file recv time: %.4g sec\n", dts_tstop (tv1));

    /* Check for a special NULL name to indicate we don't want to save
     * the data.
     */
    if (strcmp (arg->fname, "DTSNull") != 0) {

        /*  Create a lock on the thread so we have exclusive access to the 
        **  output file.  Once we write our data to the file, free the data
        **  buffer and unlock the thread.
        */
        lock = pthread_mutex_lock (&udt_mutex);

        if (access (arg->fname, W_OK) != 0) {
	    struct timeval t1 = {0, 0};

            /* File doesn't exist, pre-allocate it.
	    */
	    dts_tstart (&t1);
	    if (dts_preAlloc (arg->fname, arg->fsize) != OK) {
	        if (dbuf) free ((void *) dbuf);
                lock = pthread_mutex_unlock (&udt_mutex);
                pthread_exit (&err_return);
	    }
    	    if (dts->debug > 2)
        	dtsLog (dts, "udtReceive: file prealloc time: %.4g sec\n", 
		    dts_tstop (t1));

        } else {
	    /*  FIXME -- File exists, check the size, decide whether we want
	    **  to allow overwrite, etc. ....
	    */
	    ;
	}


	if (first_write) {
	    /*  First write gets rid of existing file.
	     */
	    fd = dts_fileOpen ( arg->fname, O_WRONLY|O_CREAT|O_TRUNC); 
	    first_write = 0;
	} else
	    fd =dts_fileOpen ( arg->fname, O_WRONLY|O_CREAT);
		
        if ((fd) > 0) {
	    struct timeval t1 = {0, 0};

	    dts_tstart (&t1);
            lseek (fd, (off_t) arg->start, SEEK_SET); /* write data stripe */
            nwrote = dts_fileWrite (fd, dbuf, arg->nbytes);
	    fsync (fd);				/* sync to disk		   */
            
    	    if (dts->debug > 1) {
		double  t = dts_tstop (t1);
        	dtsLog (dts, "udtReceive: write time: %.4g sec  %.4g MBps\n", 
		    t, ((double)arg->nbytes) / t / 1045876.0);
 	    }
	    if (TIME_DEBUG)
		dtsTimeLog ("  disk i/o:  %.4g sec\n", t1);
	    if (dbuf) 
		free ((void *) dbuf);

        } else {
	    /* Cannot open file.
	    */
	    if (dbuf) free ((void *) dbuf);
            lock = pthread_mutex_unlock (&udt_mutex);
            pthread_exit (&err_return);
        }

        lock = pthread_mutex_unlock (&udt_mutex);
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


    if (ps2 && (udt_close (ps2) < 0) )
        dtsError ("udtSendFile: Socket (ps2) shutdown fails");
    if (ps && (udt_close (ps) < 0) )
        dtsError ("udtSendFile: Socket (ps) shutdown fails");

#ifndef STATIC_ARG
    if (arg) free ((void *) arg);
#endif

    udt_cleanup ();
    pthread_exit (NULL);
}


/** 
 *  UDTSENDSTRIPE -- Do the actual transfer of the data stripe to the
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
 *  @fn     int udtSendStripe (int sock, unsigned char *dbuf, long offset, 
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
udtSendStripe (int sock, unsigned char *dbuf, long offset, int tnum, 
	long maxbytes)
{
    register int npack = 1;
    long     chunkSize, n = 0;
    phdr     ip;
    char     tfmt[SZ_FNAME];
    struct timeval t1 = {0, 0};


    if (dts_debugLevel() > 3)
	fprintf (stderr, "udtSendStripe begins\n");

    /*  Initialize.
    */
    chunkSize = maxbytes;


    /*  Send the chunk size and file offset as the first packet to the 
    **  receiver.
    */
    memset (&ip, 0, sizeof (ip));
    ip.offset    = offset;
    ip.chunkSize = maxbytes;
    ip.maxbytes  = maxbytes;
    if (dts_udtWrite (sock, (char *) &ip, (long) sizeof(ip), 0) < 0)
        dtsError ("dts_udtWrite() fails to init stripe");

    if (TIME_DEBUG) {
	memset (tfmt, 0, SZ_FNAME);
	sprintf (tfmt, "udtSendStripe[%d]: ", tnum);
	strcat (tfmt, " network transfer = %.4g sec\n");
	dts_tstart (&t1);
    }

    /* Send the data.
    */
    if ((n=dts_udtWrite (sock, (char *) &dbuf[offset], maxbytes, 364000)) < 0) {
	dtsErrLog (NULL, "UDT Send Error[%d]: %s\n", 
	    udt_getlasterror_code(), udt_getlasterror_desc());
    }

    /*  Log debug timing info.
     */
    if (TIME_DEBUG)
	dtsTimeLog (tfmt, t1);
      
    /* Clean up.
    */
    if (dts_debugLevel() > 3)
	fprintf (stderr, "udtSendStripe done\n");

    return (npack);			/* return number of chunks sent	*/
}


/** 
 *  UDTRECEIVESTRIPE -- Do the actual transfer of the data stripe to the
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
 *  @fn     uchar *udtReceiveStripe (int sock, long offset, int tnum)
 *
 *  @param  sock	socket descriptor
 *  @param  offset	file offset for this stripe
 *  @param  tnum	thread (i.e. stripe) number
 *
 *  @return		a pointer to the data read
 *
 */
unsigned char *
udtReceiveStripe (int sock, long offset, int tnum)
{
    long     maxbytes, n = 0;
    phdr     ip;
    unsigned char  *dbuf = NULL;
    char     tfmt[SZ_FNAME];
    struct timeval t1 = {0, 0};
	

    if (dts_debugLevel() > 2)
	dtsLog (dts, "udtReceiveStripe begins\n");


    /* Get the chunk size and file offset.
    */
    memset (&ip, 0, sizeof (ip));
    if (dts_udtRead (sock, (char *) &ip, (long) sizeof(ip), (int) 0) < 0)
        dtsError ("dts_udtRead() fails to init chunk/offset");

    maxbytes  = ip.maxbytes;

    dbuf = calloc (1, ip.maxbytes + 1);
    dbuf[ip.maxbytes] = 0xFF;

    if (TIME_DEBUG) {
	memset (tfmt, 0, SZ_FNAME);
	sprintf (tfmt, "udtReceiveStripe[%d]: ", tnum);
	strcat (tfmt, " net i/o:  %.4g sec\n");
	dts_tstart (&t1);
    }

    /* Start reading the data.
    */
    if ((n=dts_udtRead (sock, (char *) dbuf, maxbytes, (int) 7320000)) < 0)
        dtsError ("dts_udtRead() fails");

    /* Log timing info.
     */
    if (TIME_DEBUG)
	dtsLog (dts, tfmt, t1);
      
    if (dbuf[maxbytes] != 0xFF)
        dtsError ("udtReceiveStripe: Data buffer overflow");

    if (dts_debugLevel() > 2)
	dtsLog (dts, "udtReceiveStripe done\n");

    return (dbuf);
}


/**
 *  UDTCOMPUTESTRIPE -- Compute the parameters of a data stripe given the 
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
udtComputeStripe (long fsize, int nthreads, int tnum,
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
