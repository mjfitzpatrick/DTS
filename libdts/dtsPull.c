/**
 *
 *  DTS Pull-Model Transfer Methods
 *
 *  The methods defined here are both the client and server-side code needed
 *  to implement the "pull" model of data transfer.  In this mode, an RPC
 *  command is called to pull data from the source to the destination machine.
 *  The source machine acts as a server, opening the transfer sockets locally
 *  before sending a request to the remote machine to receive the file.  Once
 *  the connection is established, transfer begins.  The return status of
 *  this method then determines whether the transfer was successful.
 *
 *  A sequence diagram of the process would look something like:
 * 
 *    Pull Model:
 * 
 * 	  QMgr			  Host A		Host B
 * 
 *    (1)  ----[Pull Msg] ---------------------------------->
 *
 *    (2)                                             Socket setup
 *
 *    (3)			  <-----[FileSend Msg]------
 *
 *    (4)			  <=====[Data Transfer]=====>
 *
 *    (5)  <----[Status Return]-------
 *
 *  The 'QMgr' is the Queue Manager which initiates the request on the local
 *  machine.
 *
 *  RPC Methods Implemented:
 *
 *	dts_xferPullFile	initiate Pull transfer of file (dest method)
 *	dts_xferSendFile	begin sending file (src method)
 *
 *
 *  @file       dtsPull.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/10/09
 *
 *  @brief  DTS Pull-Model Transfer Methods
 */

/******************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include <pthread.h>

#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <xmlrpc-c/base.h>
#include <xmlrpc-c/client.h>
#include <xmlrpc-c/server.h>
#include <xmlrpc-c/server_abyss.h>

#include "dts.h"
#include "dtsUDT.h"
#include "dtsPSock.h"


extern  DTS  *dts;
extern  int   thread_sem;
extern  int   queue_delay;

extern int dts_nullHandler();
    


/*****************************************************************************/

/**
 *  DTS_XFERPULL -- Pull a file from the src to the dest machine.
 *
 *  RPC Params:
 *      xferId          S	transfer id
 *      method          S	transfer method
 *      fileName        S	file name
 *      fileSize        I	file size
 *      nthreads        I	num of transfer threads to open
 *      srcPort         I	starting port number
 *      destHost        S	FQDN of destination machine
 *      destCmdURL      S	destination xmlrpc URI
 * 
 *  RPC Returns:
 *      tsec		I	transfer time seconds
 *      tusec		I	transfer time micro-seconds
 *      status		I	transfer succeeded (0) or failed (1)
 *  ---------------------------------------------------------------------
 *
 *  @brief		Pull a file from the src to the dest machine.
 *  @fn	int dts_xferPullFile (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code
 */
int 
dts_xferPullFile (void *data)
{
    char  *xferID, *method, *fileName;
    char  *srcHost, *destHost, *srcCmdURL, *destCmdURL;
    char  *srcDir, *destDir, *srcFname, *destFname;
    char  *errMsg = "OK";
    int   nthreads, srcPort, fileSize, res, tsec=0, tusec=0, count;
    int   status = OK, verbose = 0, client = 0, t, async = 0, udt_rate = 0;
    int  *tstat = NULL;
    char  resStr[SZ_CONFIG+1], tlog[SZ_PATH+1], qname[SZ_PATH];

    struct timeval tv1 = {0, 0};
    struct timeval tv2 = {0, 0};

    /* static pthread_t  tids[MAX_THREADS];	*/        
    pthread_t *tids = calloc (MAX_THREADS,sizeof(pthread_t));


    /* Get the RPC arguments to local variables.
    */
    xferID     = xr_getStringFromParam (data, 0);
    method     = xr_getStringFromParam (data, 1);
    fileName   = xr_getStringFromParam (data, 2);
    fileSize   = xr_getIntFromParam    (data, 3);
    nthreads   = xr_getIntFromParam    (data, 4);
    udt_rate   = xr_getIntFromParam    (data, 5);
    srcPort    = xr_getIntFromParam    (data, 6);
    srcHost    = xr_getStringFromParam (data, 7);
    destHost   = xr_getStringFromParam (data, 8);
    srcCmdURL  = xr_getStringFromParam (data, 9);
    destCmdURL = xr_getStringFromParam (data, 10);
    srcDir     = xr_getStringFromParam (data, 11);
    destDir    = xr_getStringFromParam (data, 12);
    srcFname   = xr_getStringFromParam (data, 13);
    destFname  = xr_getStringFromParam (data, 14);

    /* Extract the queue name.
     */
    memset (qname, 0, SZ_PATH);
    strcpy (qname, 
	dts_queueFromPath ((strstr (srcDir, "spool") ? srcDir : destDir)) );


    if (dts->debug > 2) {
        fprintf (stderr, "Pull: xferID='%s'  method='%s'\n", xferID, method);
        fprintf (stderr, "      name='%s'  (%d bytes)\n", fileName, fileSize);
        fprintf (stderr, "      Nthreads=%d port=%d srcHost=%s destHost=%s\n",
                                nthreads, srcPort, srcHost, destHost);
        fprintf (stderr, "      srcCmd=%s\n", srcCmdURL);
        fprintf (stderr, "      destCmd=%s\n", destCmdURL);
        fprintf (stderr, "      srcDir=%s srcFname=%s\n", srcDir, srcFname);
        fprintf (stderr, "      destDir=%s destFname=%s\n", destDir, destFname);
        fprintf (stderr, "      qname=%s\n", qname);
    }
    if (dts->debug > 1)
        dtsLog (dts, "%6.6s <  XFER: method=%s src=%s dest=%s\n", 
	    dts_queueNameFmt (qname), method, srcHost, destHost);
    dtsLog (dts, "%6.6s <  XFER: pull[%d]: file=%s sz=%f\n", 
	dts_queueNameFmt (qname), nthreads, fileName, (fileSize/1045678.0));



    gettimeofday (&tv1, NULL);			/* start transfer timer */

    if (fileSize < MIN_MULTI_FSIZE)
	nthreads = 1;

    /* Create the 'server' sockets.  We do this by creating a socket for each
    ** thread we wish to run, spawning a thread for each that waits for a
    ** connection.
    */
    dts_qstatNetStart (qname);
    if (strncasecmp (method, "psock", 5) == 0) {
        void (*func)(void *data) = psReceiveFile;   /* function to execute  */

    	thread_sem = dts_semInit ((int)srcPort+PID_MAX, nthreads);

        psSpawnThreads (func, nthreads, destDir, destFname, fileSize, 
	    XFER_PULL, srcPort, srcHost, verbose, tids);

    } else if (strncasecmp (method, "udt", 3) == 0) {
        void (*func)(void *data) = udtReceiveFile;  /* function to execute  */

    	thread_sem = dts_semInit ((int)srcPort+PID_MAX, nthreads);

        udtSpawnThreads (func, nthreads, destDir, destFname, fileSize, 
	    XFER_PULL, udt_rate, srcPort, srcHost, verbose, tids);

    } else {
	errMsg = "Unknown Transfer Method";
	status = ERR;
	goto ret_stat;
    }


    /* Wait for the threads to ready their sockets.
     */
    count = dts_semGetVal (thread_sem);
    if (dts->verbose > 2) 
	dtsLog (dts, "xferPullFile: threads started: waiting on %d\n", count);
    while (count > 0) {
        struct timespec delay = { (time_t) 0, (long) 5000 };

        nanosleep (&delay, 0);
        count = dts_semGetVal (thread_sem);
        if (dts->verbose > 2) 
	    dtsLog (dts, "xferPullFile: started: waiting on %d\n", count);
    }
    dts_semRemove (thread_sem);
    if (dts->verbose > 2) 
	dtsLog (dts, "xferPullFile:  threads started: starting send\n", count);


    /* Sockets are now ready, initiate the message to the destination to make
    ** a connection.  The transfer begins in each thread automatically once
    ** the socket is established.
    ** 
    ** Because we might have been commanded remotely, establish a new client
    ** connection each time we're called.
    */
    client = xr_initClient (srcCmdURL, "xferPullFile", "v1.0");

    if (DTS_ASYNC) {
        async = xr_newASync (client);

        xr_setStringInParam (async, xferID); 	/* set calling params	*/
        xr_setStringInParam (async, method); 
        xr_setStringInParam (async, fileName); 
        xr_setIntInParam    (async, fileSize); 
        xr_setIntInParam    (async, nthreads); 
        xr_setIntInParam    (async, udt_rate); 
        xr_setIntInParam    (async, srcPort); 
        xr_setStringInParam (async, destHost); 
        xr_setStringInParam (async, srcDir); 

        res = xr_callASync (async, "sendFile", dts_nullHandler);

    } else {
        xr_initParam (client);

        xr_setStringInParam (client, xferID); 	/* set calling params	*/
        xr_setStringInParam (client, method); 
        xr_setStringInParam (client, fileName); 
        xr_setIntInParam    (client, fileSize); 
        xr_setIntInParam    (client, nthreads); 
        xr_setIntInParam    (client, udt_rate); 
        xr_setIntInParam    (client, srcPort); 
        xr_setStringInParam (client, destHost); 
        xr_setStringInParam (client, srcDir); 

        if (xr_callSync (client, "sendFile") == OK) {/* make the call	*/
	    xr_getIntFromResult (client, &res);
        } else {
	    dtsErrLog (NULL, "Call to sendFile fails\n");
	    res = ERR;
        }

        /* xr_clientCleanup (client); */
        xr_closeClient (client);
    }

    if ((status = res) != OK)
	goto ret_stat;


    /* Wait for sending threads to complete.  A failure on any one thread will
    ** force the entire method to fail.  The use of psCollectThreads()
    ** shouldn't matter when using UDT for transport.
    */
    if ((tstat = psCollectThreads (nthreads, tids))) {
        for (t=0; t < nthreads; t++) {
            if ( tstat[t] )
                dtsLog (dts, "%6.6s <  XFER: thread %d error: stat=%d\n", 
		    dts_queueNameFmt (qname), t, tstat[t]);
        }
    }


    /*  Stop transfer timer and calculate the transfer time return values.
    */
    gettimeofday (&tv2, NULL);			
    tsec = tv2.tv_sec - tv1.tv_sec;
    tusec = tv2.tv_usec - tv1.tv_usec;
    if (tusec >= 1000000) {
        tusec -= 1000000; tsec++;
    } else if (tusec < 0) {
        tusec += 1000000; tsec--;
    }
    memset (tlog, 0, SZ_PATH);
    sprintf (tlog, "%6.6s <  XFER: done[%d]: T=%.3f sec %.2f Mbs %.2f MB/s\n", 
	dts_queueNameFmt (qname), nthreads,
	((double)tsec + (double)tusec / 1000000.0),
	transferMb ((long)fileSize, tsec, tusec),
	transferMB ((long)fileSize, tsec, tusec));
    dtsLog (dts, tlog);
    dts_qstatNetEnd (qname);


    /* Set the XML-RPC status return code.
    */
ret_stat:
    memset (resStr, 0, SZ_LINE);
    sprintf (resStr, "%d %d %d", tsec, tusec, status);
    xr_setStringInResult (data, resStr);


    /* Free memory allocate when we got the params.
     */
    free ((char *) xferID);
    free ((char *) method);
    free ((char *) fileName);
    free ((char *) srcHost);
    free ((char *) destHost);
    free ((char *) srcCmdURL);
    free ((char *) destCmdURL);
    free ((char *) srcDir);
    free ((char *) destDir);
    free ((char *) srcFname);
    free ((char *) destFname);

    free ((char *) tids);
    free ((char *) tstat);

    return (OK);
}



/**
 *  DTS_XFERSENDFILE -- Send a file to the dest machine.
 *
 *  RPC Params:
 *      xferId          S	transfer id
 *      fileName        S	file name
 *      fileSize        I	file size
 *      nthreads        I	num of transfer threads to open
 *      srcPort         I	starting port number
 *      srcIP		S	IP address of caller (string)
 * 
 *  RPC Return:
 *      0		transfer succeeded
 *      1		transfer failed
 *  ---------------------------------------------------------------------
 *
 *  @brief		Send a file from the src to the dest machine.
 *  @fn	int dts_xferSendFile (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code
 */
int 
dts_xferSendFile (void *data)
{

    char  *xferID, *fileName, *destIP, *method, *dir, *errMsg, tlog[SZ_PATH];
    char   qname[SZ_PATH];
    int   nthreads, destPort, fileSize, *tstat = NULL;
    int   t, status = OK, verbose = 0, udt_rate = 0;
    struct timeval  t1, t2, tv;
    /* static pthread_t  tids[MAX_THREADS]; */
    pthread_t *tids = calloc (MAX_THREADS, sizeof (pthread_t));

    extern double transferMb(), transferMB();


#ifdef USE_FORK
    /* Handle processing in the background.
     */
    switch (fork ()) {
    case 0:
	//dtsSleep (1);
        break;                              /* We are the child     */
    case -1:
        return (ERR);                       /* We are an error      */
    default:
        xr_setIntInResult (data, (int) OK);
        return (OK);                        /* We are the parent    */
    }
#endif

    if (queue_delay)
	dtsSleep (queue_delay);


    /* Get the RPC arguments to local variables.
    */
    xferID     = xr_getStringFromParam (data, 0);
    method     = xr_getStringFromParam (data, 1);
    fileName   = xr_getStringFromParam (data, 2);
    fileSize   = xr_getIntFromParam    (data, 3);
    nthreads   = xr_getIntFromParam    (data, 4);
    udt_rate   = xr_getIntFromParam    (data, 5);
    destPort   = xr_getIntFromParam    (data, 6);
    destIP     = xr_getStringFromParam (data, 7);
    dir        = xr_getStringFromParam (data, 8);

    /* Extract the queue name.
     */
    memset (qname, 0, SZ_PATH);
    strcpy (qname, 
	dts_queueFromPath ((strstr (dir, "spool") ? dir : fileName)) );

    if (dts->debug > 2) {
        fprintf (stderr, "sendFile: xferID='%s'\n", xferID);
        fprintf (stderr, "      name='%s'  (%d bytes)\n", fileName, fileSize);
        fprintf (stderr, "      Nthreads=%d port=%d  destIP=%s\n",
                                nthreads, destPort, 
				(destIP ? destIP : "none"));
        fprintf (stderr, "      dir=%s\n", (dir ? dir : "none"));
    }
    dtsLog (dts, "%6.6s >  XFER: send[%s] src=%s size=%.2fMB\n", 
	dts_queueNameFmt (qname), fileName, destIP, (fileSize / 1045678.0));


    /* Create the 'client' sockets and connect to the source machine.  This
    ** will initiate the transfer automatically so all we need to do is
    ** wait for the threads to complete.
    */
    gettimeofday (&t1, NULL);

    if (strcasecmp (method, "psock") == 0) {
        void (*func)(void *data) = psSendFile;   /* function to execute  */

        psSpawnThreads (func, nthreads, dir, fileName, fileSize, 
	    XFER_PULL, destPort, destIP, verbose, tids);

    } else if (strcasecmp (method, "udt") == 0) {
        void (*func)(void *data) = udtSendFile;  /* function to execute  */

        udtSpawnThreads (func, nthreads, dir, fileName, fileSize, 
	    XFER_PULL, udt_rate, destPort, destIP, verbose, tids);

    } else {
	errMsg = "Unknown Transfer Method";
	status = ERR;
	goto ret_stat;
    }


    /* Wait for sending threads to complete.  A failure on any one thread will
    ** force the entire method to fail.
    */
    if ((tstat = psCollectThreads (nthreads, tids))) {
        for (t=0; t < nthreads; t++) {
            if ( tstat[t] )
                dtsLog (dts, "%6.6s >   XFER: thread %d error: stat=%d\n", 
		    dts_queueNameFmt (qname), t, tstat[t]);
        }
    }


    /* Update the I/O time counters.
    */
    gettimeofday (&t2, NULL);
    tv.tv_sec = t2.tv_sec - t1.tv_sec;
    tv.tv_usec = t2.tv_usec - t1.tv_usec;
    if (tv.tv_usec >= 1000000) {
        tv.tv_usec -= 1000000; tv.tv_sec++;
    } else if (tv.tv_usec < 0) {
        tv.tv_usec += 1000000; tv.tv_sec--;
    }
    memset (tlog, 0, SZ_PATH);
    sprintf (tlog, "%6.6s >  XFER: done[%d]: T=%.3f sec %.2f Mbs %.2f MB/s\n", 
	dts_queueNameFmt (qname), nthreads,
	((double)tv.tv_sec + (double)tv.tv_usec / 1000000.0),
	transferMb ((long)fileSize, tv.tv_sec, tv.tv_usec),
	transferMB ((long)fileSize, tv.tv_sec, tv.tv_usec));
    dtsLog (dts, tlog);


    /* Set the XML-RPC status return code.
    */
ret_stat:
    xr_setIntInResult (data, status);

    /* Free memory allocate when we got the params.
     */
    free ((char *) xferID);
    free ((char *) method);
    free ((char *) fileName);
    free ((char *) destIP);
    free ((char *) dir);

    free ((char *) tids);
    free ((char *) tstat);

#ifdef USE_FORK
    exit (0);
#else
    return (OK);
#endif
}
