/**
 *  DTSQUEUE.C  -- DTS queue methods.
 *
 *
 *  @file  	dtsQueue.c
 *  @author  	Mike Fitzpatrick, NOAO
 *  @date  	6/10/09
 *
 *  @brief  DTS queue methods.
 */


/*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#include <stdarg.h>

#include "dts.h"


extern  DTS  *dts;
extern  int   dts_monitor;
extern  int   queue_delay;


#define	DEBUG		(dts&&dts->debug>2)



/*
 *  ****  Queue Methods  ****
 *
 *	initTransfer				dts_hostInitTransfer
 *	endTransfer				dts_hostEndTransfer
 *
 *	queueValid				dts_hostQueueValid
 *	queueAccept				dts_hostQueueAccept
 *	queueComplete				dts_hostQueueComplete
 *	queueRelease				dts_hostQueueRelease
 */

/**
 *  DTS_HOSTINITTRANSFER -- See if a queue will accept a new entry.
 *
 *  @brief  See if a queue will accept a new entry.
 *  @fn     stat = dts_hostInitTransfer (char *host, char *qname, char *fname,
 *			char *msg)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	name of queue
 *  @param  fname	name of queue
 *  @param  msg		returned message/path
 *  @return		OK or ERR 
 */
int
dts_hostInitTransfer (char *host, char *qname, char *fname, char *msg)
{
    int client = dts_getClient (host), stat = OK;
    long  size = (long) 0;


    dts_cmdInit();			/* initialize static variables	*/

    if (dts->debug > 2) 
	dtsLog (dts, "{%s}: InitTransfer: %s:%s", qname, host, fname);

    if (fname == (char *) NULL) {
	strcpy (msg, "No filename specified.");
	dts_closeClient (client);
	return (ERR);
    } else
	size = dts_du (fname); 		/* get size (works for dirs too) */


    /* Make the service call.
    */
    xr_setStringInParam (client, qname);
    xr_setIntInParam (client, size);

    if (xr_callSync (client, "initTransfer") == OK) {
        char  *sres = calloc (1, SZ_PATH);

	/* If we got back a path, everything was OK, otherwise the string
	** is an error message.
	*/
        xr_getStringFromResult (client, &sres);
	stat = ((strncmp (sres, "Error", 5) == 0) ? ERR : OK);

        if (dts->debug > 1)
	    dtsLog (dts, "dts_hostInitTransfer: (%d) '%s'\n", stat, sres);

	strcpy (msg, sres);
	free ((char *) sres);

	dts_closeClient (client);
        return (stat);
    }

    if (dts->debug)
        dtsLog (dts, "{%s}: InitTransfer call failed: %s:%s", 
	    qname, host, fname);

    strcpy (msg, "initTransfer call failed");
    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTENDTRANSFER -- Terminate the file transfer.
 *
 *  @brief  Terminate the file transfer.
 *  @fn     stat = dts_hostEndTransfer (char *host, char *qname, char *qpath)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	name of queue
 *  @param  qpath	path to queue directory
 *  @return		OK or ERR 
 */
int
dts_hostEndTransfer (char *host, char *qname, char *qpath)
{
    int  client = dts_getClient (host), stat;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostEndTransfer: %s q=%s path=%s\n", 
	    host, qname, qpath);

    /*  Make the service call.
    */
    xr_setStringInParam (client, qname);
    xr_setStringInParam (client, qpath);

    /*  Impose an artificial delay.
     */
    if (queue_delay)
	sleep (queue_delay);
	
    if (xr_callSync (client, "endTransfer") == OK) {
        xr_getIntFromResult (client, &stat);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostEndTransfer: (%s)\n", 
		(stat == OK ? "OK" : "ERR") );
	    
	dts_closeClient (client);
        return (stat);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTQUEUEACCEPT -- See if a queue will accept a new entry.
 *
 *  @brief  See if a queue will accept a new entry.
 *  @fn     stat = dts_hostQueueAccept (char *host, char *qname, char *fname,
 *			char *msg)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	name of queue
 *  @param  fname	name of queue
 *  @param  msg		returned message/path
 *  @return		OK or ERR 
 */
int
dts_hostQueueAccept (char *host, char *qname, char *fname, char *msg)
{
    int client = dts_getClient (host), stat = OK;
    long  size = (long) 0;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostQueueAccept: %s:%s  queue=%s\n", 
	    host, fname, qname);

    if (fname == (char *) NULL) {
	strcpy (msg, "No filename specified.");
	dts_closeClient (client);
	return (ERR);
    } else
	size = dts_du (fname); 		/* get size (works for dirs too) */


    /* Make the service call.
    */
    xr_setStringInParam (client, qname);
    xr_setIntInParam (client, size);

    if (xr_callSync (client, "queueAccept") == OK) {
        char  *sres = calloc (1, SZ_PATH);

	/* If we got back a path, everything was OK, otherwise the string
	** is an error message.
	*/
        xr_getStringFromResult (client, &sres);
	stat = ((strncmp (sres, "Error", 5) == 0) ? ERR : OK);

        if (DEBUG) 
	    fprintf (stderr, "dts_hostQueueAccept: (%d) '%s'\n", stat, sres);

	strcpy (msg, sres);
	free ((char *) sres);
	dts_closeClient (client);
        return (stat);
    }

    strcpy (msg, "queueAccept call failed");
    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTQUEUECOMPLETE -- Complete a file transfer.
 *
 *  @brief  Complete a file transfer.
 *  @fn     stat = dts_hostQueueComplete (char *host, char *qname, char *qpath)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	name of queue
 *  @param  qpath	path to queue directory
 *  @return		OK or ERR 
 */
int
dts_hostQueueComplete (char *host, char *qname, char *qpath)
{
    int  client = dts_getClient (host), stat;

    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostQueueValid: %s  queue=%s\n", host, qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, qname);
    xr_setStringInParam (client, qpath);

    if (xr_callSync (client, "queueComplete") == OK) {
        xr_getIntFromResult (client, &stat);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostQueueComplete: (%s)\n", 
		(stat == OK ? "OK" : "ERR") );
	dts_closeClient (client);
        return (stat);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTQUEUERELEASE -- Release the lock semaphore on a queue.
 *
 *  @brief  Release the lock semaphore on a queue.
 *  @fn     stat = dts_hostQueueRelease (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	name of queue
 *  @return		OK or ERR 
 */
int
dts_hostQueueRelease (char *host, char *qname)
{
    int  client = dts_getClient (host), stat;

    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostQueueRelease: %s  queue=%s\n", host, qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, qname);

    if (xr_callSync (client, "queueRelease") == OK) {
        xr_getIntFromResult (client, &stat);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostQueueRelease: (%s)\n", 
		(stat == OK ? "OK" : "ERR") );
	dts_closeClient (client);
        return (stat);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTQUEUEVALID -- See if a queue name is valid on the host.
 *
 *  @brief  See if a queue name is valid on the host.
 *  @fn     stat = dts_hostQueueValid (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	name of queue
 *  @return		OK or ERR 
 */
int
dts_hostQueueValid (char *host, char *qname)
{
    int  client = dts_getClient (host), stat;

    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostQueueValid: %s  queue=%s\n", host, qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, qname);

    if (xr_callSync (client, "queueValid") == OK) {
        xr_getIntFromResult (client, &stat);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostQueueValid: (%s)\n", 
		(stat == OK ? "OK" : "ERR") );
	dts_closeClient (client);
        return (stat);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTSETQUEUECONTROL -- Set the control params for the transfer.
 *
 *  @brief  Set the control params for the transfer.
 *  @fn     stat = dts_hostSetQueueControl (char *host, char *qPath,
 *			Control *ctrl)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qPath	path to queue directory being used
 *  @param  ctrl	Control data struct
 *  @return		OK or ERR 
 */
int
dts_hostSetQueueControl (char *host, char *qPath, Control *ctrl)
{
    int  client = dts_getClient (host), stat = OK;
    int  i, anum, snum[MAXPARAMS];


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostSetQueueControl: %s qPath=%s\n", host, qPath);

    /* Set the call parameters.
    */
    xr_initParam (client);
    xr_setStringInParam (client, qPath);
    xr_setStringInParam (client, ctrl->queueHost);
    xr_setStringInParam (client, ctrl->queueName);
    xr_setStringInParam (client, ctrl->filename);
    xr_setStringInParam (client, ctrl->xferName);
    xr_setStringInParam (client, ctrl->srcPath);
    xr_setStringInParam (client, ctrl->igstPath);
    xr_setStringInParam (client, ctrl->md5);
    xr_setStringInParam (client, ctrl->deliveryName);
    xr_setIntInParam (client, ctrl->isDir);
    xr_setIntInParam (client, ctrl->fsize);
    xr_setIntInParam (client, ctrl->sum32);
    xr_setIntInParam (client, ctrl->crc32);
    xr_setIntInParam (client, ctrl->epoch);

    anum = xr_newArray ();
    for (i=0; i < ctrl->nparams; i++) {
	snum[i] = xr_newStruct();
	xr_setStringInStruct (snum[i], "p", ctrl->params[i].name);
	xr_setStringInStruct (snum[i], "v", ctrl->params[i].value);

	xr_setStructInArray (anum, snum[i]);
    }
    xr_setArrayInParam (client, anum);


    /* Make the service call.
    */
    if (xr_callSync (client, "queueSetControl") == OK) {
        xr_getIntFromResult (client, &stat);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostSetQueueControl: (%s)\n", 
		(stat == OK ? "OK" : "ERR") );
    } else 
	stat = ERR;
	    

    for (i=0; i < ctrl->nparams; i++)
	xr_freeStruct (snum[i]);
    xr_freeArray (anum);

    dts_closeClient (client);
    return (stat);
}
