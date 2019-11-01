/**
 *  DTSINGEST.C -- DTS procedures to "ingest" a transfer object.
 *
 *  @file  	dtsIngest.c
 *  @author  	Mike Fitzpatrick, NOAO
 *  @date  	6/10/09
 *
 *  @brief  DTS procedures to "ingest" a transfer object.
 */


/*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <ctype.h>
#include <stdarg.h>
#include <sys/param.h>
#include <sys/wait.h>

#include "dts.h"
#include "dtsdb.h"


extern  DTS  *dts;
extern  int   dts_monitor;

#define	DEBUG		(dts&&dts->debug)
#define	VDEBUG		(dts&&dts->debug>2)


/**
 *  DTS_INGEST -- Ingest a file for transport.
 *
 *  @brief	Ingest a file for transport.
 *  @fn		char *dts_Ingest (dtsQueue *dtsq, Control *ctrl, 
 *		    char *fpath, char *cpath, int *stat)
 *
 *  @param  dtsq	DTS queue structure
 *  @param  ctrl	Transfer control structure
 *  @param  fpath	path to the file in spool dir
 *  @param  cpath	path to the control file in spool dir
 *  @param  stat	status of the ingest application
 *  @returns		NULL or error message
 */
char *
dts_Ingest (dtsQueue *dtsq, Control *ctrl, char *fpath, char *cpath, int *stat)
{
    char  emsg[SZ_LINE], fname[SZ_PATH], qpath[SZ_PATH], lockfile[SZ_PATH];
    char  newpath[SZ_PATH], msg[SZ_PATH], pfname[SZ_PATH];
    char  *cmd=NULL, *md5=NULL;
    int   status = OK;
#ifdef USE_DTS_DB
    int   key = 0;
#endif
#ifdef DTS_INGEST_LOCK
    int   ifd = 0;
#endif


    /*  Sanity checks.
     */
    if (! dtsq) {
        fprintf (stderr, "Error: dts_Ingest gets NULL 'dtsq'\n");
        exit (1);
    } else
	dts_qstatDlvrStart (dtsq->name);


    memset (msg, 0, SZ_PATH);
    memset (fname, 0, SZ_PATH);
    memset (qpath, 0, SZ_PATH);	    
    memset (pfname, 0, SZ_PATH);	    
    memset (newpath, 0, SZ_PATH);	    
    memset (lockfile, 0, SZ_PATH);

    sprintf (fname, "%s:%s/%s", ctrl->queueHost, ctrl->srcPath, ctrl->filename);
    sprintf (qpath, "%s/%s", dts->serverRoot, ctrl->queuePath);

    md5 = dts_fileMD5 (fpath);

    /*  Create a lock file.  We use this rather than the status so
     *  we can use its existence rather than read the contents of
     *  _status.
     */
#ifdef DTS_INGEST_LOCK
    sprintf (lockfile, "%s/_lock", qpath);
    if ((ifd = creat (lockfile, DTS_FILE_MODE)) < 0)
         dtsLog (dts, "Warning: Can't creat lockfile '%s'\n", lockfile);
    close (ifd);
#endif


    /*  First step:  Execute the ingest command on the file.  In this
     *  this case we don't need to copy the file to the delivery directory
     *  and assume the ingest app can re-write the file.
     *
     *  The first step is to format the ingest command given the template
     *  argument we got from the config file, then execute the command.
     */
    if (dtsq->deliveryCmd[0]) {
        char  old[SZ_PATH], new[SZ_PATH], rejFile[SZ_PATH];
	int   rfd = -1;


	memset (dtsq->deliveryDir, 0, SZ_PATH);
	sprintf (dtsq->deliveryDir, "%s/%s", dts->serverRoot, ctrl->queuePath);
        cmd = dts_fmtQueueCmd (dtsq, ctrl);
        if (VDEBUG)
	    fprintf (stderr, "Ingest: sysExec cmd = '%s'\n", cmd);

	*stat = OK;
        if ((status = dts_sysExec (dtsq->deliveryDir, cmd)) != OK) {
	    /*  An error in executing the command.
	    */
	    *stat = status;
	    dtsErrLog (dtsq, "ingest error on '%s', status=%d", fname, status);

	    switch (status) {
	    case -1: 	break;		/* Command not found	   	*/
	    case  0: 	break;		/* OK			   	*/
	    case  1: 	break;		/* Minor error, keep going 	*/
	    case  2:			/* Fatal error, reject file 	*/
		memset (rejFile, 0, SZ_PATH);
		sprintf (rejFile, "%s%s/ERR", dts->serverRoot, ctrl->queuePath);
		if ((rfd = creat (rejFile, DTS_FILE_MODE)))
		    close (rfd);
	        goto ingest_err_;
	    case  3:			/* Fatal error, halt queue 	*/
		dts_semSetVal (dtsq->activeSem, QUEUE_PAUSED);
	        sprintf (emsg, "ingest error, halting queue %s", dtsq->name);
	        goto ingest_err_;
	    default:
	        sprintf (emsg, "Unknown ingest cmd error, status=%d", status);
	        goto ingest_err_;
	    }
        }
	dtsLog (dts, "ingest cmd file=%s, status=%d", fname, status);


        /*  See if the deliveryApp left behind a parameter file we need to
         *  append to the control file.  This can include a directive that
         *  tells us what the delivered filename should be or parameters that
         *  may be required by downstream delivery apps.
         */
        sprintf (pfname, "%s/%s.par", dtsq->deliveryDir, dtsq->name);
        if (access (pfname, R_OK) == 0) {
    	    if (IGST_DEBUG)
		fprintf (stderr, "IGST: new par file=%s\n", pfname);
            dts_loadDeliveryParams (ctrl, pfname);
#ifdef DTS_REMOVE_INGEST_PARFILE
            unlink (pfname);               	/* remove the parfile    */
#endif
        } else
	    strcpy (ctrl->deliveryName, ctrl->filename);

	/*  Ingest script may have renamed the file so recompute the
	 *  needed paths.
	 */
	strcpy (newpath, fpath);


	/*  Rename the file if the delivery script requests it by creating
	 *  a 'deliveryName' param.
	 */
	memset (new, 0, SZ_PATH);
	sprintf (new, "%s%s%s", dts->serverRoot, ctrl->queuePath, 
		ctrl->deliveryName);
	memset (old, 0, SZ_PATH);
	sprintf (old, "%s%s%s", dts->serverRoot, ctrl->queuePath, 
		ctrl->filename);

	if (access (new, F_OK) == 0) {
	    /*  New file exists, script must have done it.
	     */
	    char *ip = new;
	    int len = strlen (new);

	    for (ip = &new[len-1]; *ip && *ip != '/'; ip--)
		;
	    strcpy (ctrl->xferName, ip);
	
	} else {
	    /*  Only the name change was requested, we do the rename ourselves.
	     */
	    char *ip = new;
	    int len = strlen (new);

	    if (access (ctrl->filename, F_OK) < 0 && ctrl->deliveryName[0]) {
	        if (dts->verbose > 1 || IGST_DEBUG)
	            dtsLog (dts, "IGST: renaming: '%s' -> '%s'", old, new);
	        if (rename (new, old) < 0)
	            dtsLog (dts, "IGST ERR: can't rename: %s", strerror(errno));
	    }

	    for (ip = &new[len-1]; *ip && *ip != '/'; ip--)
		;
	    strcpy (ctrl->xferName, ip);
	}
	strcpy (newpath, new);


	/*  If the ingest script left behind a symlink, read through the
	 *  link to the actual file.
	 */
	if (dts_isLink (newpath)) {
	    char *new = dts_readLink (newpath);
	    memset (fpath, 0, strlen (fpath));
	    strcpy (fpath, new);
	    strcpy (newpath, fpath);
	    free (new);
	}


	/*  Recompute the transfer checksums and file info.
	 */
	ctrl->fsize = (long) dts_nameSize (newpath);
	ctrl->fmode = (mode_t) dts_nameMode (newpath);

	if (ctrl->isDir) {
	    ctrl->sum32 = ctrl->crc32 = 0;
 	    strcpy (ctrl->md5, " \0");
	} else {
	    ctrl->sum32 = dts_fileCRCChecksum (newpath, &ctrl->crc32);
            md5 = dts_fileMD5 (newpath);
 	    strcpy (ctrl->md5, md5);
	}
    }

    /*  Re-write the control file.  Since the file may be changed, we
     *  need to recompute the transfer checksums and file info.
     */
    if (dts_saveControl (ctrl, cpath) != OK) {
	sprintf (emsg, "ingest control save error on '%s'", cpath); 
	goto ingest_err_;
    }


    /* Last step is to append a history record of the delivery to the
     * control file.
     */
    (void) dts_addControlHistory (ctrl, NULL);
#ifdef USE_DTS_DB
    key = dts_dbNewEntry (dtsq->name, dtsq->src, dtsq->dest, 
	ctrl->igstPath, qpath, (long) ctrl->fsize, md5);
#endif

    if (IGST_DEBUG)
	dtsLog (dts, "ingest cmd file=%s, status=%d", fname, status);
    /*
    if (dts->verbose) {
	sprintf (msg, "IGST: status=%d file=%s\n", status, fname);
	dtsLogMsg (dts, 1, msg);
    }
    */
	
    if (md5) 
	free ((char *) md5);

    dts_qstatDlvrStat (dtsq->name, OK);
    dts_qstatDlvrEnd (dtsq->name);

    return ((char *) NULL);


ingest_err_:
    (void) dts_addControlHistory (ctrl, emsg);
#ifdef USE_DTS_DB
    key = dts_dbNewEntry (dtsq->name, dtsq->src, dtsq->dest, 
	ctrl->igstPath, qpath, (long) ctrl->fsize, md5);
    dts_dbAddMsg (key, emsg);
#endif

    if (IGST_DEBUG)
	fprintf (stderr, "IGST: status=%d file=%s\n", status, fname);
    /*
    if (dts->verbose) {
	sprintf (msg, "IGST: status=%d file=%s\n", status, fname);
	dtsLogMsg (dts, key, msg);
    }
    */

    if (md5) 
	free ((char *) md5);

    dts_qstatDlvrStat (dtsq->name, ERR);
    dts_qstatDlvrEnd (dtsq->name);

    return (dts_strbuf(emsg));
}
