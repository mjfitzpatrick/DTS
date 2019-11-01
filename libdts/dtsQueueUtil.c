/**
 *  DTSQUEUEUTIL.C -- DTS queue utilities.
 *
 *  @file  	dtsQueueUtil.c
 *  @author  	Mike Fitzpatrick, NOAO
 *  @date  	6/10/09
 *
 *  @brief  DTS queue utilities.  
 */


/*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <ctype.h>
#include <stdarg.h>
#include <sys/file.h>

#include "dtsPSock.h"
#include "dts.h"


extern  DTS  *dts;
extern  int   dts_monitor;

#define	DEBUG		(dts&&dts->debug)

static char *intstr (int val);
static int   strsub (char *in, char *from, char *to, char *outstr, int maxch);



/**
 *  DTS_QUEUELOOKUP -- Lookup a DTS queue struct by name.
 *
 *  @brief	Lookup a DTS queue struct by name.
 *  @fn		dtsQueue *dts_queueLookup (char *qname)
 *
 *  @param  qname	name of the queue
 *  @return		dtsQueue struct corresponding to qname
 */
dtsQueue *
dts_queueLookup (char *qname)
{
    register int i;
    char  name[SZ_PATH], *ip;


    memset (name, 0, SZ_PATH);		/* strip trailing whitespace	*/
    strcpy (name, qname);
    for (ip=&name[0]; *ip && *ip != ' '; )
	ip++;
    *ip = '\0';

    /* Check that this is a valid queue name.
    */
    if (dts->nqueues > 0) {
	for (i=0; i < dts->nqueues; i++) {
	    if (strcmp (qname, dts->queues[i]->name) == 0)
		return (dts->queues[i]);
	}
    }

    return ((dtsQueue *) NULL);
}


/**
 *  DTS_GETQUEUEPATH -- Get queue path.
 *
 *  @brief	Get the path to the spool dir for a given queue.
 *  @fn		char *dts_getQueuePath (DTS *dts, char *qname)
 *
 *  @param  dts		DTS struct pointer
 *  @param  qname	name of the queue
 *  @return		path to queue directory
 */
char *
dts_getQueuePath (DTS *dts, char *qname)
{
    char  *dir, *ret;

    dir = calloc (1, SZ_PATH);
    sprintf (dir, "%s/spool/%s/", dts->serverRoot, qname);

    ret = dts_strbuf (dir);
    free ((void *) dir);
    return ( ret );
}


/**
 *  DTS_GETQUEUELOG -- Get the path to the queue logfile.
 *
 *  @brief	Get the path to the queue logfile.
 *  @fn		char *dts_getQueueInLog (DTS *dts, char *qname)
 *
 *  @param  dts		DTS struct pointer
 *  @param  qname	name of the queue
 *  @return		path to queue log file
 */
char *
dts_getQueueLog (DTS *dts, char *qname, char *logname)
{
    char  *dir, *ret;

    dir = calloc (1, SZ_PATH);
    sprintf (dir, "%s/spool/%s/%s", dts->serverRoot, qname, logname);

    ret = dts_strbuf (dir);
    free ((void *) dir);
    return ( ret );
}


/**
 *  DTS_GETNEXTQUEUEDIR -- Get the next queue directory name.
 *
 *  @brief	Get  the next queue directory name.
 *  @fn		char *dts_getNextQueueDir (DTS *dts, char *qname)
 *
 *  @param  dts		DTS struct pointer
 *  @param  qname	name of the queue
 *  @return		path to next working spool dir
 */

#ifdef USE_NEW_NEXT

char *
dts_getNextQueueDir (DTS *dts, char *qname)
{
    FILE  *fd;
    char  *dir, *statfile, *lockfile, *next, *ret;
    int    nval = 0, ifd = 0;
    dtsQueue *dtsq = (dtsQueue *) NULL;


    dir = calloc (1, SZ_PATH);
    next = calloc (1, SZ_PATH);
    statfile = calloc (1, SZ_PATH);
    lockfile = calloc (1, SZ_PATH);

    sprintf (dir, "%s/spool/%s/", dts->serverRoot, qname);
    sprintf (next, "%s/spool/%s/next", dts->serverRoot, qname);

    dtsq = dts_queueLookup (qname);
#ifdef USE_MUTEX
    pthread_mutex_lock (&dtsq->mutex);
#endif


     /*  Get the next available queue dir.
      */
    if (access (next, R_OK) == 0)
	nval = dts_queueGetNext (next);


    /* Now create the directory and initialize the status.
    */
    sprintf (dir, "spool/%s/%d/", qname, nval);
    if (access (dir, F_OK) < 0) { 
	char *dirp = dts_sandboxPath (dir);

	mkdir (dirp, DTS_DIR_MODE);

	sprintf (statfile, "%s/_status", dirp);
	if ((fd = dts_fopen (statfile, "w+")) == (FILE *) NULL) {
	    dtsLog (dts, "Warning: Can't open statfile '%s'\n", statfile);
	    free ((void *) dirp);
	    return (dir);
	}
	fprintf (fd, "ready\n");
	dts_fclose (fd);

	/*  Create a lock file.  We use this rather than the status so
	 *  we can use its existence rather than read the contents of _status.
	 */
	sprintf (lockfile, "%s/_lock", dirp);
	if ((ifd = creat (lockfile, DTS_FILE_MODE)) < 0)
	    dtsLog (dts, "Warning: Can't creat lockfile '%s'\n", lockfile);
	close (ifd);

	free ((void *) dirp);
    } else
	dtsLog (dts, "Warning: spool directory %s already exists", dir);


     /*  Update the queue 'next' value.
      */
    if (access (next, R_OK) == 0)
	dts_queueSetNext (next, (nval + 1));
    else
	dts_queueSetNext (next, 0);

#ifdef USE_MUTEX
    pthread_mutex_unlock (&dtsq->mutex);
#endif

    ret = dts_strbuf (dir);
    free ((void *) dir);
    free ((void *) next);
    free ((void *) statfile);
    free ((void *) lockfile);

    return ( ret );
}

#else

char *
dts_getNextQueueDir (DTS *dts, char *qname)
{
    char   dir[SZ_PATH], statfile[SZ_PATH], lockfile[SZ_PATH];
    char   next[SZ_PATH], line[SZ_LINE];
    int    nval = 0, ifd = 0;
    FILE  *fd;
    dtsQueue *dtsq = (dtsQueue *) NULL;


    memset (dir,  0, SZ_PATH);
    memset (next, 0, SZ_PATH);
    memset (line, 0, SZ_LINE);
    memset (statfile, 0, SZ_PATH);
    memset (lockfile, 0, SZ_PATH);

    sprintf (dir, "%s/spool/%s/", dts->serverRoot, qname);
    sprintf (next, "%s/spool/%s/next", dts->serverRoot, qname);

    dtsq = dts_queueLookup (qname);
    pthread_mutex_lock (&dtsq->mutex);

    if (access (next, R_OK) == 0) {
	if ((fd = fopen (next, "r+")) == (FILE *) NULL)
	    return (NULL);
	flock (fileno(fd), LOCK_EX);
	fgets (line, SZ_LINE, fd);
 	nval = atoi (line);

	rewind (fd);			/* increment dir number */
	fprintf (fd, "%d\n", (nval+1) );
	fflush (fd);
	flock (fileno(fd), LOCK_UN);
	fclose (fd);

    } else {
	if ((fd = fopen (next, "w+")) == (FILE *) NULL)
	    return (NULL);
	flock (fileno(fd), LOCK_EX);
	fprintf (fd, "0\n");
	fflush (fd);
	flock (fileno(fd), LOCK_UN);
	fclose (fd);
    }

    sprintf (dir, "spool/%s/%d/", qname, nval);


    /* Now create the directory and initialize the status.
    */
    if (access (dir, F_OK) < 0) { 
	char *dirp = dts_sandboxPath (dir);

	mkdir (dirp, DTS_DIR_MODE);

	sprintf (statfile, "%s/_status", dirp);
	if ((fd = fopen (statfile, "w+")) == (FILE *) NULL) {
	    dtsLog (dts, "Warning: Can't open statfile '%s'\n", statfile);
	    free ((void *) dirp);
	    return ( dts_strbuf (dir) );
	}
	flock (fileno(fd), LOCK_EX);
	fprintf (fd, "ready\n");
	fflush (fd);
	flock (fileno(fd), LOCK_UN);
	fclose (fd);

	/*  Create a lock file.  We use this rather than the status so
	 *  we can use its existence rather than read the contents of _status.
	 */
	sprintf (lockfile, "%s/_lock", dirp);
	if ((ifd = creat (lockfile, DTS_FILE_MODE)) < 0)
	    dtsLog (dts, "Warning: Can't creat lockfile '%s'\n", lockfile);
	close (ifd);
	free ((void *) dirp);

    } else
	dtsLog (dts, "Warning: spool directory %s already exists", dir);

    pthread_mutex_unlock (&dtsq->mutex);

    return ( dts_strbuf (dir) );
}
#endif



/**
 *  DTS_QUEUEGETCURRENT -- Get the number of the currrently active spool.
 *
 *  @brief	Get the number of the currrently active spool.
 *  @fn		int dts_queueGetCurrent (char *fname)
 *
 *  @param  fname	path to 'current' file
 *  @return		queue current value
 */
int
dts_queueGetCurrent (char *fname)
{
    FILE *fd = (FILE *) NULL;
    int  val;

    if ((fd = dts_fopen (fname, "r")) == (FILE *) NULL)
        return (-1);
    fscanf (fd, "%d", &val);
    dts_fclose (fd);

    return (val);
}


/**
 *  DTS_QUEUESETCURRENT -- Set the number of the currrently active spool.
 *
 *  @brief	Set the number of the currrently active spool.
 *  @fn		int dts_queueSetCurrent (char *fname, int val)
 *
 *  @param  fname	path to 'current' file
 *  @param  val		value to set
 *  @return		nothing
 */
void
dts_queueSetCurrent (char *fname, int val)
{
    FILE *fd = (FILE *) NULL;

    if ((fd = dts_fopen (fname, "w")) == (FILE *) NULL)
        return;
    flock (fileno(fd), LOCK_EX);
    fprintf (fd, "%d\n", val);
    fflush (fd);
    flock (fileno(fd), LOCK_UN);
    dts_fclose (fd);
}


/**
 *  DTS_QUEUEGETNEXT -- Get the number of the next active spool.
 *
 *  @brief	Get the number of the next active spool.
 *  @fn		int dts_queueGetNext (char *fname)
 *
 *  @param  fname	path to 'next' file
 *  @return		queue next value
 */
int
dts_queueGetNext (char *fname)
{
    FILE *fd = (FILE *) NULL;
    int  val = -1;

    if ((fd = dts_fopen (fname, "r")) == (FILE *) NULL)
        return (-1);
    fscanf (fd, "%d", &val);
    dts_fclose (fd);

    return (val);
}


/**
 *  DTS_QUEUESETNEXT -- Set the number of the next active spool.
 *
 *  @brief	Set the number of the next active spool.
 *  @fn		void dts_queueSetNext (char *fname, int val)
 *
 *  @param  fname	path to 'Next' file
 *  @param  val		value to set
 *  @return		nothing
 */
void
dts_queueSetNext (char *fname, int val)
{
    FILE *fd = (FILE *) NULL;

    if ((fd = dts_fopen (fname, "w")) == (FILE *) NULL)
        return;
    fprintf (fd, "%d\n", val);
    dts_fclose (fd);
}


/**
 *  DTS_QUEUEGETINITTIME -- Get the time of initial file transfer.
 *
 *  @brief	Get the time of initial file transfer.
 *  @fn		void dts_queueGetInitTime (char *fname)
 *
 *  @param  fname	path to 'next' file
 *  @return		queue next value
 */
void
dts_queueGetInitTime (dtsQueue *dtsq, int *sec, int *usec)
{
    char fname[SZ_PATH];
    FILE *fd = (FILE *) NULL;

    memset (fname, 0, SZ_PATH);
    sprintf (fname, "%s/_init_time", dts_getQueuePath (dts, dtsq->name));

    if ((fd = dts_fopen (fname, "r")) == (FILE *) NULL)
        return;
    fscanf (fd, "%d %d", sec, usec);
    dts_fclose (fd);
    unlink (fname);
}


/**
 *  DTS_QUEUESETINITTIME -- Set the time of initial file transfer.
 *
 *  @brief	Set the time of initial file transfer.
 *  @fn		void dts_queueSetInitTime (char *fname, int val)
 *
 *  @param  fname	path to 'Next' file
 *  @param  val		value to set
 *  @return		nothing
 */
void
dts_queueSetInitTime (dtsQueue *dtsq, struct timeval t)
{
    char fname[SZ_PATH];
    FILE *fd = (FILE *) NULL;

    memset (fname, 0, SZ_PATH);
    sprintf (fname, "%s/_init_time", dts_getQueuePath (dts, dtsq->name));

    if ((fd = dts_fopen (fname, "w+")) == (FILE *) NULL)
        return;
    fprintf (fd, "%ld %ld\n", (long)t.tv_sec, (long)t.tv_usec);
    dts_fclose (fd);
}


/**
 *  DTS_QUEUECLEANUP -- Clean up the spool directory before processing.
 *
 *  @brief	Clean up the spool directory before processing
 *  @fn		void dts_queueCleanup (DTS *dts, dtsQueue *dtsq)
 *
 *  @param  dts		DTS structure
 *  @param  dtsq	DTS queue structure
 *  @return		nothing
 */
void
dts_queueCleanup (DTS *dts, dtsQueue *dtsq)
{
    char  curfil[SZ_PATH], nextfil[SZ_PATH];
    int   current, next;


    /*  FIXME -- Not yet implemented.	*/

    /* Get the image currently being processed.
     */
    memset (curfil, 0, SZ_PATH);
    memset (nextfil, 0, SZ_PATH);

    sprintf (curfil, "%s/spool/%s/current", dts->serverRoot, dtsq->name);
    sprintf (nextfil, "%s/spool/%s/next", dts->serverRoot, dtsq->name);

    current = dts_queueGetCurrent (curfil);
    next = dts_queueGetNext (nextfil);

    if (next == -1)
        dtsLog (dts, "QCleanup INVALID GETNEXT, returned -1\n");
 
    if (dts->verbose > 2)
        dtsLog (dts, "QCleanup[%s]: cur=%d next=%d\n", dtsq->name, 
	    current, next);

    /*
    if (current == next && /path/current doesn't exist) {
	queue is in a normal state
	return
    }
    */
}


/**
 *  DTS_LOADCONTROL -- Load the control file into a struct.
 *
 *  @brief	Load the control file into a struct.
 *  @fn		Control *dts_loadControl (char *path, Control *ctrl)
 *
 *  @param  path	path to control file
 *  @param  ctrl	pointer to Control struct
 *  @return		loaded Control structure
 */
Control *
dts_loadControl (char *path, Control *ctrl)
{
    char      param[SZ_LINE], val[SZ_LINE], line[SZ_LINE], *ip, *op;
    FILE     *fd;
    int       epoch;


    memset (val, 0, SZ_LINE);
    memset (ctrl, 0, sizeof (Control));

    /*  Error checking.
     */
    if (access (path, R_OK) < 0) {
	dtsLog (dts, "ERROR: cannot access control file '%s'\n", path);
    	return ((Control *) NULL);
    }

    if ((fd = dts_fopen (path, "r")) != (FILE *) NULL) {

	/*  Error checking ...
	 */
	if (dts_fileSize (fileno(fd)) == 0) {
	    dtsLog (dts, "ERROR: zero-length control file '%s'\n", path);
	    dts_fclose (fd);
    	    return ((Control *) NULL);
	}

        while (dtsGets (line, SZ_LINE, fd)) {
            if (strncmp (line, "qname", 5) == 0) {
                sscanf (line, "fname        = %s", &ctrl->queueName[0]);
            } else if (strncmp (line, "qhost", 5) == 0) {
                sscanf (line, "qhost        = %s", &ctrl->queueHost[0]);
            } else if (strncmp (line, "qpath", 5) == 0) {
                sscanf (line, "qpath        = %s", &ctrl->queuePath[0]);
            } else if (strncmp (line, "fname", 5) == 0) {
                sscanf (line, "fname        = %s", &ctrl->filename[0]);
            } else if (strncmp (line, "xfername", 5) == 0) {
                sscanf (line, "xfername     = %s", &ctrl->xferName[0]);
            } else if (strncmp (line, "srcpath", 7) == 0) {
                sscanf (line, "srcpath      = %s", &ctrl->srcPath[0]);
            } else if (strncmp (line, "path", 4) == 0) {
                sscanf (line, "path         = %s", &ctrl->igstPath[0]);
            } else if (strncmp (line, "fsize", 5) == 0) {
                sscanf (line, "fsize        = %ld", &ctrl->fsize);
            } else if (strncmp (line, "sum32", 5) == 0) {
                sscanf (line, "sum32        = %u", &ctrl->sum32);
            } else if (strncmp (line, "crc32", 5) == 0) {
                sscanf (line, "crc32        = %u", &ctrl->crc32);
            } else if (strncmp (line, "md5", 3) == 0) {
                sscanf (line, "md5          = %s", &ctrl->md5[0]);
            } else if (strncmp (line, "isDir", 5) == 0) {
                sscanf (line, "isDir        = %d", &ctrl->isDir);
            } else if (strncmp (line, "epoch", 5) == 0) {
                sscanf (line, "epoch        = %d", (int *)&epoch);
		ctrl->epoch = (time_t) epoch;
            } else if (strncmp (line, "deliveryName", 12) == 0) {
                sscanf (line, "deliveryName = %s", &ctrl->deliveryName[0]);
            } else {
		/* Arbitrary parameter.
		 */
                memset (val, 0, SZ_LINE);
                memset (param, 0, SZ_LINE);

		for (ip=line, op=param; !isspace(*ip); ip++) 
		    *op++ = *ip;
		while (isspace (*ip) || *ip == '=') 	/* whitespace */
		    ip++;
		for (op=val; *ip && *ip != '\n'; ip++) 
		    *op++ = *ip;

		strcpy (ctrl->params[ctrl->nparams].name, param);
		strcpy (ctrl->params[ctrl->nparams].value, val);
		ctrl->nparams++;
            }
        }
        dts_fclose (fd);
    
	return (ctrl);
    }

    return ((Control *) NULL);
}


/**
 *  DTS_SAVECONTROL -- Save the control struct.
 *
 *  @brief	Save the control struct
 *  @fn		int dts_saveControl (Control *ctrl, char *cpath)
 *
 *  @param  ctrl	control structure
 *  @param  cpath	path to control file
 *  @returns		OK or ERR code
 */
int
dts_saveControl (Control *ctrl, char *path)
{
    FILE  *fd;

    if ((fd = dts_fopen (path, "w+"))) {
        flock (fileno(fd), LOCK_EX);
	dts_printControl (ctrl, fd);
        flock (fileno(fd), LOCK_UN);
	dts_fclose (fd);
	return (OK);
    }

    return (ERR);
}


/**
 *  DTS_LOGCONTROL -- Make a log entry of the Control structure.
 *
 *  @brief	Make a log entry of the Control structure.
 *  @fn		Control *dts_logControl (Control *ctrl, int stat, char *path)
 *
 *  @param  ctrl	control structure
 *  @param  stat	status flag
 *  @param  path	path to control file
 *  @returns		OK or ERR code
 */
int
dts_logControl (Control *ctrl, int stat, char *path)
{
    FILE  *fd;

    if ((fd = dts_fopen (path, "a+")) == (FILE *) NULL)
	return (ERR);

    flock (fileno(fd), LOCK_EX); 	/* get exclusive lock file	*/

    /*  Write the log entry.
     */
    fprintf (fd, "%s %ld - %s - %s %s %s %ld\n",	
	dts_logtime(), (long) time ((time_t) 0),
	(stat ? "ERR" : "OK "),
	ctrl->filename, ctrl->xferName,
	ctrl->md5, ctrl->fsize);

    flock (fileno(fd), LOCK_UN); 	/* clean up, release lock file	*/
    dts_fclose (fd);

    return (OK);
}


/**
 *  DTS_PRINTCONTROL -- Print the control struct.
 *
 *  @brief	Print the control struct
 *  @fn		Control *dts_printControl (Control *ctrl, FILE *fd)
 *
 *  @param  cpath	path to control file
 *  @param  fd		file descriptor to which to write 
 *  @returns		nothing
 */
void
dts_printControl (Control *ctrl, FILE *fd)
{
    register int i;

    fprintf (fd, "qname        = %s\n",  ctrl->queueName);
    fprintf (fd, "qpath        = %s\n",  ctrl->queuePath);
    fprintf (fd, "qhost        = %s\n",  ctrl->queueHost);
    fprintf (fd, "fname        = %s\n",  ctrl->filename);
    fprintf (fd, "xfername     = %s\n",  ctrl->xferName);
    fprintf (fd, "srcpath      = %s\n",  ctrl->srcPath);
    fprintf (fd, "path         = %s\n",  ctrl->igstPath);
    fprintf (fd, "fsize        = %ld\n", ctrl->fsize);
    fprintf (fd, "sum32        = %u\n",  ctrl->sum32);
    fprintf (fd, "crc32        = %u\n",  ctrl->crc32);
    fprintf (fd, "md5          = %s\n",  ctrl->md5);
    fprintf (fd, "isDir        = %d\n",  ctrl->isDir);
    fprintf (fd, "epoch        = %d\n",  (int)ctrl->epoch);
    fprintf (fd, "deliveryName = %s\n",  ctrl->deliveryName);

    for (i=0; i < ctrl->nparams; i++) {
        fprintf (fd, "%-12.12s = %s\n", 
	    ctrl->params[i].name, ctrl->params[i].value);
    }
}


/**
 *  DTS_FMTQUEUECMD -- Format a queue delivery command.
 *
 *  @brief	Format a queue delivery command.
 *  @fn		char *dts_fmtQueueCmd (dtsQueue *dtsq, Control *ctrl)
 *
 *  @param  dtsq	DTS queue structure
 *  @param  ctrl	transfer control parameters
 *  @returns		formatted command string
 */
char *
dts_fmtQueueCmd (dtsQueue *dtsq, Control *ctrl)
{
    register int i;
    char out[SZ_CMD], param[SZ_LINE], ofpath[SZ_PATH];
    char delpath[SZ_PATH], cmd[SZ_CMD], srcpath[SZ_PATH];
    char  *ip = NULL, *op = NULL;


    /* We have some macros, rather than check or each macro just
     * try to replace them all.
     */
    memset (cmd,     0, SZ_CMD);
    memset (out,     0, SZ_CMD);
    memset (param,   0, SZ_LINE);
    memset (ofpath,  0, SZ_PATH);
    memset (delpath, 0, SZ_PATH);
    memset (srcpath, 0, SZ_PATH);

    pthread_mutex_lock (&dtsq->mutex);

    if (strchr (dtsq->deliveryCmd, (int) '$') == (char *) NULL) {
	/* No macros in the command, just return the input string.
	 */
        strcpy (cmd, dtsq->deliveryCmd);
	return ( dts_strbuf (cmd) );
    }

    strcpy (cmd, dtsq->deliveryCmd);
    strcpy (delpath, dts_getDeliveryPath (dtsq, ctrl));
    sprintf (ofpath, "%s/%s", dtsq->deliveryDir, ctrl->filename);

    for (ip=ctrl->igstPath; *ip; ) 
	if (*ip++ == '!')
	    break;
    strcpy (srcpath, ip);
    for (op=&srcpath[strlen(ip)]; *op != '/' && op >= ip; op--)
	*op = '\0';

    /*  Macro substitutions.
     *
     *	$F	    path to delivered file (original filename)
     *	$D	    path to delivered file (delivered filename)
     *	$S	    object size (bytes)
     *	$Q	    name of delivery queue
     *	$QP	    DTS-relative path to queue's spool directory
     *	$E	    UT epoch at the time the object was queued
     *	$SUM32	    32-bit SYSV checksum
     *	$CRC32	    32-bit CRC checksum
     *	$MD5	    MD5 checksum
     *	$FULL	    full path to originating file
     *	$ON   	    original filename (minus path)
     *	$OP	    original path to filename
     *	$DN   	    delivery filename (minus path)
     *	$DP	    delivery path
     *	$SP	    path to filename on src host
     *	$OH	    originating host name
     *
     *	$<param>    replaced with value of named param (or '')
     */

    /* Replace arbitrary parameters.  We do this first to avoid conflicts
     * with the builtin replacement strings.
     */
    op = out;
    for (i=0; i < ctrl->nparams; i++) {
	memset (param, 0, SZ_LINE);
	sprintf (param, "$%s", ctrl->params[i].name);
        (void) strsub (cmd, param, ctrl->params[i].value, op, SZ_CMD);
        (void) strcpy (cmd, out); op = out;
    }

    (void) strsub (cmd, "$FULL", ctrl->igstPath, op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$SUM32", intstr(ctrl->sum32), op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$CRC32", intstr(ctrl->crc32), op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$MD5", ctrl->md5, op, SZ_CMD); 	
    (void) strcpy (cmd, out); op = out;

    (void) strsub (cmd, "$F", ofpath, op, SZ_CMD); 	
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$D", delpath, op, SZ_CMD); 	
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$QP", ctrl->queuePath, op, SZ_CMD); 	
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$Q", ctrl->queueName, op, SZ_CMD); 	
    (void) strcpy (cmd, out); op = out;
    
    (void) strsub (cmd, "$ON", ofpath, op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$OP", srcpath, op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;

    (void) strsub (cmd, "$DN", delpath, op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$DP", dtsq->deliveryDir, op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;

    (void) strsub (cmd, "$SP", ctrl->srcPath, op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$OH", ctrl->queueHost, op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;

    (void) strsub (cmd, "$S", intstr(ctrl->fsize), op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;
    (void) strsub (cmd, "$E", intstr((int)ctrl->epoch), op, SZ_CMD);
    (void) strcpy (cmd, out); op = out;

    pthread_mutex_unlock (&dtsq->mutex);

    return ( dts_strbuf (out) );
}


/**
 *  DTS_VERIFYDTS -- Verify a DTS queue connection.
 *
 *  @brief	Verify a DTS queue connection.
 *  @fn		char *dts_verifyDTS (char *host, char *qname, char *fname)
 *
 *  @param  host	DTS host name
 *  @param  qname	queue name
 *  @param  fname	file name to transfer
 *  @return		remote path to queue directory (caller frees ptr)
 */
char *
dts_verifyDTS (char *host, char *qname, char *fname)
{
    char  queuePath[SZ_PATH], chost[SZ_PATH];


    /*  First test:  Can we contact the DTS server at all, if not,
     *  save then request to the recovery directory.
     */
    if (strchr (host, (int) ':'))
	strcpy (chost, host);
    else {
	dtsQueue *dtsq = dts_queueLookup (qname);
	DTS *dtsP = (DTS *) dtsq->dts;

	sprintf (chost, "%s:%d", host, dtsP->serverPort);
    }
    if (dts_hostPing (chost) != OK) {
        if (dts_hostContact (chost) != OK) {
            dtsLog (dts, "Error: Cannot contact DTS host '%s'\n", chost);
            return ((char *) NULL);
        }
    } else if (dts->debug >= 3)
        dtsLog (dts, "....Ping '%s' succeeds....\n", chost);


    /*  Second test:  Ask the DTS if it is willing to accept the
     *  file.  If the return value is OK, the response string will
     *  be the directory to which we should transfer the file. On
     *  ERR, this will be a message string indicating the error.
     *  Error returns can be generated if the queue name is in-
     *  valid, a lack of disk space, a disabled queue, etc. (the
     *  error message will indicate the cause).
     */
    memset (queuePath, 0, SZ_PATH);
    if (dts->debug >= 3)
        dtsLog (dts, "....Init xfer 'h=%s  q=%s' ....\n", chost, qname);
        
    if (dts_hostInitTransfer (chost, qname, fname, queuePath) != OK) {
        dtsLog (dts, "%s\n", queuePath);
        return ((char *) NULL);
    } else if (dts->debug >= 3)
        dtsLog (dts, "....Init xfer 'h=%s  q=%s' succeeds....\n", chost, qname);

    if (dts->debug >= 3)
        dtsLog (dts, "'%s -- %s -- %s' verified....\n", chost, qname, fname);

    return (strdup (queuePath));
}


/**
 *  DTS_QPROCESS -- Process a file to submit it to the named queue.
 *
 *  @brief	Process a file to submit it to the named queue.
 *  @fn		stat = dts_queueProcess (dtsQueue *dtsq, char *lpath, 
 *			char *rpath, char *fname)
 *
 *  @param  dtsq	DTS queue pointer
 *  @param  lpath	local path
 *  @param  rpath	remote path
 *  @param  fname	filename to transfer
 *  @return		status result
 */
int
dts_queueProcess (dtsQueue *dtsq, char *lpath, char *rpath, char *fname)
{
    //static xferStat  xfs;
    xferStat  xfs;
    char   log_msg[SZ_LINE], dhost[SZ_PATH], *xArgs[2], *dp = NULL, *lp = NULL;
    int    loPort  = dtsq->port;
    int    hiPort  = dtsq->port + dtsq->nthreads - 1;
    int    ntries = 3, res = OK;
    int    debug = 0, verbose = 0;


    /*  Log the command if possible.
    */
    memset (log_msg, 0, SZ_LINE);
    if (debug > 2) {
        sprintf (log_msg, "%s: [%s] file = %s\n", dts_getLocalIP(), 
	    dtsq->name, fname);
        dtsLog (dts, log_msg);
    }

    memset (dhost, 0, SZ_PATH);
    if ((dp = dts_resolveHost (dtsq->dest)))
	strcpy (dhost, dp);
    else {
	dtsErrLog (dtsq, "Cannot resolve host '%s'\n", dtsq->dest);
	return (ERR);
    }


strcpy (dhost, (dp = dts_getAliasDest (dtsq->dest)));
free ((void *) dp);
    /* Finally, transfer the file.
    */
    xArgs[0] = calloc (1, SZ_PATH);
    xArgs[1] = calloc (1, SZ_PATH);
				  
    lp = dts_sandboxPath (lpath);
    sprintf (xArgs[0], "%s:%d:%s%s", dts->serverIP, dts->serverPort, lp, fname);
    sprintf (xArgs[1], "%s:%s%s", dhost, rpath, dts_pathFname(fname));
    free ((void *) lp);

        
    if (debug) {
        fprintf (stderr, "xArgs[0] = '%s'\n", xArgs[0]);
        fprintf (stderr, "xArgs[1] = '%s'\n", xArgs[1]);
        fprintf (stderr, "mode = '%s'  method = '%s'\n", 
	    dts_cfgQModeStr (dtsq->mode), dts_cfgQMethodStr (dtsq->method)); 
    }

    /*dts_qSetStatus (dtsq->dest, queue, "transferring");*/

try_again_:
    if (dtsq->mode == QUEUE_GIVE)
        res = dts_hostTo (dhost, dts->serverPort, dtsq->method, dtsq->udt_rate,
				loPort, hiPort,
                                dtsq->nthreads, XFER_PUSH, 2, xArgs, &xfs);
    else
        res = dts_hostTo (dhost, dts->serverPort, dtsq->method, dtsq->udt_rate,
				loPort, hiPort,
                                dtsq->nthreads, XFER_PULL, 2, xArgs, &xfs);

    if (dts->debug > 2)
        dtsLog (dts, "%6.6s >  XFER: %s to %s, stat=%d", 
	    dts_queueNameFmt (dtsq->name), fname, dhost, res);
    if (res != OK) {
        dtsLog (dts, "Transfer of '%s' to DTS on %s failed.\n", fname, dhost);
	if (--ntries)
            goto try_again_;
	else
            goto err_ret_;

    } else if (verbose) {
        printf ("(%s) %7.2f sec  %7.2f Mb/s %7.2f MB/s  %.3f Mbytes\n",
            (res == OK ? "OK" : "ERR"), xfs.time, xfs.tput_mb,
            xfs.tput_MB, ((double)xfs.fsize/MBYTE));
    }

    /*  Update the transfer statistics.
     */
    dts_hostUpStats (dhost, dtsq->name, &xfs);

    /*  Update the queue status file.
    dts_qSetStatus (dtsq->dest, dtsq->name, "complete");
     */

    /* Clean up.  On success, return zero to $status.
    */
err_ret_:
    free ((char *) xArgs[0]);
    free ((char *) xArgs[1]);

    /*dts_qSetStatus (dtsq->dest, queue, "ready");*/

    return (res);
}


/**
 *  DTS_QUEUEINITCONTROL -- Initialize the control structure.
 *
 *  @brief	Initialize the control structure.
 *  @fn		stat = dts_queueInitControl (char *qhost, char *qname, 
 *		    char *qpath, char *opath, char *lfname, char *fname, 
 *		    char *dfname)
 *
 *  @param  qhost	DTS Queue host name
 *  @param  qname	queue name
 *  @param  qpath	queue path name
 *  @param  opath	output path name
 *  @param  lpath	local path name
 *  @param  fname	filename (no path)
 *  @param  dfname	delivery filename
 *  @return		status
 */
int
dts_queueInitControl (char *qhost, char *qname, char *qpath, 
    char *opath, char *lfname, char *fname, char *dfname)
{
    char  *md5, chost[SZ_PATH], *cp;
    unsigned int crc32 = 0, res;
    Control ctrl;


    memset (chost, 0, SZ_PATH);
    if (strchr (qhost, (int) ':'))
	strcpy (chost, qhost);
    else {
	strcpy (chost, (cp = dts_getAliasDest (qhost)));
	free ((void *) cp);
    }

    /* dts_qSetStatus (chost, queue, "initializing"); */

    /*  Initialize the queue control file on the target machine.
     */
    memset (&ctrl, 0, sizeof ctrl);

    strcpy (ctrl.queueHost, dts_getLocalHost());
    strcpy (ctrl.queueName, qname);
    strcpy (ctrl.filename, dts_pathFname (fname));
    strcpy (ctrl.xferName, dts_pathFname (lfname));
    strcpy (ctrl.srcPath, dts_pathDir (lfname));
    strcpy (ctrl.igstPath, opath);
    strcpy (ctrl.deliveryName, dfname);

    ctrl.fsize = dts_du (lfname);
    ctrl.epoch = time (NULL);
    ctrl.isDir = dts_isDir (lfname);

    if (ctrl.isDir == 1) {
        ctrl.sum32 = ctrl.crc32 = 0;
        strcpy (ctrl.md5, " ");
    } else {
        if ((md5 = dts_fileMD5 (lfname))) {
            strcpy (ctrl.md5, md5);
            free ((void *) md5);
        }
        ctrl.sum32 = dts_fileCRCChecksum (lfname, &crc32);
        ctrl.crc32 = crc32;
    }

    /*  Call the method.
     */
    return ((int) (res = dts_hostSetQueueControl (chost, qpath, &ctrl)));
}


/**
 *  DTS_QUEUELOCK -- Set the mutex lock on the queue.
 *
 *  @brief	Set the mutex lock on the queue.
 *  @fn		void dts_queueLock (dtsQueue *dtsq)
 *
 *  @param  dtsq	DTS queue pointer
 *  @return		nothing
 */
void
dts_queueLock (dtsQueue *dtsq)
{
    pthread_mutex_lock (&dtsq->mutex);
}


/**
 *  DTS_QUEUEUNLOCK -- Unset the mutex lock on the queue.
 *
 *  @brief	Unset the mutex lock on the queue.
 *  @fn		void dts_queueUnlock (dtsQueue *dtsq)
 *
 *  @param  dtsq	DTS queue pointer
 *  @return		nothing
 */
void
dts_queueUnlock (dtsQueue *dtsq)
{
    pthread_mutex_unlock (&dtsq->mutex);
}


/**
 *  DTS_QUEUEFROMPATH -- Get queue name from a path.
 *
 *  @brief	Get queue name from a path.
 *  @fn		char *dts_queueFromPath (char *path)
 *
 *  @param  path	spool queue directory path
 *  @return		queue name
 */
char *
dts_queueFromPath (char *path)
{
    static char  qname[SZ_PATH];
    char  *ip, *op, buf[SZ_PATH], str[SZ_PATH];
    int    len = 0;


    memset (buf, 0, SZ_PATH);
    memset (str, 0, SZ_PATH);
    memset (qname, 0, SZ_PATH);

#ifdef FROM_EOS
    /* Extract the queue name.
     */
    strcpy (buf, path);
    for (ip=&buf[strlen(buf)]-1; *ip != '/' && ip >= &buf[0]; ip--)
        *ip = '\0';
    *ip = '\0';
    for (ip-- ; *ip != '/' && ip >= &buf[0]; ip--)
        ;
    strcpy (qname, ip+1);
#else
    if ((ip = strstr (path, "spool"))) {
	strcpy (buf, ip+6);
	for (ip=&buf[0], op=&str[0]; *ip && *ip != '/'; )
	    *op++ = *ip++;

	/* Truncate to 6 chars if needed:  first_five + last_ch
	 */
	if ((len = strlen (str)) > 6) {
	    sprintf (qname, "%-5.5s", str);
	    strncat (qname, &str[len-1], 1);
	} else
	    sprintf (qname, "%-s", str);
    } else {
	strcpy (qname, "      ");
    }
#endif

    return ( dts_strbuf (qname) );
}


/**
 *  DTS_QUEUENAMEFMT -- Format a queue name to 6 chars.
 *
 *  @brief	Format a queue name to 6 chars.
 *  @fn		void dts_queueNameFmt (char *qname)
 *
 *  @param  qname	full-length queue name
 *  @return		shortened queue name
 */
char *
dts_queueNameFmt (char *qname)
{
    char  name[SZ_PATH];
    int   len = 0;

    memset (name, 0, SZ_PATH);
    if ((len = strlen (qname)) > 6) {
	sprintf (name, "%-5.5s", qname);
	strncat (name, &qname[len-1], 1);
    } else
	sprintf (name, "%-6.6s", qname);

    return ( dts_strbuf (name) );
}


/**
 *  DTS_QUEUEDELETE -- Delete the complete spool directory.
 *
 *  @brief	Delete the complete spool directory.
 *  @fn		void dts_queueDelete (DTS *dts, char *qpath)
 *
 *  @param  dts		DTS struct pointer
 *  @param  qpath	spool queue directory path
 *  @return		nothing
 */
void
dts_queueDelete (DTS *dts, char *qpath)
{
    int res;

    dtsLog (dts, "%6.6s >  PURG: deleting %s", dts_queueFromPath(qpath), qpath);
    res = dts_unlink (qpath, TRUE, "*");
    rmdir (qpath);
}


/**
 *  DTS_LOGXFERSTATS -- Make a log entry of the transfer statistics
 *
 *  @brief	Make a log entry of the transfer statistics
 *  @fn		stat = dts_logXFerStats (Control *ctrl, xferStat *xfs, 
 *			int stat, char *cpath)
 *
 *  @param  ctrl	control structure
 *  @param  xfs		transfer stats structure
 *  @param  stat	status flag
 *  @param  path	path to control file
 *  @returns		OK or ERR code
 */
int
dts_logXFerStats (Control *ctrl, xferStat *xfs, int stat, char *path)
{
    FILE  *fd;

    if ((fd = dts_fopen (path, "a+")) == (FILE *) NULL)
	return (ERR);

    flock (fileno(fd), LOCK_EX); 	/* get exclusive lock file	*/

    /*  Write the log entry.
     */
    fprintf (fd, "%s %ld - %s - %s %s %s %ld %.3f %.3f\n",	
	dts_logtime(), (long) time ((time_t) 0),
	(stat ? "ERR" : "OK "),
	ctrl->filename, ctrl->xferName, ctrl->md5, ctrl->fsize,
	xfs->tput_mb, xfs->time);

    flock (fileno(fd), LOCK_UN); 	/* get exclusive lock file	*/
    fclose (fd);

    return (OK);
}


/**
 *  DTS_QUEUESETSTATS -- Update the transfer statistics for the queue.
 *
 *  @brief	Update the transfer statistics for the queue.
 *  @fn		void dts_queueSetStats (dtsQueue *dtsq, xferStat *xfs)
 *
 *  @param  dtsq	DTS Queue struct pointer
 *  @param  xfs		transfer statistics struct
 *  @return		nothing
 */
int
dts_queueSetStats (dtsQueue *dtsq, xferStat *xfs)
{
    DTS     *dts = (DTS *) dtsq->dts;
    FILE    *fd = (FILE *) NULL;
    char    statfile[SZ_PATH];
    int     nfiles = 0;
    float   rate = 0.0, time = 0.0, size = 0.0, xfer = 0.0;


    memset (statfile, 0, SZ_PATH);
    sprintf (statfile, "%s/spool/%s/stats", dts->serverRoot, dtsq->name);

    if (access (statfile, F_OK) == 0) {
        if ((fd = dts_fopen (statfile, "r+")) != (FILE *) NULL) {
	    fscanf (fd, "%d %g %g %g %g\n", 
		&nfiles, &rate, &time, &size, &xfer);
        }
    } else {
        if ((fd = dts_fopen (statfile, "w+")) == (FILE *) NULL)
	    return (1);
    }

    xfer += (xfs->fsize / GBYTE);
    size = (xfer / (float) (nfiles + 1));
    rate = (((rate * nfiles) + xfs->tput_mb) / (float)(nfiles + 1));
    time = (((time * nfiles) + xfs->time) / (float)(nfiles + 1));

    rewind (fd);
    fprintf (fd, "%d %g %g %g %g %g\n", nfiles + 1, rate, time, size, xfer,
	xfs->tput_mb);

    dts_fclose (fd);
    return (0);
}


/**
 *  DTS_QUEUEGETSTATS -- Get the stats structure from a queue.
 *
 *  @brief	Get the stats structure from a queue.
 *  @fn		stat = dts_queueGetStats (dtsQueue *dtsq)
 *
 *  @param  dtsq	DTS Queue structure
 *  @return		stats structure
 */
queueStat *
dts_queueGetStats (dtsQueue *dtsq)
{
    DTS    *dts = (DTS *) dtsq->dts;
    FILE   *fd = (FILE *) NULL;
    char    statfile[SZ_PATH];
    static  queueStat  qs;		// FIXME


    memset (statfile, 0, SZ_PATH);
    sprintf (statfile, "%s/spool/%s/stats", dts->serverRoot, dtsq->name);

    memset (&qs, 0, sizeof (queueStat));
    if (access (statfile, R_OK) == 0) {
        if ((fd = dts_fopen (statfile, "r")) != (FILE *) NULL) {
	    fscanf (fd, "%d %g %g %g %g %g\n", 
		&qs.nfiles, &qs.avg_rate, &qs.avg_time, &qs.avg_size,
		&qs.tot_xfer, &qs.tput_mb);
            dts_fclose (fd);
        }
    }
    return (&qs);
}




/******************************************************************************/
/**  Local Procedures							     **/
/******************************************************************************/

/**
 *  STRSUB -- Do a string subsitution.
 */
static int
strsub (char *in, char *from, char *to, char *outstr, int maxch)
{
    int   flen = strlen (from);
    int   nsub = 0;
    char  *ip, *op;

    if (!from || !to)
	return (0);

    for (ip=in, op=outstr; *ip; ip++) {
	if (! *ip || (ip - in) > maxch)
	    break;
	if (*ip == '$') {
	    /* Start of a macro. 
	     */
	    if (strncasecmp (ip, from, flen) == 0) {
	        /* Our macro, do the substitution.
		 */
		char *tp = to;

		ip += flen - 1;		/* skip the input macro		*/
		while (*tp)		/* copy replacement string	*/
	 	    *op++ = *tp++;
		nsub++;
	    } else {
	        /* Not our macro, just pass it through. 
		 */
	        *op++ = *ip;
	    }
	} else {
	    *op++ = *ip;
	}
    }
    *op = '\0';

    return (nsub);
} 


/**
 *  INTSTR - Convert an int value to a string.
 */
static char *
intstr (int val)
{
    char str[SZ_LINE];

    memset (str, 0, SZ_LINE);
    sprintf (str, "%d", val);
    return ( dts_strbuf (str) );
}
