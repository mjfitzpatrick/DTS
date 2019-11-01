/**
 *  DTS Command Interface - These are the methods implemented for he 
 *  XML-RPC interace.  These command are used from a variety of locations
 *  during the operation of the DTS (i.e. the queueing, logging or
 *  interactive environments.
 *
 *	initDTS			     - reinitialize DTS
 *	shutdownDTS		     - shutdown DTS, cancel all queues
 *	abortDTS		     - exit DTS immediately
 *      nodeStat		     - get status information from a node
 *      submitLogs                   - submit DTSQ logs from source
 *      getQLog                      - get the DTSQ log from DTSD
 *      eraseQLog                    - erase the DTSQ logs from DTSD
 *
 *  File Operations:
 *
 *	access <path> <mode>	     - check file access
 *      cat                          - catenate a file
 *      checksum <path>		     - checksum a file
 *      copy <from> <to>	     - copy a file from one path to another
 *      cfg <quiet>	     	     - get the current config table name
 *	cwd			     - get current working directory
 *	del <pattern> <pass> <recur> - delete a file
 *	dir <pattern> <long>	     - get a directory listing
 *      destDir <qname>	     	     - get dir listing for queue delivery dir
 *      checkDir <path>		     - test whether path is a directory
 *      chmod <path> <mode>          - change mode of a file
 *	diskFree		     - how much disk space is available?
 *	diskUsed		     - how much disk space is used?
 *	echo <str>		     - simple echo function
 *	fsize <fname>		     - get a file size
 *	fmode <fname>		     - get a file mode
 *	ftime <fname>		     - get a file time
 *	mkdir <path>		     - make a directory
 *	ping			     - simple aliveness test function
 *	    pingsleep <host> <T>     - test func w/ timeout
 *          pingstr <value> <str>    - test func w/ string return
 *          pingArray <value>        - test func w/ array return
 *          remotePing <value>       - 3rd party test function
 *      
 *	rename <old> <new>	     - rename a file
 *	stat <fname>		     - get file stat() info
 *	touch <path>		     - touch a file access time
 *
 *	setdbg 		     	     - set the debug value
 *	setroot <path>		     - set the root directory of a DTS
 *	unsetdbg 	     	     - unset the debug value
 *
 *  I/O Operations:
 *	read <path> <offset> <NB>    - read from a file
**	write <path> <offset> <NB>   - write to a file
 *	prealloc <fname> <nbytes>    - preallocate space for a file
 *
 *  Queue Operations:
 *
?*	startQueue		     - start the named queue
?*	stopQueue		     - stop the named queue
?*	shutdownQueue		     - stop the named queue
?*	flushQueue		     - flush the named queue
**	listQueue		     - list pending queue data
 *	pokeQueue		     - bump the 'current' file
 *	pauseQueue		     - stop processing of named queue
 *	restartQueue		     - restart (stop-then-start) queue
 *	addToQueue		     - add object to queue
 *	removeFromQueue		     - delete object from queue
 *	printQueueCfg		     - print queue configuration
 *
 *	getQueueStat		     - get queue status flag
 *	setQueueStat		     - set queue status flag
 *	getQueueCount		     - get pending count of named queue (sem)
 *	setQueueCount		     - set pending count of named queue (sem)
 *	getQueueDir		     - get queue delivery directory
 *	setQueueDir		     - set queue delivery directory
 *	getQueueCmd		     - get queue delivery command
 *	setQueueCmd		     - set queue delivery command
 *
 *	getCopyDir		     - get the DTS copy directory
**	setCopyDir		     - set the DTS copy directory
**	execCmd		     	     - execute the named command
 *
 *	list <option> 		     - list config parameters
 *	set <option> <value>	     - set a specified option
 *	get <option>		     - get value of option
 *
 *  DTSD Methods
 *
 *	queueAccept <qname> <fsize>  - accept new object to queue?
 *	queueValid <qname>    	     - See if queue name is valid
 *	queueComplete <qname> <path> - complete transfer of a file
 *	queueRelease <qname>	     - release lock of a queue semaphore
 *	queueDest <qname>	     - get the destination of queue
 *	queueSrc <qname>	     - get the source of queue
 *  	queueSetControl <args...>    - Set control file for transfer
 *	queueTrace		     - trace connectivity through a queue
 *	queueUpdateStats	     - update queue statistics
 *
 *	initTransfer <method>	     - initialize an object transfer
 *	doTransfer		     - transfer the actual file
 *	endTransfer		     - end the object transfer
 *	cancelTransfer		     - abort an in-progress transfer
 *	testFault		     -
 *
 *
 *  @file       dtsMethods.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/10/09
 *
 *  @brief  DTS Command Interface.  
 */


/******************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <pthread.h>
#include <ctype.h>
#include <utime.h>
#include <string.h>
#include <errno.h>
#ifdef Linux
#include <sys/vfs.h>                            /* for statfs()         */
#else
#ifdef Darwin
#include <sys/param.h>                          /* for statfs()         */
#include <sys/mount.h>
#endif
#endif
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/file.h>
#include <fcntl.h>

#include <xmlrpc-c/base.h>
#include <xmlrpc-c/client.h>
#include <xmlrpc-c/server.h>
#include <xmlrpc-c/server_abyss.h>

#include "dts.h"
#include "dtsdb.h"
#include "dtsMethods.h"


extern  DTS  *dts;
extern  char *build_version;



/*******************************************************************************
 *	initDTS			- reinitialize DTS
 *	shutdownDTS		- shutdown DTS, cancel all queues
 */

/**
 *  DTS_INITDTS -- (Re-)Initialize the DTS.
 *
 *  @brief	Initialize the DTS
 *  @fn		int dts_initDTS (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_initDTS (void *data)
{
    char *passwd  = xr_getStringFromParam (data, 0);

    fprintf (stderr, "initDTS:  passwd = '%s'\n", (passwd ? passwd : "none") );

    /* Not yet implemented */

    if (passwd) free ((char *) passwd);
    return (OK);
}


/**
 *  DTS_SHUTDOWNDTS -- Shut down the DTS daemon.
 *
 *  @brief	Shut down the DTS
 *  @fn 	int dts_shutdownDTS (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_shutdownDTS (void *data)
{
    char *passwd  = xr_getStringFromParam (data, 0);

    if (dts_validPasswd (dts, passwd) == OK) {
        xr_setShutdown (data, 1);
        xr_setIntInResult (data, (int) OK);  /* set result	*/
    }

    if (passwd) free ((char *) passwd);
    return (OK);
}


/**
 *  DTS_ABORTDTS -- Immediately exit the DTS daemon.
 *
 *  @brief	Immediately exit the DTS
 *  @fn 	int dts_abortDTS (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_abortDTS (void *data)
{
    char *passwd  = xr_getStringFromParam (data, 0);

    if (dts_validPasswd (dts, passwd) == OK) {
        xr_setShutdown (data, 1);
        xr_setIntInResult (data, (int) OK);  /* set result	*/
    }

    if (passwd) free ((char *) passwd);
    return (OK);
}


/**
 *  DTS_NODESTAT - Return status information for the given node.
 *
 *  @brief	Return status information for the given node.
 *  @fn		int dts_nodeStat (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_nodeStat (void *data)
{
    char *qname = xr_getStringFromParam (data, 0);
    int    errs = xr_getIntFromParam (data, 1);

    int   i, res = 0, nerrs = 0;
    int   next = 0, current = 0, anum = 0, rnum = 0;
    int  dfree = 0.0, dused = 0.0;
    char *rdir = dts_sandboxPath ("/"), root[SZ_PATH];
    static char  type[SZ_PATH], stat[SZ_PATH], sline[SZ_PATH];
    static char  curfil[SZ_PATH], nextfil[SZ_PATH];

    dtsQueue *dtsq = (dtsQueue *) NULL;
#ifdef Linux
    struct statfs fs;
#else
    struct statvfs fs;
#endif
    extern char * dts_cfgQNodeStr ();


    /* Values to get:
     *
     *	 0  root         s	node root directory
     *	 1  dfree        i	amount of free disk (MB)
     *	 2  dused        i	amount of used disk (MB)
     *	 3  nerrs        i	number of error messages
     *	 4  errs[]       s	error message array
     *
     *	 5  nqueue       i	number of queues managed
     *   6  Queue {
     *	      name       s	name of queue
     *	      current    i	current spool number
     *	      remain     i	remaining files to process
     *	      type       s	queue type
     *	      status     s	queue status
     *	      src        s	queue source
     *	      infile     s	incoming filename
     *	      outfile    s	outgoing filename
     *	      count      i	queue semaphore count
     *      }
     */

    memset (root, 0, SZ_PATH);			/* get root path	*/
    strcpy (root, rdir);
#ifdef Linux
    res = statfs (rdir, &fs);		/* get disk usage	*/
#else
    res = statvfs (rdir, &fs);		/* get disk usage	*/
#endif
    dfree = (fs.f_bavail * ((float)fs.f_frsize / (1024.*1024.)));
    dused = ((fs.f_blocks-fs.f_bavail) * ((float)fs.f_frsize / (1024.*1024.)));


    rnum = xr_newArray ();			/* Get result array	*/

    xr_setStringInArray (rnum, root);		/* 0 */
    xr_setIntInArray (rnum, dfree);		/* 1 */
    xr_setIntInArray (rnum, dused);		/* 2 */

#ifdef OLD_NERRS
    if (dtsq) {
        if (dtsq->nerrs <= errs)		/* no. of error msgs	*/
	    nerrs = dtsq->nerrs;
        else if (dtsq->nerrs > errs)   
	    nerrs = (dtsq->nerrs - errs);
        xr_setIntInArray (rnum, dtsq->nerrs);	/* 3 */
    } else {
        xr_setIntInArray (rnum, 0);		/* 3 */
    }
#else
    nerrs = dts->nerrs ;
    xr_setIntInArray (rnum, nerrs);		/* 3 */
#endif

    anum = xr_newArray ();			/* error array		*/
    for (i=0; i < nerrs && i < errs; i++)
	xr_setStringInArray (anum, dts->emsgs[i]);
    xr_setArrayInArray (rnum, anum);		/* 4 */

    xr_setIntInArray (rnum, 			/* 5 */
	(strcmp (qname, "all") == 0 ? dts->nqueues : 1));


    /* Search for the requested queue name.
     */
    for (i=0; i < dts->nqueues; i++) {

        if (strcmp ("all", qname) == 0 ||
	    strcmp (qname, dts->queues[i]->name) == 0) {
		int semval = -1;
		queueStat  *qs;

                dtsq = dts->queues[i];

    	 	memset (type,    0, SZ_PATH);
    	 	memset (stat,    0, SZ_PATH);
    		memset (curfil,  0, SZ_PATH);
    		memset (nextfil, 0, SZ_PATH);

    		sprintf (curfil, "%s/spool/%s/current", dts->serverRoot, 
		    dtsq->name);
    		sprintf (nextfil, "%s/spool/%s/next", dts->serverRoot, 
		    dtsq->name);

    		current = dts_queueGetCurrent (curfil);
    		next = dts_queueGetNext (nextfil);
		strcpy (type, (char *) dts_cfgQNodeStr (dtsq->node));
    		semval = dts_semGetVal (dtsq->activeSem);
		strcpy (stat, ( (semval == QUEUE_RUNNING  ? "running" :
		        	(semval == QUEUE_ACTIVE   ? "ready" :
				(semval == QUEUE_PAUSED   ? "paused" :
		        	(semval == QUEUE_WAITING  ? "waiting" :
		        	(semval == QUEUE_SHUTDOWN ? "shutdown" :
		        	(semval == QUEUE_RESPAWN  ? "respawn" :
		        	(semval == QUEUE_KILLED   ? "killed" :
			  	                            "invalid"))))))))
			);

		qs = dts_queueGetStats (dtsq);

		memset (sline, 0, SZ_PATH);
		dtsShm (dts);			// FIXME ???

		sprintf (sline, "%s %d %d %s %s %s %d %g %g %g %g %s %s %d %d",
		    dtsq->name, current, (next - current), type, stat,
		    dtsq->src, dts_semGetVal (dtsq->countSem),
		    qs->avg_rate, qs->avg_time, qs->avg_size, 
		    qs->tot_xfer, dtsq->infile, dtsq->outfile,
		    dtsq->qstat->numflushes, 
		    dtsq->qstat->canceledxfers
		);

		xr_setStringInArray (rnum, sline);
        }
    }

    /*  Set the array in the result.
     */
    xr_setArrayInResult (data, rnum);

    if (qname) free ((char *) qname);
    if (rdir)  free ((char *) rdir);

    return (OK);
}


/**
 *  DTS_SUBMITLOGS - Submit the DTSQ logs to the ingest node.
 *
 *  @brief	Submit the DTSQ logs to the ingest node.
 *  @fn		int dts_submitLogs (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_submitLogs (void *data)
{
    char *qname = xr_getStringFromParam (data, 0);
    char *log = xr_getStringFromParam (data, 1);
    char *recover = xr_getStringFromParam (data, 2);

    //dtsQueue *dtsq = dts_queueLookup (qname);
    char *dir = dts_getQueuePath (dts, qname);
    char  fname[SZ_PATH];
    FILE *fd;
    int   res = OK;


    /*  Append the Log and Recover file information.
     */
    if (log && *log) {
        memset (fname, 0, SZ_PATH);
        sprintf (fname, "%s/Log", dir);

	if ((fd = dts_fopen (fname, "a+")) != (FILE *) NULL) {
	    fprintf (fd, "%s", log);
	    if (log[strlen(log)-1] != '\n')
	        fprintf (fd, "\n");
	    dts_fclose (fd);
	} else {
	    dtsLog (dts, "Error: Cannot open 'Log' file.");
	    res = ERR;
	}
    }

    if (recover && *recover) {
        memset (fname, 0, SZ_PATH);
        sprintf (fname, "%s/Recover", dir);

	if ((fd = dts_fopen (fname, "a+")) != (FILE *) NULL) {
	    fprintf (fd, "%s", recover);
	    if (recover[strlen(recover)-1] != '\n')
	        fprintf (fd, "\n");
	    dts_fclose (fd);
	} else {
	    dtsLog (dts, "Error: Cannot open 'Recover' file.");
	    res = ERR;
	}
    }

    xr_setIntInResult (data, res);

    /*  Clean up.
     */
    if (qname) free ((char *) qname);
    if (log) free ((char *) log);
    if (recover) free ((char *) recover);

    return (OK);
}


/**
 *  DTS_GETQLOG - Get the contents of the specified queue log/recover file.
 *
 *  @brief	Get the contents of the specified queue log/recover file.
 *  @fn		int dts_getQLog (void *data)
 *
 *  @param  data	caller param data
 *  @return		contents of the requested file
 */
int
dts_getQLog (void *data)
{
    char *qname = xr_getStringFromParam (data, 0);
    char *logname = xr_getStringFromParam (data, 1);

    char  fname[SZ_PATH];
    int   fd = 0;


    memset (fname, 0, SZ_PATH);
    sprintf (fname, "%s/%s", dts_getQueuePath (dts, qname), logname);

    if ((fd = dts_fileOpen (fname, O_RDONLY)) > 0) {
	size_t  fsize = dts_fileSize (fd);
	char   *buf = calloc (1, fsize+2);

	int nread = dts_fileRead (fd, buf, (int) fsize);
	dts_fileClose (fd);

	xr_setStringInResult (data, (nread ? buf : ""));

	if (buf) free ((void *) buf);
    }


    /*  Clean up.
     */
    if (qname) free ((char *) qname);
    if (logname) free ((char *) logname);

    return (OK);
}


/**
 *  DTS_ERASEQLOG - Erase the contents of the specified queue log/recover file.
 *
 *  @brief	Erase the contents of the specified queue log/recover file.
 *  @fn		int dts_eraseQLog (void *data)
 *
 *  @param  data	caller param data
 *  @return		success or error code
 */
int
dts_eraseQLog (void *data)
{
    char *qname = xr_getStringFromParam (data, 0);
    char *logname = xr_getStringFromParam (data, 1);
    char  fname[SZ_PATH];


    memset (fname, 0, SZ_PATH);
    sprintf (fname, "%s/%s", dts_getQueuePath (dts, qname), logname);

    if (truncate (fname, (off_t) 0) < 0)
        xr_setIntInResult (data, OK);
    else
        xr_setIntInResult (data, ERR);

    /*  Clean up.
     */
    if (qname) free ((char *) qname);
    if (logname) free ((char *) logname);

    return (OK);
}



/*******************************************************************************
 *  File Operations:
 *
 *	access <path> <mode>	    - check file access
 *	cat <file>		    - 'cat' a file
 *	copy <old> <new>	    - copy a file
 *	cwd			    - get current working directory
 *	del <pattern>		    - delete a file
 *	dir <patter>		    - get a directory listing
 *	diskFree		    - how much disk space is available?
 *	diskUsed		    - how much disk space is used?
 *	echo <str>		    - simple echo function
 *	fsize <fname>		    - get a file size
 *	ftime <fname>		    - get a file time
 *	mkdir <path>		    - make a directory
 *	ping			    - simple aliveness test function
 *	prealloc <fname> <nbytes>   - preallocate space for a file
 *	rename <old> <new>	    - rename a file
 *	stat <fname>		    - get file stat() info
 *	setroot <path>		     - set the root directory of a DTS
 *	touch <fname>		    - touch a file access time
 */


/**
 *  DTS_ACCESS -- Check a file for an access mode.
 *
 *  @brief	Check a file for an access mode
 *  @fn 	int dts_Access (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Access (void *data)
{
    int   res  = 0;
    char *arg  = xr_getStringFromParam (data, 0);
    char *path = dts_sandboxPath (arg);
    int   mode = xr_getIntFromParam (data, 1);   	/* access mode	  */


    /* Check the file access.
    */
    res = access (path, mode);
    xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result	*/

    if (dts->verbose) dtsLog (dts, "ACCESS: %s = %d", path, res);

    if (arg)  free ((char *) arg);
    if (path) free ((char *) path);

    return (OK);
}


/**
 *  DTS_CAT -- Cat a file.
 *
 *  @brief	Cat a file
 *  @fn 	int dts_Cat (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Cat (void *data)
{
    char  *arg   = xr_getStringFromParam (data, 0);
    char  *path  = dts_sandboxPath (arg);
    char  *res = NULL, *eres = NULL, *emsg = "Error reading file";
    int    fd = 0, elen = strlen (emsg), snum = 0, size = 0;
    ushort sum16 = 0;
    uint   sum32 = 0;
    long   fsize = 0;
    struct stat st;


    if (dts->verbose) dtsLog (dts, "CAT: %s", path);

    if ((fd = open (path, O_RDONLY)) < 0) {
        xr_setStringInResult (data, "Cannot open file");
        goto err_ret;
    }

    /* Get the file size and allocate the result buffer.
    */
    (void) stat (path, &st);
    fsize = st.st_size;
    res  = calloc (1, (int) fsize);
    eres = calloc (1, (int) (2 * fsize));

    /* Read the file and set the result string.
    */
    if (dts_fileRead (fd, res, fsize) > 0) {
        checksum ((unsigned char *)res, (size=fsize), &sum16, &sum32);
	(void) base64_encode ((unsigned char *)res, fsize, eres, (2 * fsize));
    } else {
        checksum ((unsigned char *)emsg, (size=elen), &sum16, &sum32);
	(void) base64_encode ((unsigned char *)emsg, elen, eres, (2 * fsize));
    }

    snum = xr_newStruct ();
	xr_setIntInStruct (snum, "size",  (int) fsize);
	xr_setIntInStruct (snum, "sum32",  (int) sum32);
	xr_setStringInStruct (snum, "data",  eres);
    xr_setStructInResult (data, snum);
    xr_freeStruct (snum);

err_ret:
    if (res)  free ((char *) res);
    if (arg)  free ((char *) arg);
    if (eres) free ((char *) eres);
    if (path) free ((char *) path);
    close (fd);

    return (OK);
}


/**
 *  DTS_CHECKSUM -- Checksum a file.
 *
 *  @brief	Checksum a file
 *  @fn 	int dts_Checksum (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Checksum (void *data)
{
    char  *arg   = xr_getStringFromParam (data, 0);
    char  *path  = dts_sandboxPath (arg);
    char  *md5 = NULL;
    int    snum = 0;
    uint   sum32 = 0, crc32 = 0;


    if (dts->verbose) dtsLog (dts, "CHECKSUM: %s", path);

    /* Read the file and set the result struct.
    */
    md5   = dts_fileMD5 (path);
    sum32 = dts_fileChecksum (path, 1);
    crc32 = dts_fileCRC32 (path);

    snum = xr_newStruct ();
	xr_setIntInStruct (snum, "sum32",  (int) sum32);
	xr_setIntInStruct (snum, "crc",  (int) crc32);
	xr_setStringInStruct (snum, "md5",  md5);
    xr_setStructInResult (data, snum);
    xr_freeStruct (snum);

    if (md5)  free ((char *) md5);
    if (arg)  free ((char *) arg);
    if (path) free ((char *) path);

    return (OK);
}


/**
 *  DTS_COPY -- Copy a file.
 *
 *  @brief      Copy a file
 *  @fn 	int dts_Copy (void *data)
 *
 *  @param  data        caller param data
 *  @return             status code or errno
 */
int
dts_Copy (void *data)
{
    int    res = 0;
    char  *o_arg =  xr_getStringFromParam (data, 0);
    char  *n_arg =  xr_getStringFromParam (data, 1);
    char  *old = dts_sandboxPath (o_arg);
    char  *new = dts_sandboxPath (n_arg);


    /*  If the file doesn't exist, we can't rename it.
    */
    if (access (old, F_OK) < 0) {
	xr_setIntInResult (data, (int) ERR);
	goto err_ret;
    }


    /*  Otherwise, try copying the file.    -- FIXME
    */



    xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result	*/

    if (dts->verbose) dtsLog (dts, "COPY: %s -> %s", old, new);

err_ret:
    if (old)   free ((char *) old);
    if (new)   free ((char *) new);
    if (o_arg) free ((char *) o_arg);
    if (n_arg) free ((char *) n_arg);

    return (OK);
}


/**
 *  DTS_CFG -- Get the current configuration table name.
 *
 *  @brief	Get the current configuration table name.
 *  @fn 	int dts_Cfg (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Cfg (void *data)
{
    char  *buf = NULL;
    int  quiet = (int) xr_getIntFromParam (data, 0);


    /* Get current working directory.
    */
    buf = (char *) calloc (1, SZ_CONFIG);
    strcpy (buf, dts_fmtConfig (dts));

    if (dts->verbose && !quiet) 
	dtsLog (dts, "CFG: res = %s", buf);

    xr_setStringInResult (data, buf);		/* set result	*/

    if (buf)
	free ((void *) buf);
    return (OK);
}



/**
 *  DTS_CWD -- Get the current working directory.
 *
 *  @brief	Get the current working directory.
 *  @fn 	int dts_Cwd (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Cwd (void *data)
{
    char  *cwd, buf[SZ_PATH];


    /* Get current working directory.
    */
    memset (buf, 0, SZ_PATH);
    cwd = getcwd (buf, (size_t) SZ_PATH);

    if (dts->verbose) dtsLog (dts, "CWD: res = %s", buf);

    if (cwd)
        xr_setStringInResult (data, buf);		/* set result	*/
    else
        xr_setStringInResult (data, "unknown");

    return (OK);
}


/**
 *  DTS_DEL -- Delete a file.
 *
 *  @brief	Delete a file.
 *  @fn 	int dts_Delete (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Delete (void *data)
{
    int   res     = 0;
    char *in      = xr_getStringFromParam (data, 0);
    char *passwd  = xr_getStringFromParam (data, 1);   	/* host passwd    */
    int   recurse = xr_getIntFromParam (data, 2);   	/* resursive?     */
    char *path    = dts_sandboxPath (in);
    char  template[SZ_PATH];
    DIR  *dir;


    if (dts->verbose) 
	dtsLog (dts, "DEL: %s %s %s %d", in, path, passwd, recurse);

    /* Verify the host password.
    */
    if (dts_validPasswd (dts, passwd) == ERR) {
        xr_setIntInResult (data, (int) ERR);
fprintf (stderr, "password failed for '%s'\n", passwd);
        goto err_ret;
    }

    /*  If the file/dir doesn't exist, we can't delete it.  Skip this check
     *  if we're doing filename matching.
     */
    if (! dts_isTemplate (in)) {
        if (access (path, R_OK) < 0) {
            xr_setIntInResult (data, (int) ERR);
fprintf (stderr, "can't access '%s'\n", path);
            goto err_ret;
        } else if ( dts_isDir (path) ) {
            /* An error opening the directory is also bad.
             */
            xr_setIntInResult (data, (int) ERR);
            goto err_ret;
        }

    } else {
        char *ip;
        int  len  = strlen (path) - 1;

        /* Back up to the directory specification so we can isolate the 
         * the filename template.
         */
        for (ip=&path[len]; len; ip--, len--) {
            if (*ip == '/') {
                strcpy (template, (ip+1));
                *ip = '\0';
                break;
            }
        } 

        if ( (dir = opendir (path)) == (DIR *) NULL) {
            /* An error opening the directory is also bad.
            */
fprintf (stderr, "can't open dir '%s'\n", path);
            xr_setIntInResult (data, (int) ERR);
            goto err_ret;
        }
    }


    /* Delete the file.
    */
    if ( dts_isDir (path) )
        res = dts_unlink ((char *) path, recurse, template);
    else
        res = unlink ((char *) path);

    xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result	*/

    if (dts->verbose) dtsLog (dts, "DEL: %s = %d", path, res);

err_ret:
    if (in)     free ((char *) in);
    if (path)   free ((char *) path);
    if (passwd) free ((char *) passwd);

    return (OK);
}


/**
 *  DTS_DIR -- Get a directory listing.
 *
 *  @brief	Get a directory listing.
 *  @fn 	int dts_Dir (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Dir (void *data)
{
    char   *in    = xr_getStringFromParam (data, 0);
    int    lsLong = xr_getIntFromParam (data, 1);
    char   *path  = dts_sandboxPath (in), *list;
    char   template[SZ_PATH], line[SZ_LINE], dat[SZ_LINE], perms[SZ_LINE];
    char   root[SZ_PATH], fpath[SZ_PATH];
    static char dlist[MAX_DIR_ENTRIES * SZ_FNAME];

    DIR    *dir = (DIR *) NULL;
    struct  dirent *entry;
    struct  stat st;
    int     list_init = 0, dlen = 0;
    time_t  mtime = (time_t) 0;


    list = dlist;
    memset (root, 0, SZ_PATH);
    memset (template, 0, SZ_PATH);
    memset (list, 0, (MAX_DIR_ENTRIES * SZ_FNAME));


    /*  If the directory doesn't exist, we can't list it.  Skip this check
     *  if we're doing filename matching.
     */
    if (! dts_isTemplate (in)) {
        if (access (path, R_OK) < 0) {
	    xr_setIntInResult (data, (int) ERR);
	    goto err_ret;
        }
	strcpy (template, "*");

    } else {
	char *ip;
	int  len  = strlen (path) - 1;

	/* Back up to the directory specification so we can isolate the 
	 * the filename template.
	 */
	for (ip=&path[len]; len; ip--, len--) {
	    if (*ip == '/') {
		strcpy (template, (ip+1));
		*ip = '\0';
		break;
	    }
	}
    }

    (void) stat (path, &st);
    if (S_ISDIR(st.st_mode)) {
        if ( (dir = opendir (path)) == (DIR *) NULL) {
            /* An error opening the directory is also bad.
	    */
	    xr_setIntInResult (data, (int) ERR);
	    goto err_ret;
        } else
	    strcpy (root, path);

    } else {
	char  npath[SZ_PATH], *ip;
	int   len = strlen (path) - 1;

	for (ip=&path[len]; len; ip--, len--) {
	    if (*ip == '/') {
		*ip = '\0';
		break;
	    }
	}
	memset (npath, 0, SZ_PATH);
	strcpy (npath, (len ? path : "/"));
	memset (template, 0, SZ_PATH);
	strcpy (template, in);

	if ( (dir = opendir (npath)) == (DIR *) NULL) {
	    xr_setIntInResult (data, (int) ERR);
	    goto err_ret;
        } else
	    strcpy (root, npath);
    }


    /* Now read through the directory.
    */
    while ( (entry = readdir (dir)) != (struct dirent *) NULL ) {

 	memset (line,  0, SZ_LINE);
 	memset (dat,   0, SZ_LINE);
 	memset (perms, 0, SZ_LINE);
        memset (fpath, 0, SZ_PATH);

	if (strcmp(entry->d_name,".") == 0 || strcmp(entry->d_name,"..") == 0)
	    continue;		

	/* See if we match an expression.
	if (dts_isTemplate (in) && !dts_patMatch (entry->d_name, template))
	*/
	if (!dts_patMatch (entry->d_name, template))
	    continue;

	sprintf (fpath, "%s/%s", root, entry->d_name);
        (void) stat (fpath, &st);

	if (lsLong) {
	    mtime = (int) st.st_mtime;
	    strcpy (dat, ctime (&mtime));
	    dlen = strlen (dat);
	    dat[dlen-1] = '\0';

  	    dts_fmtMode (perms, (int) st.st_mode);

	    if (list_init++)
	        strcat (list, "\n");

	    sprintf (line, "%10s  %14ld  %s  %s", 
	        perms, (long) st.st_size, &dat[4], entry->d_name);

	    strcat (list, line);

	} else {
	    if (list_init++)
	        strcat (list, "\n");

	    strcat (list, entry->d_name);
    	    if (S_ISDIR(st.st_mode))
	        strcat (list, "/");
	}
    }


    /* Echo the string param we were sent.
    */
    xr_setStringInResult (data, list);			/* set result	  */

    if (dts->verbose) dtsLog (dts, "DIR: %s", path);

    if (dir) closedir (dir);				/* clean up	*/

err_ret:
    if (in)   free ((char *) in);
    if (path) free ((char *) path);

    return (OK);
}


/**
 *  DTS_DESTDIR -- Get a directory listing for a queue's delivery dir.
 *
 *  @brief	Get a directory listing for a queue's delivery dir.
 *  @fn 	int dts_DestDir (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_DestDir (void *data)
{
    char   *qname = xr_getStringFromParam (data, 0);
    char   *list, *path, root[SZ_PATH], fpath[SZ_PATH];
    static char dlist[MAX_DIR_ENTRIES * SZ_FNAME];
    dtsQueue *dtsq = (dtsQueue *) NULL;

    DIR    *dir = (DIR *) NULL;
    struct  dirent *entry;
    struct  stat st;
    int     list_init = 0;


    list = dlist;
    memset (root, 0, SZ_PATH);
    memset (list, 0, (MAX_DIR_ENTRIES * SZ_FNAME));


    /***********************************************************/ 
    dtsq = dts_queueLookup (qname);
    path = dtsq->deliveryDir;
    (void) stat (path, &st);
    if (S_ISDIR(st.st_mode)) {
        if ( (dir = opendir (path)) == (DIR *) NULL) {
            /* An error opening the directory is also bad.
	    */
	    xr_setIntInResult (data, (int) ERR);
	    goto err_ret;
        } else
	    strcpy (root, path);

    } else {
	char  npath[SZ_PATH], *ip;
	int   len = strlen (path) - 1;

	for (ip=&path[len]; len; ip--, len--) {
	    if (*ip == '/') {
		*ip = '\0';
		break;
	    }
	}
	memset (npath, 0, SZ_PATH);
	strcpy (npath, (len ? path : "/"));

	if ( (dir = opendir (npath)) == (DIR *) NULL) {
	    xr_setIntInResult (data, (int) ERR);
	    goto err_ret;
        } else
	    strcpy (root, npath);
    }


    /* Now read through the directory.
    */
    while ( (entry = readdir (dir)) != (struct dirent *) NULL ) {

	if (strcmp(entry->d_name,".") == 0 || strcmp(entry->d_name,"..") == 0)
	    continue;		

        memset (fpath, 0, SZ_PATH);
	sprintf (fpath, "%s/%s", root, entry->d_name);
        (void) stat (fpath, &st);

	if (list_init++)
	    strcat (list, "\n");

	strcat (list, entry->d_name);
    	if (S_ISDIR(st.st_mode))
	    strcat (list, "/");
    }


    /* Echo the string param we were sent.
    */
    xr_setStringInResult (data, list);			/* set result	  */

    if (dts->verbose) dtsLog (dts, "DESTDIR: %s", dtsq->deliveryDir);

err_ret:
    if (dir)   closedir (dir);				/* clean up	*/
    if (qname) free ((char *) qname);

    return (OK);
}


/**
 *  DTS_CHECKDIR -- Test whether path is a directory.
 *
 *  @brief	Test whether path is a directory.
 *  @fn 	int dts_CheckDir (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_CheckDir (void *data)
{
    int   res   = 0;
    char  *arg  = xr_getStringFromParam (data, 0);
    char  *path = dts_sandboxPath (arg);


    /* Check if directory.
    */
    res = dts_isDir (path);
    xr_setIntInResult (data, (int) res);		/* set result	*/

    if (dts->verbose) dtsLog (dts, "ISDIR: %s  %d", path, res);

    if (arg)  free ((void *) arg);
    if (path) free ((void *) path);

    return (OK);
}


/**
 *  DTS_CHMOD -- Change the mode of a file.
 *
 *  @brief	Change the mode of a file.
 *  @fn 	int dts_Chmod (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Chmod (void *data)
{
    int   res   = 0;
    char  *arg  = xr_getStringFromParam (data, 0);
    char  *path = dts_sandboxPath (arg);
    mode_t mode = (mode_t) xr_getIntFromParam (data, 1);


    /* Change the file mode.
    */
    if (access (path, F_OK) == 0) {
        res = chmod (path, mode);
	if (res < 0) 
	    dtsLog (dts, "CHMOD: chmod fails '%s'", strerror (errno));
    } else {
	dtsLog (dts, "CHMOD: cannot access '%s'", path);
	res = ERR; 
    }
    xr_setIntInResult (data, (int) res);		/* set result	*/

    if (dts->verbose > 2) dtsLog (dts, "CHMOD: %s (%d)  %d", path, mode, res);

    if (arg)  free ((void *) arg);
    if (path) free ((void *) path);

    return (OK);
}


/**
 *  DTS_DISKFREE -- Get disk space available on specified path.
 *
 *  @brief	Get 1K-block disk space available on specified path.
 *  @fn 	int dts_DiskFree (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_DiskFree (void *data)
{
    int   res  = 0;
    long  size = 0L;
    char  *arg = xr_getStringFromParam (data, 0);
    char  *dir = dts_sandboxPath (arg);
    struct statfs fs;


    /* Get the available disk space.
    */
    res = statfs (dir, &fs);
    size = (fs.f_bavail * (fs.f_bsize / 1024.));

    xr_setIntInResult (data, (int) size);		/* set result	*/

    if (dts->verbose) dtsLog (dts, "DFREE: %s  %d", dir, size);

    if (arg) free ((char *) arg);
    if (dir) free ((char *) dir);

    return (OK);
}


/**
 *  DTS_DISKUSED -- Get disk space used on specified path.
 *
 *  @brief	Get 1K-block disk space used on specified path.
 *  @fn 	int dts_DiskUsed (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_DiskUsed (void *data)
{
    struct statfs fs;
    int   res  = 0;
    long  size  = 0L;
    char  *arg = xr_getStringFromParam (data, 0);
    char  *dir = dts_sandboxPath (arg);


    /* Get the available disk space.
    */
    res = statfs (dir, &fs);
    size = ((fs.f_blocks - fs.f_bavail) * (fs.f_bsize / 1024.));

    xr_setIntInResult (data, (int) size);		/* set result	*/

    if (dts->verbose) dtsLog (dts, "DUSED: %s  %d", dir, size);

    if (arg) free ((char *) arg);
    if (dir) free ((char *) dir);

    return (OK);
}


/**
 *  DTS_ECHO -- Simple echo function.
 *
 *  @brief	Echo the input text.
 *  @fn 	int dts_Echo (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Echo (void *data)
{
    char  *val = xr_getStringFromParam (data, 0);   /* echo string	*/


    /* Echo the string param we were sent.
    */
    xr_setStringInResult (data, val);

    if (dts->verbose) dtsLog (dts, "ECHO: %s", val);

    if (val) free ((char *) val);

    return (OK);
}


/**
 *  DTS_FSIZE -- Get the file size for remote file.
 *
 *  @brief	Get the file size for remote file.
 *  @fn 	int dts_FSize (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_FSize (void *data)
{
    char  *arg  = xr_getStringFromParam (data, 0);
    char  *path = dts_sandboxPath (arg);
    long   size;


    /* Get the file size.
    */
    size = dts_du (path);
    xr_setIntInResult (data, (int) size);

    if (dts->verbose) dtsLog (dts, "FSIZE: %s", path, (int)size);

    if (arg)  free ((char *) arg);
    if (path) free ((char *) path);

    return (OK);
}


/**
 *  DTS_FMODE -- Get the file mode for remote file.
 *
 *  @brief	Get the file mode for remote file.
 *  @fn 	int dts_FMode (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_FMode (void *data)
{
    char  *arg  = xr_getStringFromParam (data, 0);
    char  *path = dts_sandboxPath (arg);
    struct stat fs;
    int   res;


    /* Get the file mode.
    */
    res = stat (path, &fs);
    xr_setIntInResult (data, (int) fs.st_mode);

    if (dts->verbose) dtsLog (dts, "FMODE: %s", path, (int)fs.st_mode);

    if (arg)  free ((char *) arg);
    if (path) free ((char *) path);

    return (OK);
}


/**
 *  DTS_FTIME -- Get the file access/create/modify time for remote file.
 *
 *  @brief	Get the file times for remote file.
 *  @fn 	int dts_FTime (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_FTime (void *data)
{
    char  *a_path = xr_getStringFromParam (data, 0);
    char  *mode   = xr_getStringFromParam (data, 1);
    char  *path   = dts_sandboxPath (a_path);
    struct stat fs;
    int    res, value = 0;


    /* Get the file access/modify/create time.
    */
    res = stat (path, &fs);
    if (tolower ((int) mode[0]) == 'a')
	value = (int) fs.st_atime;
    else if (tolower ((int) mode[0]) == 'm')
	value = (int) fs.st_mtime;
    else if (tolower ((int) mode[0]) == 'c')
	value = (int) fs.st_ctime;

    xr_setIntInResult (data, value);

    if (dts->verbose) {
        time_t tm = (time_t) value;
	dtsLog (dts, "FTIME: %s %s %s", path, mode, ctime(&tm));
    }

    if (a_path) free ((char *) a_path);
    if (path)   free ((char *) path);
    if (mode)   free ((char *) mode);

    return (OK);
}


/**
 *  DTS_MKDIR -- Make a directory.
 *
 *  @brief	Make a directory
 *  @fn 	int dts_Mkdir (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Mkdir (void *data)
{
    char   *in   = xr_getStringFromParam (data, 0);
    char   *path = dts_sandboxPath (in);
    char   *ip   = in, *op, *osp = NULL, dir[PATH_MAX];
    int     res  = 0;


    /* Create the directory.  It is not an error if the directory
     * already exists.
     */
    if (access (dir, R_OK|W_OK|X_OK)) {
        memset (dir, 0, PATH_MAX);
        op = dir;

        while (*ip) {
	    while ( *ip && *ip != '/' )
	        *op++ = *ip++;

	    /* Create a sub-element of the path.
	    */
	    if (access (dir, R_OK|W_OK|X_OK)) {
	        osp = dts_sandboxPath (dir);	/* output sandbox path */
	        if ((res = mkdir (osp, (mode_t) DTS_DIR_MODE)) < 0) {
		    free ((char *) osp), osp = NULL;
		    break;
		}
		free ((char *) osp), osp = NULL;
	    }

	    *op++ = *ip;
	    if (*ip && *ip == '/') 
	        ip++;
        }
    } else
	res = 0;

    xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result	*/

    if (dts->verbose) dtsLog (dts, "MKDIR: %s", path);

    if (path) free ((char *) path);
    if (osp)  free ((char *) osp);
    if (in)   free ((char *) in);

    return (OK);
}


/**
 *  DTS_PING -- Simple aliveness test function.
 *
 *  @brief	Is host and DTS alive.
 *  @fn 	int dts_Ping (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Ping (void *data)
{
    int ping_value = (int) xr_getIntFromParam (data, 0);


    /* We don't use the caller data, if we got this far we're alive so
    ** set a positive result.
    */
    xr_setIntInResult (data, (int) OK);

    if (dts->verbose > 3) dtsLog (dts, "PING: %04d", ping_value);
    return (OK);
}


/**
 *  DTS_PINGSLEEP -- Simple aliveness test function.
 *
 *  @brief	Is host and DTS alive.
 *  @fn 	int dts_PingSleep (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 *  
 *  NOTE:  This is an ASYNC method.
 */
int 
dts_PingSleep (void *data)
{
    char     *host = xr_getStringFromParam (data, 0);
    int ping_value = (int) xr_getIntFromParam (data, 1);
    int client = 0;


#ifdef USE_FORK
    /* Handle processing in the background.
     */
    switch (fork ()) {
    case 0:
        break;                              /* We are the child     */
    case -1:
        return (ERR);                       /* We are an error      */
    default:
        xr_setIntInResult (data, (int) OK);
        return (OK);                        /* We are the parent    */
    }
#endif


    if (ping_value)
	dtsSleep (ping_value);


    /* Send caller the result.
    */
    client = dts_getClient (host);
    xr_setIntInParam (client, (int) ping_value);
    xr_setIntInParam (client, (int) OK);
    if (xr_callSync (client, "psHandler") == OK) {
        xr_setIntInResult (data, OK);			/* set result	*/
        dts_closeClient (client);
#ifdef USE_FORK
        exit (0);
#else
        return (OK);
#endif
    }
    dts_closeClient (client);

    if (dts->verbose > 2) 
	dtsLog (dts, "PINGSLEEP: %04d", ping_value);
    free ((void *) host);

#ifdef USE_FORK
    exit (0);
#else
    xr_setIntInResult (data, OK);			/* set result	*/
    return (OK);
#endif
}

int 
dts_psHandler (void *data)
{
    int      t = (int) xr_getIntFromParam (data, 0);
    int status = (int) xr_getIntFromParam (data, 1);

    dtsLog (dts, "pingsleep %d returns status=%d\n", t, status);
    xr_setIntInResult (data, OK);			/* set result	*/

    return (OK);
}

int 
dts_nullHandler (void *data)
{
    xr_setIntInResult (data, OK);			/* set result	*/
    return (OK);
}

/**
 *  DTS_PINGSTR -- Simple aliveness test function, string return
 *
 *  @brief	Is host and DTS alive.
 *  @fn 	int dts_PingStr (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_PingStr (void *data)
{
    int ping_value = xr_getIntFromParam (data, 0);
    char      *str = xr_getStringFromParam (data, 1);


    /* We don't use the caller data, if we got this far we're alive so
    ** set a positive result.
    */
    xr_setStringInResult (data, "OK");			/* set result	*/

    if (dts->verbose > 2) dtsLog (dts, "PING: %04d", ping_value);
    
    free ((char *) str);
    return (OK);
}


/**
 *  DTS_PINGARRAY -- Simple aliveness test function, array return
 *
 *  @brief	Is host and DTS alive.
 *  @fn 	int dts_PingStr (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_PingArray (void *data)
{
    int ping_value = (int) xr_getIntFromParam (data, 0);
    int anum = 0;


    anum = xr_newArray ();
        xr_setIntInArray (anum, 1);
        xr_setIntInArray (anum, 2);
        xr_setIntInArray (anum, 3);
    xr_setArrayInResult (data, anum);
/*
    xr_freeArray (anum);
*/

    if (dts->verbose > 2) dtsLog (dts, "PING: %04d", ping_value);
    
    return (OK);
}


/**
 *  DTS_REMOTEPING -- Simple 3rd party aliveness test function.
 *
 *  @brief	Is remote host and DTS alive.
 *  @fn 	int dts_remotePing (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_remotePing (void *data)
{
    char *remote   = xr_getStringFromParam (data, 0);
    int   res = -1;

    /* Our result is the result of a ping to the host named in the arg.
    */
    res = dts_hostPing (remote);   /* set result  */
    xr_setIntInResult (data, (int) res);   /* set result  */

    if (dts->verbose > 2) dtsLog (dts, "RPNG:%s", remote);

    if (remote) free ((char *) remote);

    return (OK);
}


/**
 *  DTS_RENAME -- Rename a file.
 *
 *  @brief	Rename a file.
 *  @fn 	int dts_Rename (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Rename (void *data)
{
    int    res   = 0;
    char  *o_arg = xr_getStringFromParam (data, 0);
    char  *n_arg = xr_getStringFromParam (data, 1);
    char  *old   = dts_sandboxPath (o_arg);
    char  *new   = dts_sandboxPath (n_arg);


    /*  If the file doesn't exist, we can't rename it.
    */
    if (access (old, F_OK) < 0) {
	xr_setIntInResult (data, (int) ERR);
	goto err_ret;
    }

    /*  Otherwise, try renaming the file.
    */
    res = rename (old, new);
    xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result	*/

    if (dts->verbose) dtsLog (dts, "RENAME: %s -> %s", old, new);

err_ret:
    if (old)   free ((char *) old);
    if (new)   free ((char *) new);
    if (o_arg) free ((char *) o_arg);
    if (n_arg) free ((char *) n_arg);

    return (OK);
}


/**
 *  DTS_SETROOT -- Set the root directory of a DTS.
 *
 *  @brief	Set the root directory of a DTS.
 *  @fn 	int dts_SetRoot (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_SetRoot (void *data)
{
    int     res  = 0;
    char   *arg  = xr_getStringFromParam (data, 0);
    char   *path = dts_sandboxPath (arg);


    if (dts->verbose)  dtsLog (dts, "setRoot: %s", path);

    /* If the directory doesn't exist, create it.
    */
    if (access (path, F_OK) < 0) {
        res = mkdir (path, (mode_t) DTS_DIR_MODE);
        xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result */
    }

    /* If we created the directory, or it already exists, change to the cwd.
    */
    if (res == OK) {
	res = chdir (path);
        xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result */
    }

    /* Initialize the DTS working directory.
    */
    if (dts->serverRoot)
	strcpy (dts->serverRoot, path);
    dts_initServerRoot (dts);

    if (arg)  free ((void *) arg);
    if (path) free ((void *) path);

    return (OK);
}


/**
 *  DTS_SETDBG -- Set a debug flag on the DTS node machine.
 *
 *  @brief	Set a debug flag on the DTS node machine.
 *  @fn 	int dts_SetDbg (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_SetDbg (void *data)
{
    int     res  = 0;
    char   *flag = xr_getStringFromParam (data, 0);
    char    path[SZ_LINE];


    if (dts->verbose)  dtsLog (dts, "setDbg: %s", flag);

    /* Touch the flag file in the /tmp directory.
    */
    memset (path, 0, SZ_LINE);
    sprintf (path, "/tmp/%s", flag);

    if (access (path, F_OK) < 0) {
        res = creat (path, (mode_t) DTS_DIR_MODE);
        xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result */
    } else
        xr_setIntInResult (data, (int) OK);

    if (flag)  free ((void *) flag);

    return (OK);
}


/**
 *  DTS_UNSETDBG -- Unset a debug flag on the DTS node machine.
 *
 *  @brief	Unset a debug flag on the DTS node machine.
 *  @fn 	int dts_UnsetDbg (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_UnsetDbg (void *data)
{
    int     res  = 0;
    char   *flag = xr_getStringFromParam (data, 0);
    char    path[SZ_LINE];


    if (dts->verbose)  dtsLog (dts, "unsetDbg: %s", flag);

    /* Touch the flag file in the /tmp directory.
    */
    memset (path, 0, SZ_LINE);
    sprintf (path, "/tmp/%s", flag);

    if (access (path, F_OK) < 0) {
        res = unlink (path);
        xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result */
    } else
        xr_setIntInResult (data, (int) OK);

    if (flag)  free ((void *) flag);

    return (OK);
}


/**
 *  DTS_TOUCH -- Touch a file.
 *
 *  @brief	Touch a file.
 *  @fn 	int dts_Touch (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Touch (void *data)
{
    int     res  = 0, ifd = 0;
    struct  utimbuf tb;
    time_t  now  = time ((time_t) 0);
    char   *arg  = xr_getStringFromParam (data, 0);
    char   *path = dts_sandboxPath (arg);


    if (dts->verbose)  dtsLog (dts, "TOUCH: %s", path);

    /* If the file doesn't exist, create it.
    */
    if (access (path, F_OK) < 0) {
	if ( (ifd = creat (path, DTS_FILE_MODE)) < 0 ) {
	    xr_setIntInResult (data, (int) ERR);
	    goto err_ret;
	}
	close (ifd);
    }

    /* Touch a file's access/modify time with the current time.
    */
    tb.actime  = now;
    tb.modtime = now;
    res = utime ( (char *) path, &tb);
    xr_setIntInResult (data, (int) (res ? errno : OK) );  /* set result	*/

err_ret:
    if (arg)  free ((void *) arg);
    if (path) free ((void *) path);

    return (OK);
}





/*******************************************************************************
 *	read			- read from a file
 *	write			- write to a file
 *	stat			- stat() a file
 *	prealloc		- preallocate a file
 *
 */

/**
 *  DTS_READ -- Read from a file.  We read at most 4096 bytes from the
 *  file at a time.  On input we get the offset into the file and the
 *  requested number of bytes.  We update the offset on output and return
 *  the number of bytes read as a base-64 encoded string.
 *
 *  @brief      Read from a file
 *  @fn 	int dts_Read (void *data)
 *
 *  @param  data        caller param data
 *  @return             status code or errno
 */
int
dts_Read (void *data)
{
    char  *arg  = xr_getStringFromParam (data, 0);
    char  *path = dts_sandboxPath (arg);
    int    offset = xr_getIntFromParam (data, 1);
    int    nbytes = xr_getIntFromParam (data, 2);
    char  *res = NULL, *eres = NULL, *emsg = "Error reading file";
    int    fd = 0, elen = strlen (emsg), size, off, snum, nread = 0;
    struct stat st;
    long   fsize;


    if (dts->verbose > 2) 
	dtsLog (dts, "READ: %s off=%d nb=%d", path, offset, nbytes);
	
    if ((fd = open (path, O_RDONLY)) < 0) {
        xr_setStringInResult (data, "Cannot open file");
        goto err_ret;
    }

    /*  Get the file size and allocate the result buffer.
    */
    (void) stat (path, &st);
    fsize = st.st_size;
    if (offset > fsize) {
	/* If the offset is beyong the EOF, return EOF as the size.
	*/
        snum = xr_newStruct ();
	    xr_setIntInStruct (snum, "size",   (int) EOF);
	    xr_setIntInStruct (snum, "offset", (int) 0);
	    xr_setStringInStruct (snum, "data",  "");
        xr_setStructInResult (data, snum);
        xr_freeStruct (snum);

        close (fd), fd = 0;
        goto err_ret;
    }

    /*  Now figure out how many to actually read.
    */
    if (nbytes > SZ_BLOCK) nbytes = SZ_BLOCK;	/* no more than 4096	*/
    if (nbytes > fsize)    nbytes = fsize;	/* no more than fsize	*/

    if (offset > 0) {
        if ((fsize - offset) < nbytes)
	    nbytes = (fsize - offset);
    }

    res  = calloc (1, (int) nbytes);
    eres = calloc (1, (int) (2 * nbytes));

    off = lseek (fd, (off_t) offset, SEEK_SET);	/* seek to file offset  */


    /*  Read the file and set the result base-64 encoded string.
    */
    if ((nread = dts_fileRead (fd, res, nbytes)) > 0) {
	(void) base64_encode ((unsigned char *)res, nread, eres, (2 * nbytes));
	size = nread;
    } else {
	(void) base64_encode ((unsigned char *)emsg, elen, eres, (2 * nbytes));
	size = elen;
    }
    off = offset + nread;

    /* Set the result struct.
    */
    snum = xr_newStruct ();
	xr_setIntInStruct (snum, "size",   (int) nread);
	xr_setIntInStruct (snum, "offset", (int) off);
	xr_setStringInStruct (snum, "data", eres);
    xr_setStructInResult (data, snum);
    xr_freeStruct (snum);


err_ret:
    if (arg)  free ((char *) arg);
    if (res)  free ((char *) res);
    if (eres) free ((char *) eres);
    if (path) free ((char *) path);
    if (fd)   close (fd);

    return (OK);
}


/**
 *  DTS_WRITE -- Write to a file.
 *
 *  @brief      Read from a file
 *  @fn 	int dts_Write (void *data)
 *
 *  @param  data        caller param data
 *  @return             status code or errno
 */
int
dts_Write (void *data)
{
    /*
    char  *path = dts_sandboxPath (xr_getStringFromParam (data, 0));
    int  offset = xr_getIntFromParam (data, 1);
    int  nbytes = xr_getIntFromParam (data, 2);
    char *dat   = xr_getStringFromParam (data, 3);	// free ptr
    */



    return (OK);
}


/**
 *  DTS_PREALLOC -- Pre-allocate a file.
 *
 *  @brief	Pre-allocate a file.
 *  @fn 	int dts_Prealloc (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Prealloc (void *data)
{
    int    res  = 0;
    char  *arg  = xr_getStringFromParam (data, 0);
    char  *path = dts_sandboxPath (arg);
    int    size = xr_getIntFromParam (data, 1);   	/* file size	  */


    /* Preallocate a file of the given size.
    */
    res = dts_preAlloc (path, (long) size);
    xr_setIntInResult (data, (int) res);		/* set result	*/

    if (dts->verbose) dtsLog (dts, "PREALLOC: %s %d %d", path, size, res);

    if (arg)  free ((char *) arg);
    if (path) free ((char *) path);

    return (OK);
}


/**
 *  DTS_STAT -- Get the stat() info for a file.
 *
 *  @brief	Get the  stat() info for a file.
 *  @fn 	int dts_Stat (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Stat (void *data)
{
    char  *arg  = xr_getStringFromParam (data, 0);
    char  *path = dts_sandboxPath (arg);
    struct stat st;
    struct stat *p = &st;
    int   res, anum;


    /*  Get the file size.
    */
    if ((res = stat (path, p)) < 0) {
        xr_setIntInResult (data, ERR);		// method is an ERR
	goto ret_;
    }

    /*  Store the entire structure in an array.
     */
    anum = xr_newArray ();
	xr_setIntInArray (anum,      (int) st.st_dev);
	xr_setIntInArray (anum,      (int) st.st_ino);
	xr_setIntInArray (anum,      (int) st.st_mode);
	xr_setIntInArray (anum,      (int) st.st_nlink);
	xr_setIntInArray (anum,      (int) st.st_uid);
	xr_setIntInArray (anum,      (int) st.st_gid);
	xr_setIntInArray (anum,      (int) st.st_rdev);
	xr_setLongLongInArray (anum, (long long) st.st_size);
	xr_setIntInArray (anum,      (int) st.st_blksize);
	xr_setIntInArray (anum,      (int) st.st_blocks);
	xr_setIntInArray (anum,      (int) st.st_atime);
	xr_setIntInArray (anum,      (int) st.st_mtime);
	xr_setIntInArray (anum,      (int) st.st_ctime);

    xr_setIntInResult (data, res);
    xr_setArrayInResult (data, anum);
    xr_freeArray (anum);

    if (dts->verbose) dtsLog (dts, "STAT: %s", path);

ret_:
    if (arg)  free ((char *) arg);
    if (path) free ((char *) path);

    return (OK);
}


/**
 *  DTS_STATVAL -- Get a single value from the stat() info for a file.
 *
 *  @brief	Get a single value from the stat() info for a file.
 *  @fn 	int dts_StatVal (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_StatVal (void *data)
{
    char  *arg  = xr_getStringFromParam (data, 0);
    char  *val  = xr_getStringFromParam (data, 1);
    char  *path = dts_sandboxPath (arg);
    struct stat st;
    struct stat *p = &st;
    int   res = OK;


    /*  Get the file size.
    */
    if ((res = stat (path, p)) < 0) {
        xr_setIntInResult (data, ERR);		// method is an ERR
	goto ret_;
    }

    /*  Store the entire structure in an array.
     */
    if (strcmp (val, "st_dev") == 0) {
        xr_setIntInResult (data, st.st_dev);
    } else if (strcmp (val, "st_ino") == 0) {
        xr_setIntInResult (data, st.st_ino);
    } else if (strcmp (val, "st_mode") == 0) {
        xr_setIntInResult (data, st.st_mode);
    } else if (strcmp (val, "st_nlink") == 0) {
        xr_setIntInResult (data, st.st_nlink);
    } else if (strcmp (val, "st_uid") == 0) {
        xr_setIntInResult (data, st.st_uid);
    } else if (strcmp (val, "st_gid") == 0) {
        xr_setIntInResult (data, st.st_gid);
    } else if (strcmp (val, "st_rdev") == 0) {
        xr_setIntInResult (data, st.st_rdev);
    } else if (strcmp (val, "st_size") == 0) {
        xr_setIntInResult (data, (int) st.st_size);
    } else if (strcmp (val, "st_blksize") == 0) {
        xr_setIntInResult (data, st.st_blksize);
    } else if (strcmp (val, "st_blocks") == 0) {
        xr_setIntInResult (data, st.st_blocks);
    } else if (strcmp (val, "st_atime") == 0) {
        xr_setIntInResult (data, st.st_atime);
    } else if (strcmp (val, "st_ctime") == 0) {
        xr_setIntInResult (data, st.st_ctime);
    } else if (strcmp (val, "st_mtime") == 0) {
        xr_setIntInResult (data, st.st_mtime);
    } else {
	/*  Invalid value.
	 */
        xr_setIntInResult (data, ERR);
    }

    if (dts->verbose) dtsLog (dts, "STAT: %s", path);

ret_:
    if (arg)  free ((char *) arg);
    if (val)  free ((char *) val);
    if (path) free ((char *) path);

    return (OK);
}



/*******************************************************************************
 *	startQueue		- start the named queue
 *	shutdownQueue		- shut down the named queue
 *	pauseQueue		- pause the named queue
 *	pokeQueue		- poke the named queue
 *	stopQueue		- stop the named queue
 *	flushQueue		- flush the named queue
 *	listQueue		- list pending queue data
 *	restartQueue		- restart (stop-then-start) queue
 *	addToQueue		- add object to queue
 *	removeFromQueue		- delete object from queue
 *	printQueueCfg		- print the queue configuration
 */


/**
 *  DTS_STARTQUEUE --  Start the named DTS queue
 *
 *  @brief	Start the named DTS queue
 *  @fn		int dts_startQueue (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_startQueue (void *data)
{
    char *qname   = xr_getStringFromParam (data, 0); /* queue name      */
    dtsQueue *dtsq = dts_queueLookup (qname);
    int semval = -1;


    semval = dts_semGetVal (dtsq->activeSem);

    switch(semval) {
    case QUEUE_ACTIVE:
    case QUEUE_PAUSED:
    case QUEUE_WAITING:
        dtsLog (dts, "{%s}: startQueue: setting val=%d  sem=%d\n",
                qname, QUEUE_ACTIVE, dtsq->activeSem);

        pthread_mutex_lock (&dtsq->mutex);
        dts_semSetVal (dtsq->activeSem, QUEUE_ACTIVE);
        dtsq->status = QUEUE_ACTIVE;
        pthread_mutex_unlock (&dtsq->mutex);

        xr_setIntInResult (data, OK);       /* No useful result returned .... */
        if (qname) 
            free ((void *) qname);
        return (OK);

    case QUEUE_KILLED:
        //Generate new thread.
        pthread_mutex_lock (&dtsq->mutex);
        dts_semSetVal (dtsq->activeSem, QUEUE_RESPAWN);
        dtsq->status = QUEUE_RESPAWN;
        pthread_mutex_unlock (&dtsq->mutex);
        dtsLog (dts, "{%s}: startQueue:  RESPAWNING THREAD\n",qname);

    case QUEUE_RESPAWN:
    case QUEUE_RUNNING:
    default:
        xr_setIntInResult (data, OK);       /* No useful result returned .... */
        if (qname) 
            free ((void *) qname);
        return (OK);
    }
}


/**
 *  DTS_SHUTDOWNQUEUE -- Shutdown the named DTS queue
 *
 *  @brief      Shut down the named DTS queue
 *  @fn         int dts_shutdownQueue (void *data)
 *
 *  @param  data        caller param data
 *  @return             status code or errno
 */
int
dts_shutdownQueue(void *data)
{
    char *qname   = xr_getStringFromParam (data, 0); /* queue name */
    dtsQueue *dtsq = dts_queueLookup (qname);
    int semval = -1;
 
 
    /* signal (SIGTERM, SIG_IGN); ignore this signal
     */

    semval = dts_semGetVal (dtsq->activeSem);

    switch(semval) {
    case QUEUE_RUNNING:
    case QUEUE_ACTIVE:
    case QUEUE_PAUSED:
    case QUEUE_WAITING:
        dtsLog (dts, "{%s}: shutdownQueue:  setting val=%d  sem=%d\n",
            qname, QUEUE_SHUTDOWN, dtsq->activeSem);
	pthread_mutex_lock (&dtsq->mutex);
	dts_semSetVal (dtsq->activeSem, QUEUE_SHUTDOWN);
	dtsq->status = QUEUE_SHUTDOWN;
	pthread_mutex_unlock (&dtsq->mutex);

        // dtsLog (dts, "{%s}: shutdownQueue: killing=(%d) thread.\n",
	// 	  qname, dtsq->qm_tid);

	/* FALL-THROUGH */
    case QUEUE_RESPAWN:
    case QUEUE_SHUTDOWN:
    default:
	;
    }

    xr_setIntInResult (data, OK); /* No useful result returned .... */
    if (qname)
        free ((void *) qname);
    return (OK);
}


/**
 *  DTS_FLUSHQUEUE -- Set current to next, which is used to skip over files.
 *
 *  @brief      Flush the named queue
 *  @fn         int dts_flushQueue (void *data)
 *
 *  @param  data        caller param data
 *  @return             status code or errno
 */
int
dts_flushQueue (void *data)
{
    char *qname   = xr_getStringFromParam (data, 0); /* queue name      */
    char curfil[SZ_PATH], nextfil[SZ_PATH];
    int current = 0, next = 0;
    dtsQueue *dtsq = dts_queueLookup (qname);


    memset (curfil, 0, SZ_PATH);
    sprintf (curfil, "%s/spool/%s/current", dts->serverRoot, dtsq->name);

    memset (nextfil, 0, SZ_PATH);
    sprintf (nextfil, "%s/spool/%s/next", dts->serverRoot, dtsq->name);

    pthread_mutex_lock (&dtsq->mutex);

    current = dts_queueGetCurrent (curfil);
    next = dts_queueGetNext (nextfil);

    if (current < next) {
        dtsShm(dts);
        dtsq->qstat->numflushes++;
    }
    dts_queueSetCurrent (curfil, next);

    pthread_mutex_unlock (&dtsq->mutex);

    // if (dts->verbose)
    //  dtsLog (dts, "{%s}: flushQueue current was=%d current is now=%d\n",
    //      qname,current,next);

    xr_setIntInResult (data, OK);       /* No useful result returned .... */
    if (qname)
        free ((void *) qname);
    return (OK);
}


/**
 *  DTS_POKEQUEUE --  Add 1 to current, which is used to skip over files.
 *
 *  @brief      Poke the named queue
 *  @fn         int dts_pauseQueue (void *data)
 *
 *  @param  data        caller param data
 *  @return             status code or errno
 */
int
dts_pokeQueue (void *data)
{
    char *qname   = xr_getStringFromParam (data, 0); /* queue name      */
    char curfil[SZ_PATH], nextfil[SZ_PATH];
    int current = 0;
    int next = 0;
    dtsQueue *dtsq = dts_queueLookup (qname);

    memset (curfil, 0, SZ_PATH);
    sprintf (curfil, "%s/spool/%s/current", dts->serverRoot, dtsq->name);

    memset (nextfil, 0, SZ_PATH);
    sprintf (nextfil, "%s/spool/%s/next", dts->serverRoot, dtsq->name);

    if (current < next) {
        pthread_mutex_lock (&dtsq->mutex);

        current = dts_queueGetCurrent (curfil);
        next = dts_queueGetNext (nextfil);

        dtsShm (dts);
        dtsq->qstat->canceledxfers++;
        dts_queueSetCurrent (curfil, current+1);
        current++;

        pthread_mutex_unlock (&dtsq->mutex);
    }

    // if (dts->verbose)
    //    dtsLog (dts, "{%s}: pokeQueue: current now=%d\n", qname, current);

    xr_setIntInResult (data, OK);       /* No useful result returned .... */
    if (qname)
        free ((void *) qname);
    return (OK);
}


/**
 *  DTS_PAUSEQUEUE --  Stop processing of the named queue.
 *
 *  @brief      Pause processing of the named queue
 *  @fn         int dts_pauseQueue (void *data)
 *
 *  @param  data        caller param data
 *  @return             status code or errno
 */
int
dts_pauseQueue (void *data)
{
    char *qname   = xr_getStringFromParam (data, 0); /* queue name      */
    dtsQueue *dtsq = dts_queueLookup (qname);
    int semval = -1;


    semval = dts_semGetVal (dtsq->activeSem);
    switch (semval) {
    case QUEUE_RUNNING:
    case QUEUE_ACTIVE:
    case QUEUE_WAITING:
        dtsLog (dts, "{%s}: pauseQueue:  setting val=%d  sem=%d\n",
                qname, QUEUE_PAUSED, dtsq->activeSem);

        pthread_mutex_lock (&dtsq->mutex);
        dts_semSetVal (dtsq->activeSem, QUEUE_PAUSED);
        dtsq->status = QUEUE_PAUSED;
        pthread_mutex_unlock (&dtsq->mutex);
	/*  FALL-THROUGH  */

    case QUEUE_PAUSED:
        xr_setIntInResult (data, OK);     /* No useful result returned .... */
        if (qname)
            free ((void *) qname);
        return (OK);

    case QUEUE_SHUTDOWN:
    case QUEUE_RESPAWN:
    default:
        //Generate new thread.
        dtsLog (dts, "pauseQueue (%s): Ignored queue is shutdown or invalid\n",
	    qname);

        xr_setIntInResult (data, OK);     /* No useful result returned .... */
        if (qname)
                free ((void *) qname);
        return (OK);
    }
}


/**
 *  DTS_STOPQUEUE --  Stop processing of the named queue.
 *
 *  @brief	Stop processing of the named queue
 *  @fn		int dts_stopQueue (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_stopQueue (void *data)
{
    char *qname   = xr_getStringFromParam (data, 0); /* queue name	*/
    dtsQueue *dtsq = dts_queueLookup (qname);

	
    if (dts->verbose)
	dtsLog (dts, "stopQueue (%s):  setting val=%d  sem=%d\n",
 	    qname, QUEUE_PAUSED, dtsq->activeSem);

    pthread_mutex_lock (&dtsq->mutex);
    dts_semSetVal (dtsq->activeSem, QUEUE_PAUSED);
    dtsq->status = QUEUE_PAUSED;
    pthread_mutex_unlock (&dtsq->mutex);

    xr_setIntInResult (data, OK);       /* No useful result returned .... */
    if (qname)
	free ((void *) qname);
    return (OK);
}


/**
 *  DTS_GETQUEUESTAT --  Get the status flag of the named queue.
 *
 *  @brief	Get the status flag of the named queue.
 *  @fn		int dts_getQueueStat (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_getQueueStat (void *data)
{
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    dtsQueue *dtsq = dts_queueLookup (qname);

    pthread_mutex_lock (&dtsq->mutex);
    xr_setIntInResult (data, dts_semGetVal (dtsq->activeSem)); 
    pthread_mutex_unlock (&dtsq->mutex);

    if (qname)
	free ((void *) qname);
    return (OK);
}


/**
 *  DTS_SETQUEUESTAT --  Set the status flag of the named queue.
 *
 *  @brief	Set the status flag of the named queue.
 *  @fn		int dts_setQueueStat (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_setQueueStat (void *data)
{
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    int     val = xr_getIntFromParam (data, 1);    /* queue status	*/
    dtsQueue *dtsq = dts_queueLookup (qname);

    pthread_mutex_lock (&dtsq->mutex);
    dts_semSetVal (dtsq->activeSem, val); 
    pthread_mutex_unlock (&dtsq->mutex);

    xr_setIntInResult (data, OK);

    if (qname)
	free ((void *) qname);
    return (OK);
}


/**
 *  DTS_SETQUEUECOUNT --  Set the pending count of the named queue.
 *
 *  @brief	Set the pending count of the named queue.
 *  @fn		int dts_setQueueCount (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_setQueueCount (void *data)
{
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    int   count = xr_getIntFromParam (data, 1);    /* queue count	*/
    dtsQueue *dtsq = dts_queueLookup (qname);

    pthread_mutex_lock (&dtsq->mutex);
    dts_semSetVal (dtsq->countSem, count); 
    pthread_mutex_unlock (&dtsq->mutex);

    xr_setIntInResult (data, OK);

    if (qname)
	free ((void *) qname);
    return (OK);
}


/**
 *  DTS_GETQUEUECOUNT --  Get the pending count of the named queue.
 *
 *  @brief	Get the pending count of the named queue.
 *  @fn		int dts_getQueueCount (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_getQueueCount (void *data)
{
    int  count = -1;
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    dtsQueue *dtsq = dts_queueLookup (qname);

    pthread_mutex_lock (&dtsq->mutex);
    count = dts_semGetVal (dtsq->countSem);
    pthread_mutex_unlock (&dtsq->mutex);

    xr_setIntInResult (data, count);

    if (qname)
	free ((void *) qname);
    return (OK);
}


/**
 *  DTS_SETQUEUEDIR --  Set the queue deliveryDirectory
 *
 *  @brief	Set the queue deliveryDirectory
 *  @fn		int dts_setQueueDir (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_setQueueDir (void *data)
{
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    char *ddir  = xr_getStringFromParam (data, 0); /* delivery dir	*/
    dtsQueue *dtsq = dts_queueLookup (qname);

    pthread_mutex_lock (&dtsq->mutex);
    memset (dtsq->deliveryDir, 0, SZ_PATH);
    strcpy (dtsq->deliveryDir, ddir); 
    pthread_mutex_unlock (&dtsq->mutex);

    xr_setIntInResult (data, OK);

    if (qname) free ((void *) qname);
    if (ddir)  free ((void *) ddir);
    return (OK);
}


/**
 *  DTS_GETQUEUEDIR --  Get the queue delivery directory.
 *
 *  @brief	Get the queue delivery directory.
 *  @fn		int dts_getQueueDir (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int
dts_getQueueDir (void *data)
{
    char  ddir[SZ_PATH];
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    dtsQueue *dtsq = dts_queueLookup (qname);

    memset (ddir, 0, SZ_PATH);
    if (dtsq->deliveryDir[0])
        strcpy (ddir, dtsq->deliveryDir);

    xr_setStringInResult (data, ddir);

    if (qname) free ((void *) qname);
    return (OK);
}


/**
 *  DTS_SETQUEUECMD --  Set the queue deliveryCmd
 *
 *  @brief	Set the queue deliveryCmd
 *  @fn		int dts_setQueueCmd (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_setQueueCmd (void *data)
{
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    char *ddir  = xr_getStringFromParam (data, 0); /* delivery dir	*/
    dtsQueue *dtsq = dts_queueLookup (qname);

    pthread_mutex_lock (&dtsq->mutex);
    memset (dtsq->deliveryCmd, 0, SZ_PATH);
    strcpy (dtsq->deliveryCmd, ddir); 
    pthread_mutex_unlock (&dtsq->mutex);

    xr_setIntInResult (data, OK);

    if (qname) free ((void *) qname);
    if (ddir)  free ((void *) ddir);
    return (OK);
}


/**
 *  DTS_GETQUEUECMD --  Get the queue deliveryCmd.
 *
 *  @brief	Get the queue deliveryCmd.
 *  @fn		int dts_getQueueCmd (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int
dts_getQueueCmd (void *data)
{
    char  ddir[SZ_PATH];
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    dtsQueue *dtsq = dts_queueLookup (qname);

    memset (ddir, 0, SZ_PATH);
    if (dtsq->deliveryCmd[0])
        strcpy (ddir, dtsq->deliveryCmd);

    xr_setStringInResult (data, ddir);

    if (qname) free ((void *) qname);
    return (OK);
}


/**
 *  DTS_GETCOPYDIR --  Get the DTS copy directory.
 *
 *  @brief	Get the DTS copy directory.
 *  @fn		int dts_getCopyDir (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int
dts_getCopyDir (void *data)
{
    char  cdir[SZ_PATH];

    memset (cdir, 0, SZ_PATH);
strcpy (cdir, "/tmp");
    if (dts->self && dts->self->clientCopy[0])
        strcpy (cdir, dts->self->clientCopy);

    xr_setStringInResult (data, cdir);

    return (OK);
}


/**
 *  DTS_EXECCMD -- Execute the named command.
 *
 *  @brief	Execute the named command.
 *  @fn		int dts_execCmd (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int
dts_execCmd (void *data)
{
    char *ewd = xr_getStringFromParam (data, 0); /* effective working dir  */
    char *cmd = xr_getStringFromParam (data, 1); /* queue name	           */
    int   status = -1;

    status = dts_sysExec (ewd, cmd);
    xr_setIntInResult (data, status);

    if (cmd) free ((void *) cmd);
    if (ewd) free ((void *) ewd);
    return (OK);
}


/**
 *  DTS_LISTQUEUE -- List pending queue data.
 *
 *  @brief	List pending queue data.
 *  @fn		int dts_listQueue (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_listQueue (void *data)
{
    /* Not yet implemented */

    return (OK);
}


/**
 *  DTS_RESTARTQUEUE -- Simply stop, then restart, the queue.  We pass 
 *  through the calling params and don't need to interpret them here.
 *
 *  @brief	Simply stop, then restart, the queue
 *  @fn		int dts_restartQueue (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_restartQueue (void *data)
{
    if (dts_stopQueue (data) == ERR || dts_startQueue (data) == ERR)
	return (ERR);

    return (OK);
}


/**
 *  DTS_ADDTOTQUEUE --  Add the named object to the DTS queue.
 *
 *  @brief	Add the named object to the DTS queue
 *  @fn		int dts_addToQueue (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_addToQueue (void *data)
{
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    char *fname = xr_getStringFromParam (data, 1); /* file name		*/

    /* Not yet implemented */

    if (qname) free ((void *) qname);
    if (fname) free ((void *) fname);
    return (OK);
}


/**
 *  DTS_REMOVEFROMQUEUE -- Remove the named object from the DTS queue.
 *
 *  @brief	Remove the named object from the DTS queue
 *  @fn		int dts_removeFromQueue (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_removeFromQueue (void *data)
{
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    char *fname = xr_getStringFromParam (data, 1); /* file name		*/

    /* Not yet implemented */

    if (qname) free ((void *) qname);
    if (fname) free ((void *) fname);
    return (OK);
}


/**
 *  DTS_PRINTQUEUECFG -- Print queue configuration.
 *
 *  @brief	Print queue configuration.
 *  @fn		int dts_printQueueCfg (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_printQueueCfg (void *data)
{
    char *buf = calloc (1, SZ_MAX_MSG);
    char *qname = xr_getStringFromParam (data, 0); /* queue name	*/
    dtsQueue *dtsq = dts_queueLookup (qname);
    int  lo = dtsq->port;
    int  hi = dtsq->port + dtsq->nthreads;


    dtsShm (dtsq->dts);
    doprnt (buf, "Queue:  %s\n",       dtsq->name);
    doprnt (buf, "        src:  %s\n", dtsq->src);
    doprnt (buf, "       dest:  %s\n", dtsq->dest);
    doprnt (buf, "deliveryDir:  %s\n", dtsq->deliveryDir);
    doprnt (buf, "deliveryCmd:  %s\n", dtsq->deliveryCmd);
    doprnt (buf, "   transfer:  %s\n", dtsq->transfer);

    doprnt (buf, "     status:  %d\n", dtsq->status);
    doprnt (buf, "       type:  %s\n", dts_cfgQTypeStr(dtsq->type));
    doprnt (buf, "       mode:  %s\n", dts_cfgQModeStr(dtsq->mode));
    doprnt (buf, "     method:  %s\n", dts_cfgQMethodStr(dtsq->method));
    doprnt (buf, "   udt_rate:  %d\n", dtsq->udt_rate);
    doprnt (buf, "   nthreads:  %d\n", dtsq->nthreads);
    doprnt (buf, "      nerrs:  %d\n", dtsq->qstat->failedxfers);
    doprnt (buf, "       port:  %d -> %d\n", lo, hi);
    doprnt (buf, "\n");

    xr_setStringInResult (data, buf);

    if (qname) free ((void *) qname);
    if (buf)   free ((void *) buf);
    return (OK);
}




/******************************************************************************
 *	list <option> 		- list config parameters
 *	set <option> <value>	- set a specified option
 *	get <option>		- get value of option
 */


/**
 *  DTS_LIST -- List config parameters.
 *
 *  @brief	List configuration parameters
 *  @fn		int dts_List (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_List (void *data)
{
    char  val[SZ_MAX_MSG];
    char  *d = NULL, *q = NULL;
    char  *opt = xr_getStringFromParam (data, 0);   /* scope of request  */

    extern char *dts_printDTS(), *dts_printAllQueues();


    memset (val, 0, SZ_MAX_MSG);
    if (strcmp (opt, "dts") == 0) {
	d = dts_printDTS (dts, NULL); strcat (val,  d);

    } else if (strcmp (opt, "queue") == 0) {
	q = dts_printAllQueues (dts, NULL); strcat (val,  q);

    } else if (strcmp (opt, "all") == 0) {
	d = dts_printDTS (dts, NULL); strcat (val,  d);
	q = dts_printAllQueues (dts, NULL); strcat (val,  q);
    }

    xr_setStringInResult (data, val);			/* set result	*/

    if (d)   free ((void *) d);
    if (q)   free ((void *) q);
    if (opt) free ((void *) opt);

    return (OK);				   /* method succeeded	*/
}


/**
 *  DTS_SET -  Set an option value. 
 *
 *  @brief	Set and option value
 *  @fn		int dts_Set (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Set (void *data)
{
    char *who   = xr_getStringFromParam (data, 0); /* who's asking	*/
    char *class = xr_getStringFromParam (data, 1); /* scope of request  */
    char *what  = xr_getStringFromParam (data, 2); /* specifics	        */
    char *val   = xr_getStringFromParam (data, 3); /* new value         */


    if (strcmp (class, "dts") == 0) {
	/*  Commands available for DTS scope:
	**
	**	debug	    <1|0>	debug flag
	**	verbose	    <1|0>	verbose flag
	*/
	if (strcmp (what, "debug") == 0) {
	    dts->debug = atoi (val);

	} else if (strcmp (what, "verbose") == 0) {
	    dts->verbose = atoi (val);
	}

	if (dts->verbose)
	    dtsLog (dts, "SET cmd from '%s', %s %s %s", who, class, what, val);

    } else if (strcmp (class, "queue") == 0) {
	/*  Commands available for QUEUE scope:
	**
	**
	*/

    } else if (strcmp (class, "xfer") == 0) {
	/*  Commands available for XFER scope:
	**
	*/

    } 

    xr_setIntInResult (data, OK);       /* No useful result returned .... */

    if (who)   free ((char *) who);
    if (class) free ((char *) class);
    if (what)  free ((char *) what);
    if (val)   free ((char *) val);

    return (OK);
}


/**
 *  DTS_GET -  Get an option value. 
 *
 *  @brief	Get and option value
 *  @fn		int dts_Get (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_Get (void *data)
{
    char *class = xr_getStringFromParam (data, 0); /* scope of request  */
    char *what  = xr_getStringFromParam (data, 1); /* specifics	        */
    char  val[SZ_FNAME];


    memset (val, 0, SZ_FNAME);
    if (strcmp (class, "dts") == 0) {
	dtsClient *client = dts->self;

	if (strcmp (what, "debug") == 0) {
	    sprintf (val, "%d", dts->debug);
	} else if (strcmp (what, "verbose") == 0) {
	    sprintf (val, "%d", dts->verbose);

	} else if (strcmp (what, "copyMode") == 0) {
	    sprintf (val, "%s", dts->copyMode ? "copy" : "normal");

	} else if (strcmp (what, "clientUrl") == 0) {
	    sprintf (val, "%s", client->clientUrl);
	} else if (strcmp (what, "clientHost") == 0) {
	    sprintf (val, "%s", client->clientHost);
	} else if (strcmp (what, "clientDir") == 0) {
	    sprintf (val, "%s", client->clientDir);
	} else if (strcmp (what, "clientCopy") == 0) {
	    sprintf (val, "%s", client->clientCopy);
	} else if (strcmp (what, "clientIP") == 0) {
	    sprintf (val, "%s", client->clientIP);
	} else if (strcmp (what, "clientName") == 0) {
	    sprintf (val, "%s", client->clientName);
	} else if (strcmp (what, "clientNet") == 0) {
	    sprintf (val, "%s", client->clientNet);

	} else if (strcmp (what, "build_date") == 0) {
	    sprintf (val, "%s", dts->build_date);
	}

    } else if (strcmp (class, "queue") == 0) {
	char  *qname  = xr_getStringFromParam (data, 2);
	dtsQueue *dtsq = dts_queueLookup (qname);

	if (dtsq) {
	    if (strcmp (what, "deliveryDir") == 0) {
	        sprintf (val, "%s", dtsq->deliveryDir);
	    } else if (strcmp (what, "deliveryCmd") == 0) {
	        sprintf (val, "%s", dtsq->deliveryCmd);
	    } else if (strcmp (what, "src") == 0) {
	        sprintf (val, "%s", dtsq->src);
	    } else if (strcmp (what, "dest") == 0) {
	        sprintf (val, "%s", dtsq->dest);
	    } else if (strcmp (what, "transfer") == 0) {
	        sprintf (val, "%s", dtsq->transfer);
	    }
	}

        if (qname)  free ((char *) qname);

    } else if (strcmp (class, "xfer") == 0) {
	;

    } 

    xr_setStringInResult (data, val);			/* set result	*/

    if (class) free ((char *) class);
    if (what)  free ((char *) what);

    return (OK);
}




/*******************************************************************************
 *	queueAccept 	    - See if queue will accept a new object
 *	queueValid 	    - See if queue name is valid
 *  	queueSetControl	    - Set control file for transfer
 *  	queueUpdateStats    - Update queue transfer stats
 */


/**
 *  DTS_QUEUEACCEPT - See if queue will accept a new object.
 *
 *  @brief	See if queue will accept a new object.
 *  @fn		int dts_queueAccept (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_queueAccept (void *data)
{
    struct statfs fs;
    int   i, stat = 0, res = 0, valid = 0;
    char *qname = xr_getStringFromParam (data, 0);
    int   fsize = xr_getIntFromParam (data, 1);
    char  *dir, resp[SZ_LINE];


    memset (resp, 0, SZ_LINE);

    /* Check that this is a valid queue name.
    */
    for (i=0; i < dts->nqueues; i++) {
	if (strcmp (qname, dts->queues[i]->name) == 0) {
	    valid++;
	    break;
	}
    }
    if (!valid) {
	sprintf (resp, "Error(queueAccept): Invalid queue name '%s'", qname);
        xr_setStringInResult (data, "Error(queueAccept): Invalid queue name");
        dtsLog (dts, resp);
        goto err_ret;
    }

    /* Get the available disk space.
    */
    dir = dts_getQueuePath (dts, qname);
    if ((res = statfs (dir, &fs)) < 0) {
	sprintf (resp, "Error: Cannot statfs(%s): %s", dir, strerror (errno));
        xr_setStringInResult (data, resp);
	goto err_ret;
    }

    if (fs.f_bsize > 0) {
        if ((fsize / fs.f_bsize) > (fs.f_bavail * fs.f_bsize) ) {
	    sprintf (resp, "Error: Insufficient disk space %ld < %ld",
		(long) (fsize / fs.f_bsize), (long) (fs.f_bavail * fs.f_bsize));
	    dtsLog (dts, resp);
	    stat = ERR;
        } else {
	    strcpy (resp, dts_getNextQueueDir (dts, qname));
	    stat = OK;
        }
    } else {
	sprintf (resp, "Error: statfs() return zero blocksize on %s", dir);
	stat = OK;
    }
    xr_setStringInResult (data, resp);

    if (dts->verbose) dtsLog (dts, "qAccept: '%s'", resp);

err_ret:
    if (qname) free ((char *) qname);

    return (OK);
}


/**
 *  DTS_QUEUEVALID - See if queue name is valid on the host.
 *
 *  @brief	See if queue name is valid on the host.
 *  @fn		int dts_queueValid (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_queueValid (void *data)
{
    int   i, valid = ERR;
    char *qname = xr_getStringFromParam (data, 0);


    /* Check that this is a valid queue name.
    */
    if (strcmp (qname, "default") == 0)
	valid = OK;
    else {
        for (i=0; i < dts->nqueues; i++) {
	    if (strcmp (qname, dts->queues[i]->name) == 0) {
	        valid = OK;
	        break;
	    }
        }
    }

    xr_setIntInResult (data, (int) valid);		/* set result	*/

    if (dts->verbose) dtsLog (dts, "qValid: %d", valid);

    if (qname) free ((char *) qname);

    return (OK);
}


/**
 *  DTS_QUEUEDEST -- Get the destination of the named queue.
 *
 *  @brief	Get the destination of the named queue.
 *  @fn		int dts_queueDest (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_queueDest (void *data)
{
    register int  i;
    char *qname = xr_getStringFromParam (data, 0);
    dtsQueue *dtsq = (dtsQueue *) NULL;


    /* Search for the requested queue name.
     */
    for (i=0; i < dts->nqueues; i++) {
	if (strcmp (qname, dts->queues[i]->name) == 0) {
	    dtsq = dts->queues[i];
	    break;
	}
    }

    if (dts->verbose > 2) 
	dtsLog (dts, "destTrace: %s", (dtsq ? dtsq->dest : "error"));

    if (dtsq)
        xr_setStringInResult (data, 
	    (dtsq->node == QUEUE_ENDPOINT ? "end" : dtsq->dest));
    else
        xr_setStringInResult (data, "invalid queue");

    if (qname) free ((char *) qname);

    return (OK);
}


/**
 *  DTS_QUEUESRC -- Get the source of the named queue.
 *
 *  @brief	Get the source of the named queue.
 *  @fn		int dts_queueSrc (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_queueSrc (void *data)
{
    register int  i;
    char *qname = xr_getStringFromParam (data, 0);
    dtsQueue *dtsq = (dtsQueue *) NULL;


    /* Search for the requested queue name.
     */
    for (i=0; i < dts->nqueues; i++) {
	if (strcmp (qname, dts->queues[i]->name) == 0) {
	    dtsq = dts->queues[i];
	    break;
	}
    }

    if (dts->verbose > 2) 
	dtsLog (dts, "srcTrace: %s", (dtsq ? dtsq->src : "error"));

    if (dtsq)
        xr_setStringInResult (data, 
	    (dtsq->node == QUEUE_INGEST ? "start" : dtsq->src));
    else
        xr_setStringInResult (data, "invalid queue");

    if (qname) free ((char *) qname);

    return (OK);
}


/**
 *  DTS_QUEUECOMPLETE - Complete transfer of a file.
 *
 *  @brief	Complete transfer of a file.
 *  @fn		int dts_queueComplete (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_queueComplete (void *data)
{
    int   i, valid = ERR;
    char *qname = xr_getStringFromParam (data, 0);
    char *qpath = xr_getStringFromParam (data, 1);
    char *qp    = dts_sandboxPath (qpath);
    char  spath[SZ_LINE], cpath[SZ_LINE];
    FILE *fd = (FILE *) NULL;


    /* Check that this is a valid queue name.
    */
    if (strcmp (qname, "default") == 0)
	valid = OK;
    else {
        for (i=0; i < dts->nqueues; i++) {
	    if (strcmp (qname, dts->queues[i]->name) == 0) {
	        valid = OK;
	        break;
	    }
        }
    }

    /*  Set the transfer status.
     */
    memset (spath, 0, SZ_LINE);
    sprintf (spath, "%s/_status", qp);
    if ((fd = fopen (spath, "w+")) != (FILE *) NULL) {
	flock (fileno(fd), LOCK_EX);
        fprintf (fd, "transfer complete\n");
	fflush (fd);
	flock (fileno(fd), LOCK_UN);
        fclose (fd);
    } else
        valid = ERR;

    /*  Validate the file checksum.
     */
    memset (cpath, 0, SZ_LINE);
    sprintf (cpath, "%s/_control", qp);
    if ((fd = fopen (cpath, "r")) != (FILE *) NULL) {
	char line[SZ_LINE], fname[SZ_LINE], md5[SZ_LINE], fpath[SZ_LINE];
	unsigned int sum32 = -1, crc = -1, valid = 0;


	memset (fname, 0, SZ_LINE);
	memset (md5,   0, SZ_LINE);
	while (dtsGets (line, SZ_LINE, fd)) {
	    if (strncmp (line, "fname", 5) == 0) {
		sscanf (line, "fname     = %s", fname);
		memset (fpath, 0, SZ_LINE);
    		sprintf (fpath, "%s/%s", qp, fname);
	    } else if (strncmp (line, "crc32", 5) == 0) {
		sscanf (line, "crc32     = %d", &crc);
	    } else if (strncmp (line, "sum32", 5) == 0) {
		sscanf (line, "sum32     = %d", &sum32);
	    } else if (strncmp (line, "md5", 3) == 0) {
		sscanf (line, "md5       = %s", md5);
	    }
	}
        fclose (fd);

	/* Validate the file against whatever checksums we were
	 * given in the control file.
	 */
	valid = dts_fileValidate (fpath, sum32, crc, md5);

    } else
        valid = ERR;

    xr_setIntInResult (data, (int) valid);		/* set result	*/

    if (dts->verbose) dtsLog (dts, "qComplete: %d", valid);

    if (qname) free ((char *) qname);
    if (qpath) free ((char *) qpath);
    if (qp)    free ((char *) qp);

    return (OK);
}


/**
 *  DTS_QUEUERELEASE -- Release lock on queue semaphore.
 *
 *  @brief	Release lock on queue semaphore.
 *  @fn		int dts_queueRelease (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 *
 */
int 
dts_queueRelease (void *data)
{
    char  *qname = xr_getStringFromParam (data, 0);
    dtsQueue *dtsq;


    if (qname) {
        /*  Get the parameters for the current queue.
         */
        if ((dtsq = dts_queueLookup (qname)))
            dts_semSetVal (dtsq->activeSem, QUEUE_ACTIVE);

        xr_setIntInResult (data, (int) OK);		/* set result	*/
	free ((void *) qname);
    }

    return (OK);
}


/**
 *  DTS_QUEUESETCONTROL - Create the control file for the transfer.
 *
 *  @brief	Create the control file for the transfer.
 *  @fn		int dts_queueSetControl (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_queueSetControl (void *data)
{
    FILE  *fd;
    char  *qPath, *qHost, *qName, *fileName, *xferName, *dfname;
    char  *srcpath, *igstpath, *md5, ctrl_fname[SZ_FNAME];
    unsigned int  isDir, fileSize, sum32, crc32, epoch;
    unsigned int  i, ifd, pars, npars;
    int    status = OK, snum=0;
    char  *param = calloc (1, SZ_FNAME), 
	  *val   = calloc (1, SZ_FNAME),
	  *qp    = NULL;
    dtsQueue *dtsq = (dtsQueue *) NULL;;


    /* Get the RPC arguments to local variables.
    */
    qPath     = xr_getStringFromParam (data, 0);
    qHost     = xr_getStringFromParam (data, 1);
    qName     = xr_getStringFromParam (data, 2);
    fileName  = xr_getStringFromParam (data, 3);
    xferName  = xr_getStringFromParam (data, 4);
    srcpath   = xr_getStringFromParam (data, 5);
    igstpath  = xr_getStringFromParam (data, 6);
    md5       = xr_getStringFromParam (data, 7);
    dfname    = xr_getStringFromParam (data, 8);
    isDir     = xr_getIntFromParam    (data, 9);
    fileSize  = xr_getIntFromParam    (data, 10);
    sum32     = xr_getIntFromParam    (data, 11);
    crc32     = xr_getIntFromParam    (data, 12);
    epoch     = xr_getIntFromParam    (data, 13);
    pars      = xr_getArrayFromParam  (data, 14); 	/* param array */

    if (dts->debug > 2) {
	fprintf (stderr, "setCtrl: qPath='%s' qHost='%s' qName='%s'\n",
			      qPath, qHost, qName);
	fprintf (stderr, "         filename='%s' srcpath='%s'\n",
	    fileName, srcpath);
	fprintf (stderr, "         xferName='%s'\n", xferName);
	fprintf (stderr, "         igstpath='%s'\n", igstpath);
	fprintf (stderr, "         dir=%d md5='%s' sum32=%d\n",isDir,md5,sum32);
	fprintf (stderr, "         epoch=%d fileSize=%d\n", epoch, fileSize);
    }


    /* Create the control file.  Not sure why it would happen, but if
     * we're called more than once remove an existing control.
     */
    qp = dts_sandboxPath ("/");
    sprintf (ctrl_fname, "%s/%s/_control", qp, qPath);
    if (access (ctrl_fname, F_OK) == 0) 
	unlink (ctrl_fname);
    if ((ifd = creat (ctrl_fname, DTS_FILE_MODE)) > 0)
	close (ifd);
    if (qp) free ((void *) qp);


    /*  Save the incoming filename to the queue structure.
     */
    if (qName && (dtsq = dts_queueLookup (qName))) {
        dtsShm (dts);
        memset (dtsq->qstat->infile, 0, SZ_PATH);
        strcpy (dtsq->qstat->infile, xferName);
    }


    /*  Write the control file.
     */
    if ((fd = fopen (ctrl_fname, "w+")) == (FILE *) NULL)  {
	status = ERR;
	dtsLog (dts, "%6.6s <  XFER: Error: cannot open control file='%s'", 
	    (dtsq? dts_queueNameFmt (dtsq->name) : " "), ctrl_fname);

    } else {

	flock (fileno(fd), LOCK_EX);
        fprintf (fd, "qname        = %s\n", qName);
        fprintf (fd, "fname        = %s\n", fileName);
        fprintf (fd, "xfername     = %s\n", xferName);
        fprintf (fd, "srcpath      = %s\n", srcpath    );
        fprintf (fd, "path         = %s\n", igstpath);
        fprintf (fd, "fsize        = %d\n", fileSize);
        fprintf (fd, "sum32        = %u\n", sum32);
        fprintf (fd, "crc32        = %u\n", crc32);
        fprintf (fd, "md5          = %s\n", md5);
        fprintf (fd, "qpath        = %s\n", qPath);
        fprintf (fd, "qhost        = %s\n", qHost);
        fprintf (fd, "isDir        = %d\n", isDir);
        fprintf (fd, "epoch        = %d\n", epoch);
        fprintf (fd, "deliveryName = %s\n", dfname);

         /* Save the params.
          */
        npars = xr_arrayLen (pars);
        for (i=0; i < npars; i++) {
	    memset (param, 0, SZ_FNAME);
	    memset (val,   0, SZ_FNAME);

	    xr_getStructFromArray (pars, i, &snum);
	    xr_getStringFromStruct (snum, "p", &param);
	    xr_getStringFromStruct (snum, "v", &val);
	    fprintf (fd, "%-9s = %s\n", param, val);
        }
        fflush (fd);
	flock (fileno(fd), LOCK_UN);
        fclose (fd);
    }


    xr_setIntInResult (data, (int) status);		/* set result	*/
    if (dts->verbose > 2) 
	dtsLog (dts, "%6.6s <  XFER: control file: status=%d", 
	    (dtsq ? dts_queueNameFmt (dtsq->name) : " "), status);

    if (qPath)    free ((char *) qPath);
    if (qHost)    free ((char *) qHost);
    if (qName)    free ((char *) qName);
    if (fileName) free ((char *) fileName);
    if (xferName) free ((char *) xferName);
    if (srcpath)  free ((char *) srcpath);
    if (igstpath) free ((char *) igstpath);
    if (md5)      free ((char *) md5);
    if (dfname)   free ((char *) dfname);

    if (val)      free ((char *) val);
    if (param)    free ((char *) param);
    xr_freeArray (pars);

    return (status);
}


/**
 *  DTS_QUEUEUPDATESTATS -- Update queue transfer statistics.
 *
 *  @brief	Update queue transfer statistics.
 *  @fn		int dts_queueUpdateStats (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 *
 */
int 
dts_queueUpdateStats (void *data)
{
    char  *qname = xr_getStringFromParam (data, 0);
    dtsQueue *dtsq;
    xferStat  xfs;


    if (qname) {
        /*  Get the parameters for the current queue.
         */
        if ((dtsq = dts_queueLookup (qname))) {

	    memset (&xfs, 0, sizeof (xferStat));
	    xfs.fsize   = xr_getIntFromParam (data, 1);
	    xfs.tput_mb = xr_getDoubleFromParam (data, 2);
	    xfs.time    = xr_getDoubleFromParam (data, 3);

	    dts_queueSetStats (dtsq, &xfs);
	}

        xr_setIntInResult (data, (int) OK);		/* set result	*/
	free ((void *) qname);
    }

    return (OK);
}




/*******************************************************************************
 *	initTransfer <method>	- initialize an object transfer
 *	doTransfer		- transfer the actual file
 *	endTransfer		- end the object transfer
 *	cancelTransfer		- abort an in-progress transfer
 */

/**
 *  DTS_INITTRANSFER -- Initialize a transfer operation.
 *
 *  @brief	Initialize a transfer operation.
 *  @fn		int dts_initTransfer (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 *
 */
int 
dts_initTransfer (void *data)
{
    struct statfs fs;
    int   i, stat = 0, res = 0, valid = 0;
    char *qname = xr_getStringFromParam (data, 0);
    int   fsize = xr_getIntFromParam (data, 1);
    char  *dir, resp[SZ_LINE];
    dtsQueue *dtsq;
    int   semval = -1;
 

    memset (resp, 0, SZ_LINE);

    /*  Get the parameters for the current queue.
     */
    
    dtsq = dts_queueLookup (qname);
    semval = dts_semGetVal (dtsq->activeSem);
/*  dts_queueLock (dtsq); */

    gettimeofday (&dtsq->init_time, NULL);
    dts_queueSetInitTime (dtsq, dtsq->init_time);

    /* Check that this is a valid queue name and status.
    */
    for (i=0; i < dts->nqueues; i++) {
	if (strcmp (qname, dts->queues[i]->name) == 0 &&
           ((semval == QUEUE_RUNNING) ||
            (semval == QUEUE_ACTIVE) ||
            (semval == QUEUE_PAUSED)) ) {
		//  FIXME -- Possible bug didn't get all the correct statuses?
	        valid++;
	        break;
	}
    }
    if (!valid) {
	sprintf (resp, "Error(initTransfer): Invalid queue name '%s'", qname);
        xr_setStringInResult (data, "Error(initTransfer): Invalid queue name");
        if (qname) free ((char *) qname);
        dtsLog (dts, resp);

        return (OK);
    }

    /* Get the available disk space.
    */
    dir = dts_getQueuePath (dts, qname);
    if (PERF_DEBUG) 
	dtsLog (dts, "%6.6s <  XFER: init: dir '%s'", 
	    dts_queueNameFmt (qname), dir);
    if ((res = statfs (dir, &fs)) < 0) {
	sprintf (resp, "Error: Cannot statfs() %s", dir);
        xr_setStringInResult (data, resp);
	goto err_ret;
    }

    if (STAT_DEBUG) {
	fprintf (stderr, "dir = '%s'  fsize=%d\n", dir, fsize);
	fprintf (stderr, "f_bsize=%ld  f_bavail=%ld  f_bfree=%ld\n", 
	    (long)fs.f_bsize, (long)fs.f_bavail, (long)fs.f_bfree);
    }

    if (fs.f_bsize > 0) {
        if (((float)fsize / (float)fs.f_bsize) > ((float)fs.f_bavail * (float)fs.f_bsize) ) {
	    sprintf (resp, 
		"initTransfer: Insufficient disk space on '%s' %ld < %ld",
		    dir, (long) ((float)fsize / (float)fs.f_bsize), 
		    (long) ((float)fs.f_bavail * (float)fs.f_bsize));
	    dtsLog (dts, resp);
	    stat = ERR;
        } else {
            if (PERF_DEBUG) 
		dtsLog (dts, "%6.6s <  XFER: init: getting next queue dir", 
		    dts_queueNameFmt (qname));
	    strcpy (resp, dts_getNextQueueDir (dts, qname));
            if (PERF_DEBUG) 
		dtsLog (dts, "%6.6s <  XFER: init: got next dir '%s'",
		    dts_queueNameFmt (qname), resp);
	    stat = OK;
        }
    } else {
	sprintf (resp, "Error: statfs() return zero blocksize on %s", dir);
    }
    xr_setStringInResult (data, resp);


err_ret:

    dts_qstatInit (qname, NULL, (size_t) fsize);
    dts_qstatStart (qname);
    if (dts->verbose) 
	dtsLog (dts, "%6.6s <  XFER: init: '%s'", 
	    dts_queueNameFmt (qname), resp);

    if (qname) free ((char *) qname), qname = NULL;

/*  dts_queueUnlock (dtsq); */

    return (OK);
}


/**
 *  DTS_ENDTRANSFER -- Clean up and terminate a transfer operation.
 *
 *  @brief	Clean up and terminate a transfer operation
 *  @fn		int dts_endTransfer (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 *
 */
int 
dts_endTransfer (void *data)
{
    int    i, valid = ERR, key = 0, stat = OK, sec, usec;
    char  *qname = xr_getStringFromParam (data, 0);
    char  *qpath = xr_getStringFromParam (data, 1);
    char  *qp    = dts_sandboxPath (qpath), *cqp = NULL;
    char   spath[SZ_LINE], fpath[SZ_LINE], cpath[SZ_LINE];
    Control *ctrl = (Control *) NULL;
    static Control cdata;
    FILE  *fd = (FILE *) NULL;
    dtsQueue *dtsq;
    xferStat  xfs;

    struct timeval  t1, t2, t3;


    gettimeofday (&t1, NULL);

    /*  Get the parameters for the current queue.
     */
    dtsq = dts_queueLookup (qname);
    /* dts_queueLock (dtsq); */
    
    /*  Check that this is a valid queue name.
    */
    if (strcmp (qname, "default") == 0)
	valid = OK;
    else {
        for (i=0; i < dts->nqueues; i++) {
	    if (strcmp (qname, dts->queues[i]->name) == 0) {
	        valid = OK;
	        break;
	    }
        }
    }

    /*  Set the transfer status.
     */
    memset (spath, 0, SZ_LINE);
    sprintf (spath, "%s/_status", qp);
    if ((fd = fopen (spath, "w+")) != (FILE *) NULL) {
	flock (fileno(fd), LOCK_EX);
        fprintf (fd, "transfer complete\n");
	fflush (fd);
	flock (fileno(fd), LOCK_UN);
        fclose (fd);
    } else {
	dtsErrLog (dtsq, "Cannot open _status file.");
        valid = ERR;
    }

    /*  Load the control file.
     */
    memset (cpath, 0, SZ_LINE);
    sprintf (cpath, "%s/_control", qp);
    if (! (ctrl = dts_loadControl (cpath, &cdata))) {
	dtsErrLog (dtsq, "Cannot load control file.");
	valid = ERR;
    }
    strcpy (ctrl->queueName, qname);


    /*  Validate the file against whatever checksums we were
     *  given in the control file.
     */
    cqp = dts_sandboxPath (ctrl->queuePath);
    sprintf (fpath, "%s%s", cqp, ctrl->xferName);
    dts_qstatSetFName (qname, ctrl->xferName);
    valid = dts_fileValidate (fpath, ctrl->sum32, ctrl->crc32, ctrl->md5);
    free ((void *) cqp);

    gettimeofday (&t2, NULL);

    /*  Do the post-transfer processing.
     */
    key = 0;
    if (valid == OK) {
	char  *emsg = NULL, lockfile[SZ_PATH], spoolDir[SZ_PATH];

	/*  Deliver the object.
	 */
	switch (dtsq->node) {
	case QUEUE_INGEST:
	    if (strcmp (dtsq->deliveryDir, "spool") == 0) {
	        memset (dtsq->deliveryDir, 0, SZ_PATH);
	        strcpy (dtsq->deliveryDir, qp);
	    }

	    dts_qstatDiskStart (qname);
	    if ((emsg = dts_Ingest (dtsq, ctrl, fpath, cpath, &stat))) {
		dtsErrLog (dtsq, "Ingest failure: '%s'\n", emsg);
	    } else {
                memset (spoolDir, 0, SZ_PATH);
                sprintf (spoolDir, "%s/%s", dts->serverRoot, ctrl->queuePath);
	        if (!emsg && strcmp (spoolDir, dtsq->deliveryDir) != 0)
	            emsg = dts_Deliver (dtsq, ctrl, fpath, &stat);

	        if (dts_semIncr (dtsq->countSem) < 0 && dts->verbose > 1) 
		    dtsErrLog (dtsq, "Error: ingest countSem incr failed\n"); 
	    }
	    dts_qstatDiskEnd (qname);
	    break;

	case QUEUE_TRANSFER:
	    /* FIXME -- need to handle queue transfer/tee
	     */
#ifdef USE_DTS_DB
	    key = dts_dbNewEntry (dtsq->name, dtsq->src, dtsq->dest,
		ctrl->igstPath, ctrl->queuePath, ctrl->fsize, ctrl->md5);
#endif

	    dts_qstatDiskStart (qname);
	    if ((emsg = dts_Deliver (dtsq, ctrl, fpath, &stat)) == NULL) {
	        if (dts_semIncr (dtsq->countSem) < 0 && dts->verbose > 1) 
		    dtsErrLog (dtsq, "Error: transfer countSem incr failed\n"); 
	    } else
		dtsErrLog (dtsq, "Error: Delivery failure: '%s'\n", emsg); 
#ifdef USE_DTS_DB
	    dts_dbSetTime (key, DTS_TEND);
#endif
	    dts_qstatDiskEnd (qname);
	    break;

	case QUEUE_ENDPOINT:
#ifdef USE_DTS_DB
	    key = dts_dbNewEntry (dtsq->name, dtsq->src, dtsq->dest, 
		ctrl->igstPath, ctrl->queuePath, ctrl->fsize, ctrl->md5);
#endif
	    dts_qstatDiskStart (qname);
	    emsg = dts_Deliver (dtsq, ctrl, fpath, &stat);
	    dts_qstatDiskEnd (qname);
#ifdef USE_DTS_DB
	    dts_dbSetTime (key, DTS_TIME_OUT);
#endif
            if (emsg == NULL) {
		char curfil[SZ_PATH];
		int  current;

		/*  Increment the value in the current file.
		 */
        	memset (curfil,   0, SZ_PATH);
        	sprintf (curfil, "%s/spool/%s/current", 
		    dts->serverRoot, dtsq->name);
        	current = dts_queueGetCurrent (curfil);

                dts_queueSetCurrent (curfil, ++current);
	    }

    	    if (dtsq->auto_purge)
       		dts_queueDelete (dts, qp);
	    break;
	}
	if (emsg != NULL) {
	    valid = ERR;
    	    if (dts->verbose > 1)
		dtsLog (dts, "ERROR: %s\n", emsg);
	}

	/*  Remove the lockfile on the queue directory.
	 */
	memset (lockfile, 0, SZ_PATH);
	sprintf (lockfile, "%s/_lock", qp);
	if (access (lockfile, F_OK) == 0) {
	    if (unlink (lockfile) < 0) {
	        dtsLog (dts, "%6.6s <  XFER: cannot remove lock file %s\n", 
		    dts_queueNameFmt (qname), lockfile);
	        valid = ERR;
	    }
	}


    } else {
	dtsLog (dts, "%6.6s <  XFER: file=%s   status=%s %s",
	    dts_queueNameFmt (qname), fpath, 
	    "ERROR", " transfer checksum failed");
    }

    gettimeofday (&t3, NULL);

    if (dts->verbose) {
        if (dts->verbose > 2) {
	    dtsLog (dts, 
		"%6.6s <  XFER: deliver times:  validate=%gs deliver=%ds",
		dts_queueNameFmt (qname), 
		dts_timediff (t1, t2), dts_timediff (t1, t3));
	    dtsLog (dts, "%6.6s <  XFER: deliver stat=%d", 
		dts_queueNameFmt (qname), stat);
	}

	dts_qstatDlvrStat (qname, stat);
	dts_qstatEnd (qname);

        dts_qstatSummary (dts, qname);

	dtsLog (dts, "%6.6s <  XFER: end file=%s   status=%s %s", 
	    dts_queueNameFmt (qname), fpath,
	    (valid ? "ERROR" : "OK"), " <<<<<<<<<<<<<<<");
    }

    if (valid == ERR || stat == ERR) {
	dtsLog (dts, "endTransfer:  valid=%s  stat=%s\n", 
	    ((valid == ERR) ? "ERR" : "OK"), ((stat == ERR) ? "ERR" : "OK"));
    }
    xr_setIntInResult (data, (int) (valid || stat));	/* set result	*/

    gettimeofday (&dtsq->end_time, NULL);
    dts_queueGetInitTime (dtsq, &sec, &usec);
    dtsq->init_time.tv_sec = sec;
    dtsq->init_time.tv_usec = usec;

    xfs.fsize = ctrl->fsize;
    xfs.time = (float) dts_timediff (dtsq->init_time, dtsq->end_time);
    xfs.sec =  (int) xfs.time;
    xfs.usec = (int) ((xfs.time - (int) xfs.time) * 1000000.0);
    xfs.tput_mb = transferMb (xfs.fsize, xfs.sec, xfs.usec);
    xfs.tput_MB = transferMB (xfs.fsize, xfs.sec, xfs.usec);
    dts_logXFerStats (ctrl, &xfs, valid, dts_getQueueLog(dts, qname, "log.in"));

    /* dts_queueUnlock (dtsq); */

    /*  Remove the lockfile on the queue directory.
#define ALWAYS_UNLOCK
     */
    if (stat == OK) {
#ifdef ALWAYS_UNLOCK
        char  lockfile[SZ_PATH];

        memset (lockfile, 0, SZ_PATH);
        sprintf (lockfile, "%s/_lock", qp);
        if (access (lockfile, F_OK) == 0) {
	    if (unlink (lockfile) < 0) {
	        dtsLog (dts, "%6.6s <  XFER: cannot remove lock file %s\n", 
		    dts_queueNameFmt (qname), lockfile);
	    }
        }
#else
	;
#endif
    } else {
	        
	dtsLog (dts, "%6.6s <  XFER: WARNING: leaving lock file in %s\n", 
		    dts_queueNameFmt (qname), qp);
    }



    if (qname) free ((char *) qname);
    if (qpath) free ((char *) qpath);
    if (qp)    free ((void *) qp);	/* free sandbox pathname ptr	*/

    /*  FIXME -- 
    if (dtsq->auto_purge)
       	dts_queueDelete (dts, qp);
     */

    return (OK);
}


/**
 *  DTS_DOTRANSFER -- Do the actual transfer of an object.
 *
 *  @brief	Do the actual transfer of an object.
 *  @fn		int dts_doTransfer (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 *
 */
int 
dts_doTransfer (void *data)
{
    /* Not yet implemented */

    return (OK);
}


/**
 *  DTS_CANCELTRANSFER -- Cancel an active file transfer.
 *
 *  @brief	Cancel an active file transfer
 *  @fn		int dts_cancelTransfer (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_cancelTransfer (void *data)
{
    /* Not yet implemented */

    return (OK);
}

/***************************************************************************
*********    Test Methods					   *********
***************************************************************************/


/**
 *  DTS_PING -- Simple aliveness test function.
 *
 *  @brief	Is host and DTS alive.
 *  @fn 	int dts_Ping (void *data)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
int 
dts_testFault (void *data)
{
    char  *s = (char *) NULL;

    strcpy (s, "SEGFAULT");


    /* We don't use the caller data, if we got this far we're alive so
    ** set a positive result.
    */
    xr_setIntInResult (data, (int) OK);

    if (dts->verbose > 3) dtsLog (dts, "FAULT: Shouldn't be here ....");
    return (OK);
}
