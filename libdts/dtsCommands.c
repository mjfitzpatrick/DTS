/**
 *  DTSCOMMANDS.C -- Client-callable DTS commands.
 *
 *  @brief      Client-callable DTS commands.
 *
 *  @file       dtsCommands.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/15/09
 */
/*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>

#ifdef Linux
#include <sys/vfs.h>                            /* for statfs()         */
#else
#ifdef Darwin
#include <sys/param.h>                          /* for statfs()         */
#include <sys/mount.h>
#endif
#endif



#include "dts.h"
#include "dtsPSock.h"


static char *url	= (char *) NULL;
static char *localhost 	= (char *) NULL;
#ifdef USE_CLEVEL
static int   clevel	= 0;
#endif

extern  DTS *dts;
extern  int  dts_monitor;


#define	DEBUG		CMD_DEBUG



/*  Externally used methods.
 */
void  dts_cmdInit();
int   dts_getClient (char *host);



/*
 *   ****  File Utility Methods  ****
 *
 *      access <path> [mode]				dts_hostAccess
 *      cat <fname>            				dts_hostCat
 *      copy <old> <new> 				dts_hostCopy     **
 *      cwd             				dts_hostCwd
 *      del <path> <passwd> 				dts_hostDel
 *      dir [path] 					dts_hostDir
 *      isdir [path] 					dts_hostIsDir
 *      diskFree <path>        				dts_hostDiskFree
 *      diskUsed <path>        				dts_hostDiskUsed
 *      echo <str>	        			dts_hostEcho
 *      fsize <path>           				dts_hostFSize
 *      fmode <path>           				dts_hostFMode
 *      ftime <path>           				dts_hostFTime
 *      fget <remote> <local> <blksz>			dts_hostFGet
 *      fput <local> <remote> <blksz>			dts_hostFPut
 *      mkdir <path>           				dts_hostMkdir
 *      ping             				dts_hostPing
 *      pingstr             				dts_hostPingStr
 *      pingarray             				dts_hostPingArray
 *      pingsleep             				dts_hostPingSleep
 *      rename <old> <new> 				dts_hostRename
 *      stat <path> 					dts_hostStat
 *      setroot <path> 					dts_hostSetRoot
 *      setDbg <flag> 					dts_hostSetDbg
 *      touch <path> 					dts_hostTouch
 *
 *  ****  File Transfer Methods  ****
 *
 *      push <path> [mode]				dts_hostTo
 *      pull <path> [mode]				dts_hostFrom
 *      give <path> [mode]				dts_hostTo
 *      take <path> [mode]				dts_hostFrom
 *
 *
 *  ****  Low Level I/O Methods  ****
 *
 *      read  <path> <offset> <nbytes>       		dts_hostRead     **
 *      write <path> <offset> <nbytes> <data>		dts_hostWrite    **
 *      stat  <path> 					dts_hostStat
 *      prealloc <path> <nbytes> 			dts_hostPrealloc
 *
 *
 *  ****  Queue Methods  ****
 *
 *	dest						dts_hostDest
 *	src 						dts_hostSrc
 *	startQueue 					dts_hostStartQueue
 *	stopQueue 					dts_hostStopQueue
 *	shutdownQueue 					dts_hostShutdownQueue
 *	pauseQueue 					dts_hostPauseQueue
 *	pokeQueue 					dts_hostPokeQueue
 *	flushQueue 					dts_hostFlushQueue
 *	listQueue 					dts_hostListQueue
 *	addToQueue 					dts_hostAddToQueue
 *	delFromQueue 					dts_hostDelFromQueue
 *	setQueueCount 					dts_hostSetQueueCount
 *	getQueueCount 					dts_hostGetQueueCount
 *	setQueueDir 					dts_hostSetQueueDir
 *	getQueueDir 					dts_hostGetQueueDir
 *	setQueueCmd 					dts_hostSetQueueCmd
 *	getQueueCmd 					dts_hostGetQueueCmd
 *	getCopyDir 					dts_hostCopyDir
 *	execCmd 					dts_hostExecCmd
 *
 *
 *  ****  DTS Methods  ****
 *
 *	abort						dts_hostAbort
 *	shutdown					dts_hostShutdown
 *	contact						dts_hostContact
 *	set						dts_hostSet
 *	get						dts_hostGet
 *
 */


/**
 *  DTS_HOSTACCESS -- Check file access on a host DTS machine.
 *
 *  @brief  Check file access on a host DTS machine.
 *  @fn     stat = dts_hostAccess (char *host, char *path, int mode)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path name to be checked
 *  @param  mode	access mode (R_OK|W_OK|X_OK, default F_OK)
 *  @return		1 (one) if access allowed/file exists
 */
int
dts_hostAccess (char *host, char *path, int mode)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostAccess: %s:%s  mode=%d\n", host, path, mode);

    if (path == (char *) NULL) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : "/"));
    xr_setIntInParam (client, mode);

    if (xr_callSync (client, "access") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostAccess:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTCAT -- Cat a file.
 *
 *  @brief  Cat a file.
 *  @fn     str = dts_hostCat (char *host, char *fname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  fname	file name to be read
 *  @return		Current working dir of DTS host
 */
char *
dts_hostCat (char *host, char *fname)
{
    int    client = dts_getClient (host);
    long   fsize = dts_hostFSize (host, fname);	/* get the file size */
    int    sz, snum, sum32;
    uint   r_sum32;
    ushort r_sum16;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostCat: %s:%s\n", host, fname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (fname ? fname : ""));
    if (xr_callSync (client, "cat") == OK) {
        char  res[2*SZ_BLOCK], *sres = res;
        unsigned char  *dres = (unsigned char *) calloc (1, (2*(fsize+2)));

        xr_getStructFromResult (client, &snum);
	    xr_getIntFromStruct (snum, "size", &sz);
	    xr_getIntFromStruct (snum, "sum32", &sum32);
	    xr_getStringFromStruct (snum, "data", &sres);
	xr_freeStruct (snum);

	(void) base64_decode (sres, dres, (size_t) (2*fsize));

	/* Validate the checksum.
	*/
        checksum (dres, sz, &r_sum16, &r_sum32);

        if (DEBUG) 
	    fprintf (stderr, "dts_hostCat:  dres='%s' sum match = %d\n",
		dres, (sum32 == r_sum32));

	dts_closeClient (client);

        return ((char *) dres);
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTCHECKSUM -- Checksum a file.
 *
 *  @brief  Checksum a file.
 *  @fn     str = dts_hostChecksum (char *host, char *fname, 
 * 		unsigned int sum32, unsigned int crc, char **md5)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  fname	file name to be read
 *  @param  sum32	32-but sum
 *  @param  crc		CRC checksum
 *  @param  md5		MD5 sum (allocated if NULL)
 *  @return		Current working dir of DTS host
 */
int
dts_hostChecksum (char *host, char *fname, unsigned int *sum32, 
		unsigned int *crc, char **md5)
{
    int    client = dts_getClient (host);


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostChecksum: %s:%s\n", host, fname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (fname ? fname : ""));
    if (xr_callSync (client, "checksum") == OK) {
        char  res[2*SZ_BLOCK], *sres = res;
	int   snum;

        xr_getStructFromResult (client, &snum);
	    xr_getIntFromStruct (snum, "sum32", (int *) sum32);
	    xr_getIntFromStruct (snum, "crc", (int *) crc);
	    xr_getStringFromStruct (snum, "md5", &sres);
	xr_freeStruct (snum);

	if (*md5 == NULL)
	    *md5 = calloc (1, SZ_LINE);
	strcpy (*md5, sres);

        if (DEBUG) 
	    fprintf (stderr, "dts_hostChecksum:  sum32=%d crc=%d md5=%s\n",
		*sum32, *crc, *md5);
	    
	dts_closeClient (client);
        return (OK);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTCOPY -- Copy a file on a host DTS machine.
 *
 *  @brief  Copy a file on a host DTS machine.
 *  @fn     stat = dts_hostCopy (char *host, char *old, char *new)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  old		old path
 *  @param  new		new path
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostCopy (char *host, char *old, char *new)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostCopy: %s -> %s\n", old, new);

    if (old == (char *) NULL || new == (char *) NULL) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, old);
    xr_setStringInParam (client, new);

    if (xr_callSync (client, "copy") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostCopy:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTCFG -- Get the config table of the DTS host.
 *
 *  @brief  Get the config table of the DTS host.
 *  @fn     cfg = dts_hostCfg (char *host)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  quiet	suppress output?
 *  @return		Current working dir of DTS host
 */
char *
dts_hostCfg (char *host, int quiet)
{
    int   client = dts_getClient (host);

    if (quiet != 0 && quiet != 1)
	quiet = 0;
    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostCfg: %s\n", dts_resolveHost (host) );

    /* Make the service call.
    */
    xr_setIntInParam (client, (int) quiet);
    if (xr_callSync (client, "cfg") == OK) {
        char  res[SZ_CONFIG+1], *sres = res;

	memset (res, 0, SZ_CONFIG+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostCfg:  sres = '%s'\n", sres);
	dts_closeClient (client);

        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTCHMOD -- Change the mode of a file on a remote DTS.
 *
 *  @brief  Change the mode of a file on a remote DTS.
 *  @fn     int dts_hostChmod (char *host, char *path, mode_t mode)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path to file
 *  @param  mode	file access mode
 *  @return		status (OK or ERR)
 */
int
dts_hostChmod (char *host, char *path, mode_t mode)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostChmod: %s (%d)\n", path, mode);

    /* Make the service call.
    */
    xr_setStringInParam (client, path);
    xr_setIntInParam (client, (int) mode);

    /* Make the service call.
    */
    if (xr_callSync (client, "chmod") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostChmod:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return ( ERR );
}


/**
 *  DTS_HOSTCWD -- Get the cwd of the DTS host.
 *
 *  @brief  Get the cwd of the DTS host.
 *  @fn     cwd = dts_hostCwd (char *host)
 *
 *  @param  host	host machine name (or IP string)
 *  @return		Current working dir of DTS host
 */
char *
dts_hostCwd (char *host)
{
    int   client = dts_getClient (host);


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostCwd: %s\n", dts_resolveHost (host) );

    /* Make the service call.
    */
    if (xr_callSync (client, "cwd") == OK) {
        char  res[SZ_PATH+1], *sres = res;

	memset (res, 0, SZ_PATH+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostCwd:  sres = '%s'\n", sres);
	dts_closeClient (client);

        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTDEL -- Delete a file on a DTS host.
 *
 *  @brief  Delete a file on a DTS host.
 *  @fn     stat = dts_hostDel (char *host, char *path, char *passwd, 
 *			int recursive)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path to file to be deleted
 *  @param  passwd	DTS host passwd
 *  @param  recursive	recursive delete?
 *  @return		success code
 */
int
dts_hostDel (char *host, char *path, char *passwd, int recursive)
{
    int   client = dts_getClient (host), res = -1;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostDel: %s:%s %s\n", host, path, passwd);

    if (path == (char *) NULL) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));
    xr_setStringInParam (client, (passwd ? passwd : ""));
    xr_setIntInParam (client, recursive);

    if (xr_callSync (client, "del") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostDel:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTDIR -- Get a directory listing from a DTS host.
 *
 *  @brief  Get a directory listing from a DTS host.
 *  @fn     list = dts_hostDir (char *host, char *path, int lsLong)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path on disk partition to be listed.
 *  @param  lsLong	long listing?
 *  @return		requested directory listing
 */
char *
dts_hostDir (char *host, char *path, int lsLong)
{
    int   client = dts_getClient (host);


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostDir:  '%s' '%s'\n", host, path);

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));
    xr_setIntInParam (client, lsLong);

    if (xr_callSync (client, "dir") == OK) {
        char  res[(MAX_DIR_ENTRIES * SZ_FNAME)+1], *sres = res;

	memset (res, 0, (MAX_DIR_ENTRIES * SZ_FNAME)+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostDir: '%s' \n", sres);
	dts_closeClient (client);

	return (strdup (sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTDESTDIR -- Get a listing for a queue's delivery directory
 *
 *  @brief  Get a listing for a queue's delivery directory
 *  @fn     list = dts_hostDestDir (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name to list
 *  @return		path to queue delivery directory
 */
char *
dts_hostDestDir (char *host, char *qname)
{
    int   client = dts_getClient (host);


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostDestDir:  '%s' '%s'\n", host, qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : ""));

    if (xr_callSync (client, "ddir") == OK) {
        char  res[(MAX_DIR_ENTRIES * SZ_FNAME)+1], *sres = res;

	memset (res, 0, (MAX_DIR_ENTRIES * SZ_FNAME)+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostDestDir: '%s' \n", sres);
	dts_closeClient (client);

	return (strdup (sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTISDIR -- Test whether the given path is a directory.
 *
 *  @brief  Test whether the given path is a directory.
 *  @fn     list = dts_hostIsDir (char *host, char *path)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path to be tested.
 *  @return		1 (one) if path is a directory, zero otherwise
 */
int
dts_hostIsDir (char *host, char *path)
{
    int   client = dts_getClient (host), res = -1;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostIsDir:  '%s' '%s'\n", host, path);

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));

    if (xr_callSync (client, "isDir") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostIsDir: '%d' \n", res);
	dts_closeClient (client);
	return (res);
    }

    dts_closeClient (client);
    return ( -1 );
}


/**
 *  DTS_HOSTDISKFREE -- Get the space available on the DTS host 
 *  containing a specified path.
 *
 *  @brief  Get the space available on the give containing a specified path.
 *  @fn     free = dts_hostDiskFree (char *host, char *path)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path on disk partition to be checked.
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostDiskFree (char *host, char *path)
{
    int   client = dts_getClient (host), size;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostDiskFree:  '%s' '%s'\n", host, path);

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));

    if (xr_callSync (client, "diskFree") == OK) {
	xr_getIntFromResult (client, &size);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostDiskFree:  sz = %d\n", size);
	dts_closeClient (client);
	return (size);
    }

    dts_closeClient (client);
    return (-1);
}


/**
 *  DTS_HOSTDISKUSED -- Get the space used on the DTS host containing a
 *  specified path.
 *
 *  @brief  Get the space available on the give containing a specified path.
 *  @fn     used = dts_hostDiskUsed (char *host, char *path)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path on disk partition to be checked.
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostDiskUsed (char *host, char *path)
{
    int   client = dts_getClient (host), size;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostDiskUsed:  '%s' '%s'\n", host, path);

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));

    if (xr_callSync (client, "diskUsed") == OK) {
	xr_getIntFromResult (client, &size);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostDiskUsed:  sz = %d\n", size);
	dts_closeClient (client);
	return (size);
    }

    dts_closeClient (client);
    return (-1);
}


/**
 *  DTS_HOSTECHO -- Have a host DTS machine echo a string.
 *
 *  @brief  Have a host DTS machine echo a string.
 *  @fn     str = dts_hostEcho (char *host, char *str)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  str		string to echo
 *  @return		1 (one) if DTS responds.
 */
char *
dts_hostEcho (char *host, char *str)
{
    int client = dts_getClient (host), len = strlen (str);


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostEcho:  '%s' '%s'\n", host, str);

    /* Make the service call.
    */
    xr_setStringInParam (client, (str ? str : ""));

    if (xr_callSync (client, "echo") == OK) {
        char  res[len+1], *sres = res;

	memset (res, 0, len+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostEcho:  sres = '%s'\n", sres);
	dts_closeClient (client);

        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTFSIZE -- Get a file size on a remote DTS host.
 *
 *  @brief  Get a file size on a remote DTS host.
 *  @fn     size = dts_hostFSize (char *host, char *path)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	pathname to be checked
 *  @return		size (bytes) of file
 */
long
dts_hostFSize (char *host, char *path)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostFSize:  %s:%s\n", host, path);

    if (path == (char *) NULL) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));

    if (xr_callSync (client, "fsize") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostFSize:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTFMODE -- Get a file mode on a remote DTS host.
 *
 *  @brief  Get a file mode on a remote DTS host.
 *  @fn     mode = dts_hostFMode (char *host, char *path)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	pathname to be checked
 *  @return		size (bytes) of file
 */
long
dts_hostFMode (char *host, char *path)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostFMode:  %s:%s\n", host, path);

    if (path == (char *) NULL) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));

    if (xr_callSync (client, "fmode") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostFMode:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTFTIME -- Get a file time on a remote DTS host.
 *
 *  @brief  Get a file time on a remote DTS host.
 *  @fn     size = dts_hostFTime (char *host, char *path, char *mode)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	pathname to be checked
 *  @param  mode	mode to check ('c'reate, 'm'odified, 'a'ccess)
 *  @return		time (seconds)
 */
int
dts_hostFTime (char *host, char *path, char *mode)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostFTime: %s:%s %s\n", host,path,mode);

    if (path == (char *) NULL) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));
    xr_setStringInParam (client, (mode ? mode : "r"));

    if (xr_callSync (client, "ftime") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostFTime:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTFGET -- Get a remote file to a local file.
 *
 *  @brief  Get a remote file to a local file.
 *  @fn     stat = dts_hostFGet (char *host, char *fname, char *local, int blk)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  fname	remote file name to be read
 *  @param  local	local file name to write
 *  @param  blk		i/o block size
 *  @return		status of transfer 
 */

#define MAX_TRIES	5

int
dts_hostFGet (char *host, char *fname, char *local, int blk)
{
    long   fsize = dts_hostFSize (host, fname);	/* get the file size */
    int    noff = 0, ofd, sz;
    int    nread = 0, nleft, nw=0, snum, tsec=0, tusec=0;
    struct timeval tv1, tv2;

    char  res[65536], *sres = res;
    extern double transferMB();


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostFGet: %s:%s -> %s\n", host, fname, local);


    /* Open the output file.
    */
    if (access (local, F_OK) == 0)
	unlink (local);
    if ((ofd = open (local, O_WRONLY|O_CREAT|O_TRUNC, DTS_FILE_MODE)) < 0) {
	fprintf (stderr, "Error: Cannot open local file '%s'\n", local);
	return (ERR);
    }


    /* Get the number of bytes to be read, but read the entire file.
    */
    if (blk > (65536 / 2)) 
	blk = (65536 / 2);
    nleft = fsize;

    /* Initialize the I/O time counters.
    */
    gettimeofday (&tv1, NULL);


    /*  Repeatedly make calls until we get all of the requested data.
    */
    while (nleft > 0) {

    	int    client = dts_getClient (host);
        unsigned char *dres = (unsigned char *) calloc (1, (2*blk+2));

	/* Make the service call to read a chunk of data.
	*/
	xr_setStringInParam (client, (fname ? fname : ""));
	xr_setIntInParam (client, noff);
	xr_setIntInParam (client, blk);

	memset (sres, 0, (2*blk));
    	if (xr_callSync (client, "read") == OK) {
            xr_getStructFromResult (client, &snum);
	        xr_getIntFromStruct (snum, "size",   &sz);
	        xr_getIntFromStruct (snum, "offset", &noff);
	        xr_getStringFromStruct (snum, "data", &sres);

	    /* Decode.
	    */
	    (void) base64_decode (sres, dres, (size_t) (2*blk));

	    if (sz == EOF) {
	        break;
	    } else {
		/*  Copy result to the output buffer.
		*/
		if ((nw = dts_fileWrite (ofd, dres, sz)) < 0) {
		    fprintf (stderr, "Error writing to file '%s'\n", local);
		    free ((char *) dres);
    	    	    dts_closeClient (client);
		    return (ERR);
		}
                nleft  -= sz;
                nread  += sz;
		nw     += nw;
	    }

	} else {
	    fprintf (stderr, "Error: RPC call failed\n");
	    free ((char *) dres); 	/* Bad call, return a null  	*/
	    xr_freeStruct (snum);
    	    dts_closeClient (client);
	    return (ERR);
	}

        dts_closeClient (client);
	sres = res;
	xr_freeStruct (snum);
        free ((char *) dres);
    }

    fsync (ofd);
    close (ofd);		/* close the otuput file	*/


    /* Update the I/O time counters.
    */
    gettimeofday (&tv2, NULL);
    tsec += tv2.tv_sec - tv1.tv_sec;
    tusec += tv2.tv_usec - tv1.tv_usec;
    if (tusec >= 1000000) {
        tusec -= 1000000; tsec++;
    } else if (tusec < 0) {
        tusec += 1000000; tsec--;
    }
    
    printf ("%s: %ld bytes transferred in %d.%d sec (%.2f MB/s)\n", fname,
	(long) fsize, tsec, tusec, transferMB((long)fsize, tsec, tusec));

    return (OK);
}


/**
 *  DTS_HOSTMKDIR -- Make a directory on a host DTS machine.
 *
 *  @brief  Make a directory on a host DTS machine.
 *  @fn     stat = dts_hostMkdir (char *host, char *path)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	directory to create
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostMkdir (char *host, char *path)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostMkdir:  '%s' '%s'\n", host, path);

    if (path == (char *) NULL) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));

    if (xr_callSync (client, "mkdir") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostMkdir:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTPOKE -- Poke a host DTS machine, i.e. ping w/out a restart.
 *
 *  @brief  Poke a host DTS machine, i.e. ping w/out a restart.
 *  @fn     stat = dts_hostPoke (char *host)
 *
 *  @param  host	host machine name (or IP string)
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostPoke (char *host)
{
    int client = dts_getClient (host), res = -1;
    static int value = 0;


    value++;
    if (client < 0)
	return (ERR);
    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostPoke:  '%s'\n", host);

    /* Make the service call.
    */
    xr_setIntInParam (client, (int) value);
    if (xr_callSync (client, "ping") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostPoke:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    } else {
        if (DEBUG) 
	    fprintf (stderr, "dts_hostPing RPC-FAILS:  value = %d\n", value);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTPING -- Ping a host DTS machine.
 *
 *  @brief  Ping a host DTS machine.
 *  @fn     stat = dts_hostPing (char *host)
 *
 *  @param  host	host machine name (or IP string)
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostPing (char *host)
{
    int client = dts_getClient (host), res = -1;
    static int value = 0;


    value++;

    if (client < 0)
	return (ERR);
    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostPing:  '%s'\n", host);

    /* Make the service call.
    */
    xr_setIntInParam (client, (int) value);
    if (xr_callSync (client, "ping") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostPing:  res = %d\n", res);
	    
	dts_closeClient (client);
        return (res);
    } else {
	if (dts_hostContact (host) == OK) {
            if (DEBUG) 
	        fprintf (stderr, "dts_hostPing:  contact res = OK\n");
    	    dts_closeClient (client);
    	    return (OK);
	}
        if (DEBUG) 
	    fprintf (stderr, "dts_hostPing RPC-FAILS:  value = %d\n", value);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTPINGSLEEP -- Ping a host DTS machine.
 *
 *  @brief  Ping a host DTS machine.
 *  @fn     stat = dts_hostPingSleep (char *host, int time)
 *
 *  @param  host	host machine name (or IP string)
 *  @return		1 (one) if DTS responds.
 *
 *  NOTE:  This is implemented as an ASYNC call.
 */

static int 
dts_pingSleepHandler (void *data)
{
    int client = *((int *)data), res = 0;
            
    xr_getIntFromResult (client, &res);
    if (DEBUG) fprintf (stderr, "handlePSResponse[%d]:  res=%d\n", client, res);

    return (OK);
}

int
dts_hostPingSleep (char *host, int time)
{
    int client = dts_getClient (host), res = -1;
    char  cbHost[SZ_FNAME];
    static int value = 0;


    value++;

    if (client < 0)
	return (ERR);
    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostPingSleep:  '%s' %d\n", host, time);

    /* Make the service call.
    */
    memset (cbHost, 0, SZ_FNAME);
    sprintf (cbHost, "%s:2998", dts_getLocalHost());

    xr_setStringInParam (client, cbHost);
    xr_setIntInParam (client, (int) time);

    if (xr_callSync (client, "pingsleep") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) fprintf (stderr, "dts_hostPingSleep:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    } else {
	fprintf (stderr, "dts_hostPingSleep RPC-FAILS:  value = %d\n", value);
    }

    dts_closeClient (client);
    return (ERR);
}

int
dts_hostSvrPing (char *host, int time)
{
    int client = dts_getClient (host), async = 0;
    static int value = 0;

    value++;

    if (client < 0)
	return (ERR);
    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostSvrPing: '%s' t=%d\n", host, time);

    /* Make the service call.
    */
    async = xr_newASync (client);
    xr_setIntInParam (async, (int) time);

    return (xr_callASync (async, "svrping", dts_pingSleepHandler));
}


/**
 *  DTS_HOSTPINGSTR -- Ping a host DTS machine, string return.
 *
 *  @brief  Ping a host DTS machine.
 *  @fn     str = dts_hostPingStr (char *host)
 *
 *  @param  host	host machine name (or IP string)
 *  @return		string response
 */
char *
dts_hostPingStr (char *host)
{
    int client = dts_getClient (host);
    static int value = 0;


    /*
    value = (value+1) % 4096;
    */
    value++;

    if (client < 0)
	return (NULL);
    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostPingStr:  '%s'\n", host);

    /* Make the service call.
    */
    xr_setIntInParam (client, (int) value);
    xr_setStringInParam (client, "test string");

    if (xr_callSync (client, "pingstr") == OK) {
       char  res[SZ_CONFIG+1], *sres = res;

	memset (res, 0, SZ_CONFIG+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostPingStr:  res = '%s;\n", sres);
	dts_closeClient (client);
        return (strdup (sres));
    } else
	fprintf (stderr, "dts_hostPingStr RPC-FAILS:  value = %d\n", value);

    dts_closeClient (client);
    return (NULL);
}


/**
 *  DTS_HOSTPINGARRAY -- Ping a host DTS machine, array return.
 *
 *  @brief  Ping a host DTS machine.
 *  @fn     stat = dts_hostPingArray (char *host)
 *
 *  @param  host	host machine name (or IP string)
 *  @return		sum of array values if DTS responds, -1 on error.
 */
int
dts_hostPingArray (char *host)
{
    int client = dts_getClient (host), res = -1;
    static int value = 0;


    /*
    value = (value+1) % 4096;
    */
    value++;

    if (client < 0)
	return (ERR);
    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostPingArray:  '%s'\n", host);

    /* Make the service call.
    */
    xr_setIntInParam (client, (int) value);
    if (xr_callSync (client, "pingarray") == OK) {
        int  anum, v1, v2, v3, sum = 0;

        xr_getArrayFromResult (client, &anum);
            xr_getIntFromArray (anum, 0, &v1);
            xr_getIntFromArray (anum, 1, &v2);
            xr_getIntFromArray (anum, 2, &v3);
 	res = v1 + v2 + v3;
 	sum = res;
        xr_freeArray (anum);

        if (DEBUG) 
	    fprintf (stderr, "dts_hostPingArray:  sum = %d\n", sum);
	dts_closeClient (client);
        return (OK);
    } else
	fprintf (stderr, "dts_hostPingArray RPC-FAILS:  value = %d\n", value);

    dts_closeClient (client);
    return (-1);
}


/**
 *  DTS_HOSTREMOTEPING -- Tell one machine to ping another.
 *
 *  @brief  Tell one machine to ping another.
 *  @fn     stat = dts_hostRemotePing (char *local, char *remote)
 *
 *  @param  local	local host machine name (or IP string)
 *  @param  remote	remote machine name (or IP string)
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostRemotePing (char *local, char *remote)
{
    int client = dts_getClient (local), res;


    if (client < 0)
	return (ERR);
    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostRemotePing: %s %s\n", local, remote);

    /* Make the service call.
    */
    xr_setStringInParam (client, (remote ? remote : ""));
    if (xr_callSync (client, "remotePing") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostRemotePing:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTPREALLOC -- Prealloc space on a host DTS machine.
 *
 *  @brief  Prealloc space on a host DTS machine
 *  @fn     stat = dts_hostPrealloc (char *host, char *path, long size)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path to file to prealloc
 *  @param  size	size of allocation
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostPrealloc (char *host, char *path, long size)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostPrealloc:  %s:%s %ld\n", host, path, size);

    if (!path) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : ""));
    xr_setIntInParam (client, (int) size);

    if (xr_callSync (client, "prealloc") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostPrealloc:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTRENAME -- Rename a file on a host DTS machine.
 *
 *  @brief  Rename a file on a host DTS machine.
 *  @fn     stat = dts_hostRename (char *host, char *old, char *new)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  old		old path
 *  @param  new		new path
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostRename (char *host, char *old, char *new)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostRename: %s -> %s\n", old, new);

    if (old == (char *) NULL || new == (char *) NULL) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, old);
    xr_setStringInParam (client, new);

    if (xr_callSync (client, "rename") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostRename:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTSTAT -- Get the file stat() information
 *
 *  @brief  Update access time on a host DTS machine.
 *  @fn     stat = dts_hostStat (char *host, char *path, struct stat *st)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path to touch
 *  @param  st		stat struct
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostStat (char *host, char *path, struct stat *st)
{
    int  client = dts_getClient (host), res = OK, ival;
#ifdef USE_STAT_STRUCT
    int  snum;
#else
    int  anum;
    long long  lval;
#endif
    

    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostStat:  %s:%s\n", host, path);

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : "/"));

    if (xr_callSync (client, "stat") == OK) {

        xr_getIntFromResult (client, &res);
#ifdef USE_STAT_STRUCT
	xr_getStructFromResult (client, &snum);
          xr_getIntFromStruct (snum, "device",  &ival); st->st_dev     = ival;
          xr_getIntFromStruct (snum, "inode",   &ival); st->st_ino     = ival;
          xr_getIntFromStruct (snum, "mode",    &ival); st->st_mode    = ival;
          xr_getIntFromStruct (snum, "nlink",   &ival); st->st_nlink   = ival;
          xr_getIntFromStruct (snum, "uid",     &ival); st->st_uid     = ival;
          xr_getIntFromStruct (snum, "gid",     &ival); st->st_gid     = ival;
          xr_getIntFromStruct (snum, "rdev",    &ival); st->st_rdev    = ival;
          xr_getIntFromStruct (snum, "size",    &ival); st->st_size    = ival;
          xr_getIntFromStruct (snum, "blksize", &ival); st->st_blksize = ival;
          xr_getIntFromStruct (snum, "blocks",  &ival); st->st_blocks  = ival;
          xr_getIntFromStruct (snum, "atime",   &ival); st->st_atime   = ival;
          xr_getIntFromStruct (snum, "mtime",   &ival); st->st_mtime   = ival;
          xr_getIntFromStruct (snum, "ctime",   &ival); st->st_ctime   = ival;
	xr_freeStruct (snum);
#else
        xr_getArrayFromResult (client, &anum);
          xr_getIntFromArray (anum, 0,  &ival);    st->st_dev     = ival; //0
          xr_getIntFromArray (anum, 1,  &ival);    st->st_ino     = ival; //1
          xr_getIntFromArray (anum, 2,  &ival);    st->st_mode    = ival; //2
          xr_getIntFromArray (anum, 3,  &ival);    st->st_nlink   = ival; //3
          xr_getIntFromArray (anum, 4,  &ival);    st->st_uid     = ival; //4
          xr_getIntFromArray (anum, 5,  &ival);    st->st_gid     = ival; //5
          xr_getIntFromArray (anum, 6,  &ival);    st->st_rdev    = ival; //6
          xr_getLongLongFromArray (anum,7, &lval); st->st_size    = lval; //7
          xr_getIntFromArray (anum, 8,  &ival);    st->st_blksize = ival; //8
          xr_getIntFromArray (anum, 9,  &ival);    st->st_blocks  = ival; //9
          xr_getIntFromArray (anum, 10, &ival);    st->st_atime   = ival; //10
          xr_getIntFromArray (anum, 11, &ival);    st->st_mtime   = ival; //11
          xr_getIntFromArray (anum, 12, &ival);    st->st_ctime   = ival; //12
	xr_freeArray (anum);
#endif

        if (DEBUG) 
	    fprintf (stderr, "dts_hostStat:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTSTATVAL -- Get a specific value from a file's stat() structure.
 *
 *  @brief  Get a specific value from a file's stat() structure.
 *  @fn     stat = dts_hostStatVal (char *host, char *path, char *val)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path
 *  @param  val		value to retrieve
 *  @return		value from stat() structure
 */
int
dts_hostStatVal (char *host, char *path, char *val)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostStatVal: %s -> %s\n", path, val);

    if (path == (char *) NULL) {
        dts_closeClient (client);
	return (ERR);
    }

    /* Make the service call.
    */
    xr_setStringInParam (client, path);
    xr_setStringInParam (client, val);

    if (xr_callSync (client, "statVal") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostStatVal:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTSTAT -- Get the file stat() information
 *
 *  @brief  Update access time on a host DTS machine.
 *  @fn     stat = dts_hostStat (char *host, char *path, struct stat *st)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path to touch
 *  @param  st		stat struct
 *  @return		1 (one) if DTS responds.

/**
 *  DTS_HOSTSETROOT -- Set the root directory of a DTS.
 *
 *  @brief  Set the root directory of a DTS.
 *  @fn     stat = dts_hostSetRoot (char *host, char *path)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path to touch
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostSetRoot (char *host, char *path)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostSetRoot:  %s:%s\n", host, path);

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : "/"));

    if (xr_callSync (client, "setRoot") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostSetRoot:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTSETDBG -- Set a debug flag on a DTS node.
 *
 *  @brief  Set a debug flag on a DTS node.
 *  @fn     stat = dts_hostSetDbg (char *host, char *flag)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  flag	flag to set
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostSetDbg (char *host, char *flag)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostSetDbg:  %s:%s\n", host, flag);

    /* Make the service call.
    */
    xr_setStringInParam (client, (flag ? flag : "/"));

    if (xr_callSync (client, "setDbg") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostSetDbg:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTUNSETDBG -- Unset a debug flag on the DTS node.
 *
 *  @brief  Unset a debug flag on the DTS node.
 *  @fn     stat = dts_hostUnsetDbg (char *host, char *flag)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  flag	flag to set
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostUnsetDbg (char *host, char *flag)
{
    int client = dts_getClient (host), res;

    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostUnsetDbg:  %s:%s\n", host, flag);

    /* Make the service call.
    */
    xr_setStringInParam (client, (flag ? flag : "/"));

    if (xr_callSync (client, "unsetDbg") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostUnsetDbg:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTTOUCH -- Update access time on a host DTS machine.
 *
 *  @brief  Update access time on a host DTS machine.
 *  @fn     stat = dts_hostTouch (char *host, char *path)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  path	path to touch
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostTouch (char *host, char *path)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostTouch:  %s:%s\n", host, path);

    /* Make the service call.
    */
    xr_setStringInParam (client, (path ? path : "/"));

    if (xr_callSync (client, "touch") == OK) {
        xr_getIntFromResult (client, &res);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostTouch:  res = %d\n", res);
	dts_closeClient (client);
        return (res);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTREAD -- Read a chunk from a file.
 *
 *  @brief  Read a chunk from a file.
 *  @fn     str = dts_hostRead (char *host, char *fname, int offset, 
 *				    int sz, int *retnb)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  fname	file name to be read
 *  @param  offset	file offset
 *  @param  sz		size of chunk to read
 *  @param  retnb	number of bytes read
 *  @return		Current working dir of DTS host
 */

#define MAX_TRIES	5


unsigned char *
dts_hostRead (char *host, char *fname, int offset, int sz, int *retnb)
{
    long   fsize = dts_hostFSize (host, fname);	/* get the file size */
    int    nbytes, chunk, noff;
    int    nread = 0, nleft = sz, snum;
    char  res[2*SZ_BLOCK], *sres = res;
    unsigned char  *data, *dp;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) 
	fprintf (stderr, "dts_hostCat: %s:%s\n", host, fname);

    /* Get the number of bytes to be read.
    */
    *retnb = -1;
    if (sz < 0) {
	/* Read the entire file.
	*/
    	nbytes = ((fsize > SZ_BLOCK) ? fsize : SZ_BLOCK);
    	chunk  = ((nbytes > SZ_BLOCK) ? SZ_BLOCK : nbytes);
    	nleft  = fsize;
    } else {
	/* Read only the requested number of bytes.
	*/
    	nbytes = ((sz > fsize) ? fsize : sz);
    	chunk  = ((nbytes > SZ_BLOCK) ? SZ_BLOCK : nbytes);
    	nleft  = sz;
    }
    dp = calloc (1, (2*nbytes));
    data = dp;
    noff = offset;

  
    /*  Repeatedly make calls until we get all of the requested data.
    */
    while (nleft > 0) {
    	int    client = dts_getClient (host);
        unsigned char *dres = (unsigned char *) calloc (1, (2*SZ_BLOCK));


	/* Make the service call to read a chunk of data.
	*/
	xr_setStringInParam (client, (fname ? fname : ""));
	xr_setIntInParam (client, noff);
	xr_setIntInParam (client, chunk);

	memset (sres, 0, (2*SZ_BLOCK));
    	if (xr_callSync (client, "read") == OK) {
            xr_getStructFromResult (client, &snum);
	        xr_getIntFromStruct (snum, "size",   &sz);
	        xr_getIntFromStruct (snum, "offset", &noff);
	        xr_getStringFromStruct (snum, "data", &sres);
	    xr_freeStruct (snum);


	    /* Decode and validate the checksum.
	    */
	    (void) base64_decode (sres, dres, (size_t) (2*sz));

	    if (sz == EOF)
	        break;
	    else {
	        memcpy (dp, dres, sz);  /* copy result to the output buffer */
	        dp     += sz;
                nleft  -= sz;
                nread  += sz;
	    }

	} else {
	    free ((char *) dres); 	/* Bad call, return a null  	*/
    	    dts_closeClient (client);
	    return ((unsigned char *) NULL);
	}

        dts_closeClient (client);
	sres = res;
        free ((char *) dres);
    }

    *retnb = nread;

    return (data);
}



/****************************************************************************
 *	dest						dts_hostDest
 *	src 						dts_hostSrc
 *	startQueue 					dts_hostStartQueue
 *	stopQueue 					dts_hostStopQueue
 *	listQueue 					dts_hostListQueue
 *	addToQueue 					dts_hostAddToQueue
 *	delFromQueue 					dts_hostDelFromQueue
 *	setQueueCount 					dts_hostSetQueueCount
 *	getQueueCount 					dts_hostGetQueueCount
 *	setQueueDir 					dts_hostSetQueueDir
 *	getQueueDir 					dts_hostGetQueueDir
 *	setQueueCmd 					dts_hostSetQueueCmd
 *	getQueueCmd 					dts_hostGetQueueCmd
 *	getCopyDir 					dts_hostCopyDir
 *	execCmd 					dts_hostExecCmd
 */

/**
 *  DTS_HOSTSRC -- Get the source of the named queue.
 *
 *  @brief  Get the source of the named queue.
 *  @fn     cwd = dts_hostSrc (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		Current working dir of DTS host
 */
char *
dts_hostSrc (char *host, char *qname)
{
    int   client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostSrc: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "queueSrc") == OK) {
        char  res[SZ_PATH+1], *sres = res;

	memset (res, 0, SZ_PATH+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) 
	    fprintf (stderr, "dts_hostSrc:  sres = '%s'\n", sres);
	dts_closeClient (client);

        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTDEST -- Get the destination of the named queue.
 *
 *  @brief  Get the destination of the named queue.
 *  @fn     cwd = dts_hostDest (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		Current working dir of DTS host
 */
char *
dts_hostDest (char *host, char *qname)
{
    int   client = dts_getClient (host);


    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostDest: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "queueDest") == OK) {
        char  res[SZ_PATH+1], *sres = res;

	memset (res, 0, SZ_PATH+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) fprintf (stderr, "dts_hostDest:  sres = '%s'\n", sres);
	dts_closeClient (client);

        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTSTARTQUEUE -- Start the named queue.
 *
 *  @brief  Start the named queue.
 *  @fn     cwd = dts_hostStartQueue (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		status
 */
int
dts_hostStartQueue (char *host, char *qname)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostStartQueue: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "startQueue") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( OK );
}


/**
 *  DTS_HOSTSHUTDOWNQUEUE -- Shutdown the named queue.
 *
 *  @brief  Shutdown the named queue.
 *  @fn     cwd = dts_hostShutdownQueue (char *host, char *qname)
 *
 *  @param  host        host machine name (or IP string)
 *  @param  qname       queue name
 *  @return             status
 */
int
dts_hostShutdownQueue (char *host, char *qname)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();                      /* initialize static variables  */
    if (DEBUG) fprintf (stderr, "dts_hostShutdownQueue: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "shutdownQueue") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( OK );
}


/**
 *  DTS_HOSTPRINTQUEUECFG -- Print the configuration of the named queue.
 *
 *  @brief  Print the configuration of the named queue.
 *  @fn     val = dts_hostPrintQueueCfg (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		status flag
 */
char *
dts_hostPrintQueueCfg (char *host, char *qname)
{
    int   client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostPrintQueueCfg: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "printQueueCfg") == OK) {
        char  res[8192], *sres = res;

	memset (res, 0, 8192);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) fprintf (stderr, "dts_hostPrintQueue:  sres = '%s'\n", sres);
	dts_closeClient (client);
        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( NULL );
}


/**
 *  DTS_HOSTGETQUEUESTAT -- Get the status flag on the named queue.
 *
 *  @brief  Get the status flag on the named queue.
 *  @fn     val = dts_hostGetQueueStat (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		status flag
 */
int
dts_hostGetQueueStat (char *host, char *qname)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostGetQueueStat: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "getQueueStat") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( res );
}


/**
 *  DTS_HOSTSETQUEUESTAT -- Set the status flag on the named queue.
 *
 *  @brief  Set the status flag on the named queue.
 *  @fn     val = dts_hostSetQueueStat (char *host, char *qname, int val)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @param  val 	queue status value
 *  @return		status flag
 */
int
dts_hostSetQueueStat (char *host, char *qname, int val)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostSetQueueStat: %s = %d\n", qname, val);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    xr_setIntInParam (client, val);
    if (xr_callSync (client, "setQueueStat") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( res );
}


/**
 *  DTS_HOSTGETQUEUEDIR -- Get the queue deliverDirectory.
 *
 *  @brief  Get the queue deliverDirectory.
 *  @fn     dir = dts_hostGetQueueDir (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		delivery directory for queue
 */
char *
dts_hostGetQueueDir (char *host, char *qname)
{
    int   client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostGetQueueDir: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "getQueueDir") == OK) {
        char  res[SZ_PATH+1], *sres = res;

	memset (res, 0, SZ_PATH+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) fprintf (stderr, "dts_hostGetQueueDir:  sres = '%s'\n",sres);
	dts_closeClient (client);
        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTSETQUEUEDIR -- Set the queue deliveryDir.
 *
 *  @brief  Set the queue deliveryDir.
 *  @fn     val = dts_hostSetQueueDir (char *host, char *qname, char *dir)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @param  dir 	queue delivery directory
 *  @return		status flag
 */
int
dts_hostSetQueueDir (char *host, char *qname, char *dir)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostSetQueueDir: %s = %s\n", qname, dir);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    xr_setStringInParam (client, dir);
    if (xr_callSync (client, "setQueueDir") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( res );
}


/**
 *  DTS_HOSTGETQUEUECMD -- Get the queue deliverCmd.
 *
 *  @brief  Get the queue deliverCmd.
 *  @fn     dir = dts_hostGetQueueCmd (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		delivery directory for queue
 */
char *
dts_hostGetQueueCmd (char *host, char *qname)
{
    int   client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostGetQueueCmd: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "getQueueCmd") == OK) {
        char  res[SZ_PATH+1], *sres = res;

	memset (res, 0, SZ_PATH+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) fprintf (stderr, "dts_hostGetQueueCmd:  sres = '%s'\n",sres);
	dts_closeClient (client);
        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTSETQUEUECMD -- Set the queue deliveryCmd.
 *
 *  @brief  Set the queue deliveryCmd.
 *  @fn     val = dts_hostSetQueueCmd (char *host, char *qname, char *cmd)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @param  cmd 	queue delivery command
 *  @return		status flag
 */
int
dts_hostSetQueueCmd (char *host, char *qname, char *cmd)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostSetQueueCmd: %s = %s\n", qname, cmd);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    xr_setStringInParam (client, cmd);
    if (xr_callSync (client, "setQueueCmd") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( res );
}


/**
 *  DTS_HOSTGETCOPYDIR -- Get the DTS copy directory
 *
 *  @brief  Get the DTS copy directory
 *  @fn     dir = dts_hostGetCopyDir (char *host)
 *
 *  @param  host	host machine name (or IP string)
 *  @return		delivery directory for queue
 */
char *
dts_hostGetCopyDir (char *host)
{
    int   client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostGetCopyDir: %s\n", host);

    /* Make the service call.
    */
    if (xr_callSync (client, "getCopyDir") == OK) {
        char  res[SZ_PATH+1], *sres = res;

	memset (res, 0, SZ_PATH+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) fprintf (stderr, "dts_hostGetCopyDir:  sres = '%s'\n", sres);
	dts_closeClient (client);
        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTEXECCMD -- Execute the given command.
 *
 *  @brief  Execute the given command.
 *  @fn     val = dts_hostExecCmd (char *host, char *ewd, char *cmd)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  ewd		effective working dir
 *  @param  cmd 	queue delivery command
 *  @return		status flag
 */
int
dts_hostExecCmd (char *host, char *ewd, char *cmd)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostExecCmd: cmd = '%s'\n", cmd);

    /* Make the service call.
    */
    xr_setStringInParam (client, ewd);
    xr_setStringInParam (client, cmd);
    if (xr_callSync (client, "execCmd") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( res );
}


/**
 *  DTS_HOSTSTOPQUEUE -- Stop the named queue.
 *
 *  @brief  Stop the named queue.
 *  @fn     cwd = dts_hostStopQueue (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		status
 */
int
dts_hostStopQueue (char *host, char *qname)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostStopQueue: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "stopQueue") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( OK );
}


/**
 *  DTS_HOSTFLUSHQUEUE -- Stop the named queue.
 *
 *  @brief  Stop the named queue.
 *  @fn     cwd = dts_hostFlushQueue (char *host, char *qname)
 *
 *  @param  host        host machine name (or IP string)
 *  @param  qname       queue name
 *  @return             status
 */
int
dts_hostFlushQueue (char *host, char *qname)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();                      /* initialize static variables  */
    if (DEBUG) fprintf (stderr, "dts_hostFlushQueue: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "flushQueue") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( OK );
}

/**
 *  DTS_HOSTPOKEQUEUE -- Poke the named queue.
 *
 *  @brief  Poke the named queue.
 *  @fn     cwd = dts_hostPokeQueue (char *host, char *qname)
 *
 *  @param  host        host machine name (or IP string)
 *  @param  qname       queue name
 *  @return             status
 */
int
dts_hostPokeQueue (char *host, char *qname)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();                      /* initialize static variables  */
    if (DEBUG) fprintf (stderr, "dts_hostPokeQueue: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "pokeQueue") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( OK );
}


/**
 *  DTS_HOSTPAUSEQUEUE -- Pause the named queue.
 *
 *  @brief  Pause the named queue.
 *  @fn     cwd = dts_hostPauseQueue (char *host, char *qname)
 *
 *  @param  host        host machine name (or IP string)
 *  @param  qname       queue name
 *  @return             status
 */
int
dts_hostPauseQueue (char *host, char *qname)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();                      /* initialize static variables  */
    if (DEBUG) fprintf (stderr, "dts_hostPauseQueue: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "pauseQueue") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( OK );
}


/**
 *  DTS_HOSTLISTQUEUE -- List the named queue.
 *
 *  @brief  List the named queue.
 *  @fn     cwd = dts_hostListQueue (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		status
 */
char *
dts_hostListQueue (char *host, char *qname)
{
    int   client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostListQueue: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "listQueue") == OK) {
        char  res[SZ_PATH+1], *sres = res;

	memset (res, 0, SZ_PATH+1);
        xr_getStringFromResult (client, &sres);
        if (DEBUG) fprintf (stderr, "dts_hostListQueue:  sres = '%s'\n", sres);
	dts_closeClient (client);
        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}



/*	addToQueue 					dts_hostAddToQueue
 *	delFromQueue 					dts_hostDelFromQueue
 *	setQueueCount 					dts_hostSetQueueCount
 *	getQueueCount 					dts_hostGetQueueCount
 *	setQueueDir 					dts_hostSetQueueDir
 *	getQueueDir 					dts_hostGetQueueDir
 *	setQueueCmd 					dts_hostSetQueueCmd
 *	getQueueCmd 					dts_hostGetQueueCmd
 *	getCopyDir 					dts_hostCopyDir
 *	execCmd 					dts_hostExecCmd
 */


/**
 *  DTS_HOSTSETQUEUECOUNT -- Set the pending count for the named queue.
 *
 *  @brief  Set the pending count for the named queue.
 *  @fn     cwd = dts_hostSetQueueCount (char *host, char *qname, int count)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @param  count	queue count
 *  @return		status
 */
int
dts_hostSetQueueCount (char *host, char *qname, int count)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostSetQueueCount: %s %d\n", qname, count);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    xr_setIntInParam (client, count);
    if (xr_callSync (client, "setQueueCount") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( OK );
}


/**
 *  DTS_HOSTGETQUEUECOUNT -- Get the pending count for the named queue.
 *
 *  @brief  Get the pending count for the named queue.
 *  @fn     cwd = dts_hostGetQueueCount (char *host, char *qname)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @return		status
 */
int
dts_hostGetQueueCount (char *host, char *qname)
{
    int   res=ERR, client = dts_getClient (host);

    dts_cmdInit();			/* initialize static variables	*/
    if (DEBUG) fprintf (stderr, "dts_hostGetQueueCount: %s\n", qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "default"));
    if (xr_callSync (client, "getQueueCount") == OK)
        xr_getIntFromResult (client, &res);

    dts_closeClient (client);
    return ( res );
}




/****************************************************************************/

/**
 *  DTS_HOSTABORT -- Exit the DTS without prejudice.
 *
 *  @brief  Just freakin' die 
 *  @fn     stat = dts_hostAbort (char *host, char *passwd)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  passwd	DTS host passwd
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostAbort (char *host, char *passwd)
{
    int client = dts_getClient (host);


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostAbort:  %s\n", host);
    
    /* Exit.  That's it, no real "computer science things" to do, just die.
    */
    if (!passwd || !passwd[0]) {
        fprintf (stderr, "Error: Passwd required to abort on '%s'\n", host);
        dts_closeClient (client);
	return (ERR);
    }

    /* Note we can't call an async method here because we don't ever expect
    ** a response.  The sync method will return a fault we just ignore.
    */
    xr_setStringInParam (client, passwd);
    if (xr_callSync (client, "sys.abort") == OK)
        ;

    dts_closeClient (client);
    return (OK);
}


/**
 *  DTS_HOSTCONTACT -- Attempt to contact/start the remote DTS.
 *
 *  @brief  Attempt to contact/start the remote DTS.
 *  @fn     stat = dts_hostContact (char *host)
 *
 *  @param  host	host machine name (or IP string)
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostContact (char *host)
{
    int   i, sock = 0, port = -1, len;


    for (i=0; i < dts->nclients; i++) {
	len = min (strlen (host), strlen (dts->clients[i]->clientHost));
	if (strncmp (host, dts->clients[i]->clientHost, len) == 0) {
	    port = dts->clients[i]->clientContact;
	    break;
	}
    }

    /*  If the client has a contact port specified, try to open the 
     *  connection on a client socket.
     */
    if (port > 0) {
        if ((sock = dts_openClientSocket (host, port, 0)) > 0) {
	    if (DEBUG)
		fprintf (stderr, "Closing contact socket '%d'\n", sock);
	    close (sock);
	    sleep (SOCK_PAUSE_TIME);	/* give it a chance to start up */
	    return (OK);
	}
    }
    return (ERR);
}


/**
 *  DTS_HOSTSHUTDOWN -- Shutdown the DTS cleanly.
 *
 *  @brief  Just freakin' die 
 *  @fn     stat = dts_hostShutdown (char *host, char *passwd)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  passwd	DTS host passwd
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostShutdown (char *host, char *passwd)
{
    int client = dts_getClient (host);


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostShutdown:  %s\n", host);

    /* Exit.  That's it, no real "computer science things" to do, just die.
    */
    if (!passwd || !passwd[0]) {
        fprintf (stderr, "Error: Passwd required to shutdown on '%s'\n", host);
        dts_closeClient (client);
	return (ERR);
    }


    /* Note we can't call an async method here because we don't ever expect
    ** a response.  The sync method will return a fault we just ignore.
    */
    xr_setStringInParam (client, passwd);
    if (xr_callSync (client, "system.shutdown") == OK)
        ;

    dts_closeClient (client);
    return (OK);
}


/**
 *  DTS_HOSTSET -- Set a value on the remote host.
 *
 *  @brief  Set a value on the remote host.
 *  @fn     stat = dts_hostSet (char *host, char *class, char *key, char *val)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  class	command class
 *  @param  key		keyword
 *  @param  val		value
 *  @return		1 (one) if DTS responds.
 */
int
dts_hostSet (char *host, char *class, char *key, char *val)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostSet: %s -> %s\n", key, val);

    /* Make the service call.
    */
    xr_setStringInParam (client, "client");
    xr_setStringInParam (client, class);
    xr_setStringInParam (client, key);
    xr_setStringInParam (client, val);

    if (xr_callSync (client, "dtsSet") == OK) {
        xr_getIntFromResult (client, &res);
	dts_closeClient (client);
        return (OK);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTGET -- Get a value from the remote host.
 *
 *  @brief  Get a value from the remote host.
 *  @fn     val = dts_hostGet (char *host, char *class, char *key)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  class	class
 *  @param  key		keyword
 *  @return		value from remote DTS system
 */
char *
dts_hostGet (char *host, char *class, char *key)
{
    int client = dts_getClient (host);
    char  *val = NULL;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostGet: %s -> %s\n", key, val);

    /* Make the service call.
    */
    xr_setStringInParam (client, class);
    xr_setStringInParam (client, key);

    if (xr_callSync (client, "dtsGet") == OK) {
	char  res[SZ_LINE], *sres = res;

	memset (res, 0, SZ_LINE);
        xr_getStringFromResult (client, &sres);
        if (DEBUG)
            fprintf (stderr, "dts_hostGet:  sres = '%s'\n", sres);
        dts_closeClient (client);

        return (strdup(sres));
    }

    dts_closeClient (client);
    return ( (char *) NULL );
}


/**
 *  DTS_HOSTUPSTATS -- Update the transfer statistics.
 *
 *  @brief  Update the transfer statistics.
 *  @fn     stat = dts_hostUpStats (char *host, char *qname, xferStat *xfs)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	queue name
 *  @param  xfs		transfer stats
 *  @return		status
 */
int
dts_hostUpStats (char *host, char *qname, xferStat *xfs)
{
    int client = dts_getClient (host), res;


    dts_cmdInit();			/* initialize static variables	*/

    if (DEBUG) fprintf (stderr, "dts_hostUpStats: %s:%s\n", host, qname);
    
    /* Make the service call.
    */
    xr_setStringInParam (client, qname);
    xr_setIntInParam (client, xfs->fsize);
    xr_setDoubleInParam (client, xfs->tput_mb);
    xr_setDoubleInParam (client, xfs->time);

    if (xr_callSync (client, "updateStats") == OK) {
        xr_getIntFromResult (client, &res);
	dts_closeClient (client);
        return (OK);
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTNODESTAT -- Get the queue statistics.
 *
 *  @brief  Get the queue statistics.
 *  @fn     stat = dts_hostNodeStat (char *host, char *qname, int errs, 
 *			struct nodeStat ns)
 *
 *  @param  host	host machine name (or IP string)
 *  @param  qname	Queue name to stat, or 'all'
 *  @param  errs	Number of error messages to return
 *  @param  ns		node stat struct
 *  @return		1 (one) if DTS responds.
 */

int
dts_hostNodeStat (char *host, char *qname, int errs, nodeStat *ns)
{
    int client = dts_getClient (host);
    int i, anum;
    char *sres = (char *) NULL;
    qStat *qs = (qStat *) NULL;


    dts_cmdInit();			/* initialize static variables	*/

    memset (ns, 0, sizeof(nodeStat));

    if (DEBUG) fprintf (stderr, "dts_hostNodeStat:  %s:%s\n", host, qname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : "all"));
    xr_setIntInParam (client, errs);

    if (xr_callSync (client, "nodeStat") == OK) {
	int rnum = 0;

	xr_getArrayFromResult (client, &rnum);

        xr_getStringFromArray (rnum, 0, &sres);
	strcpy (ns->root, (sres ? sres : "/"));

        xr_getIntFromArray (rnum, 1, &ns->dfree);
        xr_getIntFromArray (rnum, 2, &ns->dused);

	/*  Process error message array.
	 */
       	xr_getIntFromArray (rnum, 3, &ns->nerrors);
       	xr_getArrayFromArray (rnum, 4, &anum);
	for (i=0; i < ns->nerrors; i++) {
            xr_getStringFromArray (anum, i, &sres);
	    if (sres)
	 	strcpy ((ns->emsgs[i] = calloc (1, SZ_PATH)),  sres);
	}
	xr_freeArray (anum);

	/*  Process the queue array.
	 */
        xr_getIntFromArray (rnum, 5, &ns->nqueues);

	for (i=0; i < ns->nqueues; i++) {
	    memset (&ns->queue[i], 0, sizeof(qStat));
	    qs = &ns->queue[i];

	    xr_getStringFromArray (rnum, (6+i), &sres);
	    sscanf (sres, "%s %d %d %s %s %s %d %g %g %g %g %s %s %d %d", 
		qs->name,
		&qs->current, &qs->pending, qs->type, qs->status, 
	        qs->src, &qs->qcount,
		&qs->rate, &qs->time, &qs->size, &qs->total,
		qs->infile, qs->outfile, &qs->numflushes, &qs->canceledxfers);
	}

        if (DEBUG) 
	    fprintf (stderr, "dts_hostNodeStat:  done\n");

        xr_freeArray (rnum);
	dts_closeClient (client);
        return (OK);

    } else {
        if (DEBUG) fprintf (stderr, "dts_hostNodeStat:  returns ERR\n");
    }

    dts_closeClient (client);
    return (ERR);
}


/**
 *  DTS_HOSTSUBMITLOGS -- Submit logs from the DTSQ process.
 *
 *  @brief  Submit logs from the DTSQ process.
 *  @fn     list = dts_hostSubmitLogs (char *host, char *qname, 
 *			char *log, char *recover)
 *
 *  @param  host        host machine name (or IP string)
 *  @param  qname       queue name to list
 *  @param  log         submission logfile
 *  @param  recover     recover logfile
 *  @return             1 (one) if DTS responds.
 */
int
dts_hostSubmitLogs (char *host, char *qname, char *log, char *recover)
{
    int   client = dts_getClient (host);


    dts_cmdInit();                      /* initialize static variables  */

    if (DEBUG) fprintf (stderr, "dts_hostSubmitLogs:  '%s' '%s' '%s' '%s'\n", 
	host, qname, log, recover);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : ""));
    xr_setStringInParam (client, (log ? log : ""));
    xr_setStringInParam (client, (recover ? recover : ""));

    if (xr_callSync (client, "submitLogs") == OK) {
        int  res = 0;

        xr_getIntFromResult (client, &res);
        if (DEBUG)
            fprintf (stderr, "dts_hostSubmitLogs: %d \n", res);
        dts_closeClient (client);

        return (res);
    }

    dts_closeClient (client);
    return ( OK );
}


/**
 *  DTS_HOSTGETQLOG -- Get the named logfile from the DTSD queue.
 *
 *  @brief  Get the named logfile from the DTSD queue.
 *  @fn     str = dts_hostGetQLog (char *host, char *qname, char *fname)
 *
 *  @param  host        host machine name (or IP string)
 *  @param  qname       queue name to list
 *  @param  fname       logfile name
 *  @return             contents of requested file.
 */

#define	SZ_LOGBUF	(SZ_LINE * 1024)

char *
dts_hostGetQLog (char *host, char *qname, char *fname)
{
    int   client = dts_getClient (host);


    dts_cmdInit();                      /* initialize static variables  */

    if (DEBUG) fprintf (stderr, "dts_hostGetQLog:  '%s' '%s' '%s'\n", 
	host, qname, fname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : ""));
    xr_setStringInParam (client, (fname ? fname : ""));

    if (xr_callSync (client, "getQLog") == OK) {
	char  res[SZ_LOGBUF], *sres = res;

	memset (res, 0, SZ_LOGBUF);
        xr_getStringFromResult (client, &sres);
        if (DEBUG)
            fprintf (stderr, "dts_hostGetQLog: '%s' \n", sres);
        dts_closeClient (client);

        return ( strdup(sres) );
    }

    dts_closeClient (client);
    return ( NULL );
}


/**
 *  DTS_HOSTERASEQLOG -- Erase the named logfile in the DTSD queue.
 *
 *  @brief  Erase the named logfile in the DTSD queue.
 *  @fn     str = dts_hostEraseQLog (char *host, char *qname, char *fname)
 *
 *  @param  host        host machine name (or IP string)
 *  @param  qname       queue name to list
 *  @param  fname       logfile name
 *  @return             1 (one) if DTS responds.
 */
int
dts_hostEraseQLog (char *host, char *qname, char *fname)
{
    int   client = dts_getClient (host);


    dts_cmdInit();                      /* initialize static variables  */

    if (DEBUG) fprintf (stderr, "dts_hostEraseQLog:  '%s' '%s' '%s'\n", 
	host, qname, fname);

    /* Make the service call.
    */
    xr_setStringInParam (client, (qname ? qname : ""));
    xr_setStringInParam (client, (fname ? fname : ""));

    if (xr_callSync (client, "eraseQLog") == OK) {
        int  res = 0;

        xr_getIntFromResult (client, &res);
        if (DEBUG)
            fprintf (stderr, "dts_hostEraseQLog: %d \n", res);
        dts_closeClient (client);

        return ( res );
    }

    dts_closeClient (client);
    return ( OK );
}





/** *********************************************************************
 *  Private Utility Procedures
 ********************************************************************** */

/**
 *  DTS_ISLOCAL -- Is the given host the local machine?
 *
 *  @brief  Is the given host the local machine?
 *  @fn     stat = dts_isLocal (char *host)
 *
 *  @param  host	host machine name (or IP string)
 *  @return		1 (one) if DTS responds.
 */
int
dts_isLocal (char *host)
{
    char  *lhost = strdup (host);
    char  *cp = strchr (lhost, (int)':');
    int   isLocal = -1;


    if (cp)		/* truncate any port spec	*/
	*cp = '\0';

    /*  Match name, IP, or host within FQDN.
     */
    if (strcasecmp (lhost, localhost) == 0 || 
	strcasecmp (lhost, dts_getLocalIP()) == 0 ||
	strncasecmp (lhost, localhost, strlen (lhost)) ==0)
	    isLocal = 1;
    else
	isLocal = 0;

    free ((void *) lhost);
    return (isLocal);
}


/**
 *  DTS_CMDINIT - Initialize the command interface.
 */
void
dts_cmdInit ()
{
    /* Get the local host name the first time called.
    */
    if (!localhost) {
	localhost = (char *) calloc (1, SZ_PATH);
	gethostname (localhost, SZ_PATH);
    } 
    if (!url)
	url = (char *) calloc (1, SZ_URL);
}


/**
 *  DTS_GETCLIENT - Get a client handle for the given host.
 */
int
dts_getClient (char *host)
{
    int  client;
    char *s_host = dts_resolveHost (host);


    if (s_host == NULL)			/* cannot resolve host		*/
	return (0);

    dts_cmdInit();			/* initialize static variables	*/

    memset (url, 0, SZ_URL);
    if (host && strchr (host, (int)':'))
	sprintf (url, "http://%s/RPC2", host);
    else
	sprintf (url, "http://%s:%d/RPC2", s_host, DTS_PORT);

    if (DEBUG) 
	fprintf (stderr, "getClient: host '%s' -> server url = '%s'\n", 
	    host, url);
    
    client = xr_initClient (url, "client", "1.0");

    if (client >= 0) {
        xr_initParam (client);
#ifdef USE_CLEVEL
        clevel++;
        if (clevel > 1)
	    fprintf (stderr, "\nNested Client Level Error\n\n");
#endif
    }

    return (client);
}


/**
 *  DTS_CLOSECLIENT - Close a client handle for the given host.
 */
int
dts_closeClient (int client)
{
    xr_closeClient (client);

#ifdef USE_CLEVEL
    clevel--;
    if (clevel < 0)
	fprintf (stderr, "\nClosed Client Level Error\n\n");
#endif

    return (OK);
}
