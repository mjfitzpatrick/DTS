/**
 *  DTS Local I/O Interface - Local operations
 *
 *  File Operations:
 *
 *	access <path> <mode>	     - check file access
 *	cwd			     - get current working directory
 *	del <pattern> <pass> <recur> - delete a file
 *	dir <pattern>		     - get a directory listing
 *	diskFree		     - how much disk space is available?
 *	diskUsed		     - how much disk space is used?
 *	echo <str>		     - simple echo function
 *	fsize <fname>		     - get a file size
 *	ftime <fname>		     - get a file time
 *	mkdir <path>		     - make a directory
 *	mtime <path> <tm>	     - set file modification time
 *	ping			     - simple aliveness test function
 *	prealloc <fname> <nbytes>    - preallocate space for a file
 *	rename <old> <new>	     - rename a file
 *	stat <fname>		     - get file stat() info
 *	setroot <path>		     - set the root directory of a DTS
 *	touch <path>		     - touch a file access time
 *
 *  @file       dtsLocal.c
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
#include <sys/types.h>
#include <sys/time.h>
#include <fcntl.h>

#include <xmlrpc-c/base.h>
#include <xmlrpc-c/client.h>
#include <xmlrpc-c/server.h>
#include <xmlrpc-c/server_abyss.h>

#include "dts.h"
#include "dtsMethods.h"


extern  DTS  *dts;



/*******************************************************************************
 *  File Operations:
 *
 *	access <path> <mode>	     - check file access
 *	copy <src> <dest>	     - copy a file
 *	cwd			     - get current working directory
 *	del <pattern>		     - delete a file
 *	dir <patter>		     - get a directory listing
 *	diskFree		     - how much disk space is available?
 *	diskUsed		     - how much disk space is used?
 *	echo <str>		     - simple echo function
 *	fsize <fname>		     - get a file size
 *	ftime <fname>		     - get a file time
 *	mkdir <path>		     - make a directory
 *	mtime <path> <tm>	     - set file modification time
 *	ping			     - simple aliveness test function
 *	prealloc <fname> <nbytes>    - preallocate space for a file
 *	rename <old> <new>	     - rename a file
 *	stat <fname>		     - get file stat() info
 *	setroot <path>		     - set the root directory of a DTS
 *	touch <fname>		     - touch a file access time
 */


/**
 *  DTS_LOCALACCESS -- Check a local file for an access mode.
 *
 *  @brief	Check a local file for an access mode
 *  @fn 	int dts_localAccess (char *path, int mode)
 *
 *  @param  path	file path to check
 *  @param  mode	access mode
 *  @return		status code or errno
 */
int 
dts_localAccess (char *path, int mode)
{
    return ( access (path, mode) );
}


/**
 *  DTS_LOCALCOPY -- Copy a local file.
 *
 *  @brief      Copy a local file
 *  @fn 	int dts_localCopy (char *old, char *new)
 *
 *  @param  old        old filename
 *  @param  new        new filename
 *  @return            status code or errno
 */
int
dts_localCopy (char *old, char *new)
{
    int    res = OK;


    /*  If the file doesn't exist, we can't rename it.
    */
    if (access (old, F_OK) < 0)
	return (ERR);

    /*  Otherwise, try copying the file.
    */

	/* FIXME -- Not Yet Implemented */


    return (res);
}


/**
 *  DTS_LOCALCWD -- Get the current working directory.
 *
 *  @brief	Get the current working directory.
 *  @fn 	dir = dts_localCwd (void)
 *
 *  @param  data	caller param data
 *  @return		status code or errno
 */
char *
dts_localCwd ()
{
    static char  *cwd, buf[SZ_PATH];


    /* Get current working directory.
    */
    memset (buf, 0, SZ_PATH);
    cwd = getcwd (buf, (size_t) SZ_PATH);

    return ( dts_strbuf (buf) );
}


/**
 *  DTS_LOCALDELETE -- Delete a local file.
 *
 *  @brief	Delete a local file.
 *  @fn 	int dts_localDelete (char *path, int recurse)
 *
 *  @param  path	local file to delete
 *  @param  recurse	recursive delete?
 *  @return		status code or errno
 */
int 
dts_localDelete (char *path, int recurse)
{
    int   res  = 0;
    char  template[SZ_PATH];
    DIR  *dir;


    /*  If the file/dir doesn't exist, we can't delete it.  Skip this check
     *  if we're doing filename matching.
     */
    if (! dts_isTemplate (path)) {
        if (access (path, R_OK) < 0) {
            return (ERR);
        } else if ( (dir = opendir (path)) == (DIR *) NULL) {
            /* An error opening the directory is also bad.
            */
            return (ERR);
        }
	if (dir) 
	    closedir (dir);

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
            return (ERR);
        }
    }


    /* Delete the file.
    */
    res = dts_unlink ((char *) path, recurse, template);

    return (res);
}


/**
 *  DTS_LOCALDIR -- Get a local directory listing.
 *
 *  @brief	Get a local directory listing.
 *  @fn 	int dts_localDir (char *path, int lsLong)
 *
 *  @param  path	path to direcory root
 *  @param  lsLong	get long directory listing?
 *  @return		status code or errno
 */
char * 
dts_localDir (char *path, int lsLong)
{
    char   template[SZ_PATH], line[SZ_LINE], dat[SZ_LINE], perms[SZ_LINE];
    char   root[SZ_PATH], fpath[SZ_PATH], *list;
    static char dlist[MAX_DIR_ENTRIES * SZ_FNAME];

    DIR    *dir;
    struct  dirent *entry;
    struct  stat st;
    int     list_init = 0, dlen;
    time_t  mtime = (time_t) 0;


    list = dlist;
    memset (root, 0, SZ_PATH);
    memset (template, 0, SZ_PATH);
    memset (list, 0, (MAX_DIR_ENTRIES * SZ_FNAME));


    /*  If the directory doesn't exist, we can't list it.  Skip this check
     *  if we're doing filename matching.
     */
    if (! dts_isTemplate (path)) {
        if (access (path, R_OK) < 0)
	    return (NULL);
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
        if ( (dir = opendir (path)) == (DIR *) NULL)
            /* An error opening the directory is also bad.
	    */
	    return (NULL);
        else
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
	strcpy (template, path);

	if ( (dir = opendir (npath)) == (DIR *) NULL)
	    return (NULL);
        else
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
    closedir (dir);					/* clean up	*/

    return (list);
}


/**
 *  DTS_LOCALCHMOD -- Change the mode of a local file.
 *
 *  @brief	Change the mode of a local file.
 *  @fn 	int dts_localChmod (char *path, mode_t mode)
 *
 *  @param  path	file name
 *  @param  mode	file mode
 *  @return		status code or errno
 */
int 
dts_localChmod (char *path, mode_t mode)
{
    int   res  = 0;

    /* Change the file mode.
    */
    if (access (path, F_OK) == 0)
        res = chmod (path, mode);
    else
	res = ERR;

    return (res);
}


/**
 *  DTS_LOCALMKDIR -- Make a local directory.
 *
 *  @brief	Make a local directory
 *  @fn 	int dts_localMkdir (char *path)
 *
 *  @param  path	directory path to create
 *  @return		status code or errno
 */
int 
dts_localMkdir (char *path)
{
    char   *ip = path, *op, dir[PATH_MAX];
    int     res = 0;


    /* Create the directory.  It is not an error if the directory
     * already exists.
     */
    if (access (dir, R_OK|W_OK|X_OK)) {
        memset (dir, 0, PATH_MAX);
        op = dir;

        while (*ip) {
	    *op++ = *ip++;
	    while ( *ip && *ip != '/' )
	        *op++ = *ip++;

	    /* Create a sub-element of the path.
	    */
	    if (access (dir, F_OK)) {
	        if ((res = mkdir (dir, (mode_t) DTS_DIR_MODE)) < 0)
		    break;
	    }

	    *op++ = *ip;
	    if (*ip && *ip == '/') 
	        ip++;
        }
    } else
	res = 0;

    return (res);
}


/**
 *  DTS_LOCALMtime -- Update file modification time.
 *
 *  @brief	Update file modification time
 *  @fn 	int dts_localMtime (char *path, long mtime)
 *
 *  @param  path	directory path to create
 *  @param  mtime	modification time
 *  @return		status code or errno
 */
int 
dts_localMtime (char *path, long mtime)
{
    struct  timeval  tvp[2];
    int     res = 0;


    /* Create the directory.  It is not an error if the directory
     * already exists.
     */
    if (access (path, R_OK)) {
        tvp[0].tv_sec  = tvp[1].tv_sec  = mtime;
        tvp[0].tv_usec = tvp[1].tv_usec = 0L;

	res = utimes (path, tvp);
    }

    return (res);
}


/**
 *  DTS_LOCALRENAME -- Rename a local file.
 *
 *  @brief	Rename a local file.
 *  @fn 	int dts_localRename (char *old, char *new)
 *
 *  @param  old		old file name
 *  @param  new		new file name
 *  @return		status code or errno
 */
int 
dts_localRename (char *old, char *new)
{
    int    res = 0;


    /*  If the file doesn't exist, we can't rename it.
    */
    if (access (old, F_OK) < 0)
	return (OK);

    /*  Otherwise, try renaming the file.
    */
    res = rename (old, new);

    return (res);
}


/**
 *  DTS_LOCALTOUCH -- Touch a local file.
 *
 *  @brief	Touch a local file.
 *  @fn 	int dts_localTouch (char *path)
 *
 *  @param  path	filename path to touch
 *  @return		status code or errno
 */
int 
dts_localTouch (char *path)
{
    int     res  = 0, ifd = 0;
    struct  utimbuf tb;
    time_t  now = time ((time_t) 0);


    /* If the file doesn't exist, create it.
    */
    if (access (path, F_OK) < 0) {
	if ( (ifd = creat (path, DTS_FILE_MODE)) < 0 )
	    return (OK);
	close (ifd);
    }

    /* Touch a file's access/modify time with the current time.
    */
    tb.actime  = now;
    tb.modtime = now;
    res = utime ((char *) path, &tb);

    return ( (res ? errno : OK) );
}


/**
 *  DTS_LOCALPREALLOC -- Pre-allocate a local file.
 *
 *  @brief	Pre-allocate a local file.
 *  @fn 	int dts_localPrealloc (char *path, int size)
 *
 *  @param  path	filename path to prealloc
 *  @param  size	file size
 *  @return		status code or errno
 */
int 
dts_localPrealloc (char *path, int size)
{
    int    res  = 0;

    /* Preallocate a file of the given size.
    */
    res = dts_preAlloc (path, (long) size);

    return (res);
}
