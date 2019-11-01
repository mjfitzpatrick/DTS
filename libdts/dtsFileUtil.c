/**
 *  DTS File Utilities
 *
 *	dts_fopen (char *fname, char *flags)
 *	dts_fclose (FILE *fd)
 *	dts_fileOpen (char *fname, int flags)
 *	dts_fileClose (int fd)
 *	dts_preAlloc (char *fname, long fsize)
 *	dts_fileSize (int fd)
 *	dts_fileMode (int fd)
 *	dts_nameSize (char *fname)
 *	dts_nameMode (char *fname)
 *	dts_getBuffer (int fd, unsigned char **buf, long chunkSize, int tnum)
 *	dts_fileRead (int fd, void *vptr, int nbytes)
 *	dts_fileWrite (int fd, void *vptr, int nbytes)
 *	dts_fileCopy (char *in, char *out)
 *	dts_isDir (char *path)
 *	dts_isLink (char *path)
 *	dts_makePath (char *path, int isDir)
 *	dts_pathDir (char *path)
 *	dts_pathFname (char *path)
 *	dts_du (char *filename)
 *	dts_unlink (char *dir, int recurse, char *template)
 *	dts_dirSize (char *dir, char *template)
 *	dts_dirCopy (char *in, char *out)
 *	dts_statfs (char *dir, struct statfs *fs)
 *
 *
 *  @file       dtsFileUtil.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/10/09
 *
 *  @brief  DTS file utilities.
 *
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <dirent.h>
#include <string.h>

#include <sys/errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>			
 #include <sys/statvfs.h>
#include <sys/mman.h>		

/* needed for posix_fadvise()
*/
#define  _GNU_SOURCE
#define  _XOPEN_SOURCE 600
#include <fcntl.h>

#ifndef PATH_MAX
#define PATH_MAX	4096
#endif

#include "dts.h"


static int    statfs_count = 0;
static char   statfs_path[SZ_FNAME];
#ifdef LINUX
static struct statfs sf;
#else
static struct statvfs sf;
#endif



/**
 *  DTS_FOPEN -- Open the named file and return the file descriptor.
 *
 *  @brief  Open the named file and return the file descriptor.
 *  @fn     fd = dts_fopen (char *fname, char *mode)
 *
 *  @param  fname	file name to open
 *  @param  flags	file open mode
 *
 *  @return		FILE * descriptor
 */
FILE * 
dts_fopen (char *fname, char *mode)
{
    FILE *fp = (FILE *) NULL;

    if ((fp = fopen (fname, mode)) == (FILE *) NULL) {
	dtsErrLog (NULL, "failed to open file '%s' with mode '%s'\n", 
	    fname, mode);
	return ((FILE *) NULL);
    }
#ifdef NO_TRY
    flockfile (fp); 				/* create lockfile 	*/
#else
    while (ftrylockfile (fp)) {
	dtsErrLog (NULL, "failed to get lock on '%s'\n", fname);
	dtsSleep (1);
    }
#endif

    return (fp);
}


/**
 *  DTS_FCLOSE -- Close the file stream.
 *
 *  @brief  Close the file stream.
 *  @fn     status = dts_fclose (FILE *fp)
 *
 *  @param  fp		file stream to close
 *  @return		status
 */
int 
dts_fclose (FILE *fp)
{
    int  stat = 0;

    fflush (fp);
    funlockfile (fp); 				/* remove lockfile  	*/
    stat = fclose (fp);

    return (stat);
}


/**
 *  DTS_FILEOPEN -- Open the named file and return the file descriptor.
 *
 *  @brief  Open the named file and return the file descriptor.
 *  @fn     int dts_fileOpen (char *fname, int flags)
 *
 *  @param  fname	file name to open
 *  @param  flags	file open flags
 *
 *  @return		file descriptor
 */
int 
dts_fileOpen (char *fname, int flags)
{
    int fd = -1;

    if (flags & O_WRONLY) {
        if ((fd = open ((const char *)fname, flags, DTS_FILE_MODE)) < 0) {
	    fprintf (stderr, "dts_fileOpen: '%s': %s\n", 
		fname, strerror(errno));
	    return (-1);	/* cannot open file */
	}

    } else {
        if ((fd = open ((const char *)fname, flags)) < 0) {
	    fprintf (stderr, "dts_fileOpen: '%s': %s\n", 
		fname, strerror(errno));
	    return (-1);	/* cannot open file */
	}
    }

    return (fd);
}


/**
 *  DTS_FILECLOSE -- Close the file descriptor.
 *
 *  @brief  Close the file descriptor.
 *  @fn     dts_fileClose (int fd)
 *
 *  @param  fd		file descriptor to close
 *  @return		nothing
 */
int 
dts_fileClose (int fd)
{
    return (close (fd));
}


/**
 *  DTS_FILESYNC -- Sync the file descriptor.
 *
 *  @brief  Sync the file descriptor.
 *  @fn     dts_fileSync (int fd)
 *
 *  @param  fd		file descriptor to sync to disk
 *  @return		nothing
 */
int 
dts_fileSync (int fd)
{
    return (fsync (fd));
}


/**
 *  DTS_PREALLOC -- Pre-allocate the space for a file.
 *
 *  @brief  Pre-allocate the space for a file.
 *  @fn     int dts_preAlloc (char *fname, long fsize)
 *
 *  @param  fname	file name to create
 *  @param  fsize	size of the file
 *  @return		status code
 */
#define  DTS_ALLOC_SIZE		4096000

int 
dts_preAlloc (char *fname, long fsize)
{
    int   fd;


    if (access (fname, F_OK) == 0) {
	/*  Truncate an existing file.  Note that in this case the file
	 *  is not zero-filled.
	 */
	truncate (fname, fsize);

    } else if ((fd = open (fname, O_RDWR|O_CREAT, DTS_FILE_MODE)) > 0) {
	if (ftruncate (fd, (off_t) fsize) < 0) {
            close (fd);
	    return (ERR);
	}
        close (fd);
    }

    return (OK);
}


/**
 *  DTS_FILESIZE - Return the size (in bytes) of the specified file descriptor.
 *
 *  @brief  Return the size (in bytes) of the specified file descriptor.
 *  @fn     long dts_fileSize (int fd)
 *
 *  @param  fd		file descriptor
 *  @return		file size
 */
long
dts_fileSize (int fd)
{
    struct stat filstat;

    if ( fstat (fd, &filstat) < 0)
	return ( (long) -1 );
    else
	return ( (long) filstat.st_size );
}


/**
 *  DTS_FILEMODE - Return the protection mode of the specified file descriptor.
 *
 *  @brief  Return the protection mode of the specified file descriptor.
 *  @fn     long dts_fileMode (int fd)
 *
 *  @param  fd		file descriptor
 *  @return		file mode
 */
long
dts_fileMode (int fd)
{
    struct stat filstat;

    if ( fstat (fd, &filstat) < 0)
	return ( (long) -1 );
    else
	return ( (long) filstat.st_mode );
}


/**
 *  DTS_NAMESIZE - Return the size (in bytes) of the named file.
 *
 *  @brief  Return the size (in bytes) of the specified file descriptor.
 *  @fn     long dts_nameSize (char *fname)
 *
 *  @param  fname	file name
 *  @return		file size
 */
long
dts_nameSize (char *fname)
{
    struct stat filstat;

    int fd = open ((const char *)fname, O_RDONLY);

    if ( fstat (fd, &filstat) < 0) {
	close (fd);
	return ( (long) -1 );
    } else {
	close (fd);
	return ( (long) filstat.st_size );
    }
}


/**
 *  DTS_NAMEMODE - Return the protection mode of the named file.
 *
 *  @brief  Return the protection mode of the specified file descriptor.
 *  @fn     long dts_nameMode (char *fname)
 *
 *  @param  fname	file name
 *  @return		file mode
 */
long
dts_nameMode (char *fname)
{
    struct stat filstat;

    int fd = open ((const char *)fname, O_RDONLY);

    if ( fstat (fd, &filstat) < 0) {
	close (fd);
	return ( (long) -1 );
    } else {
	close (fd);
	return ( (long) filstat.st_mode );
    }
}


/**
 *  DTS_GETBUFFER -- Get a buffer of the specified size from the file.  Return
 *  the number of characters read.
 *
 *  @brief  Get a buffer of the specified size from the file.
 *  @fn     int dts_getBuffer (int fd, unsigned char **buf, 
 *		long chunkSize, int tnum)
 *
 *  @param  fd		file descriptor
 *  @param  buf		buffer to create
 *  @param  chunkSize	size of transfer chunk (bytes)
 *  @param  tnum	thread number
 *
 *  @return		file size
 */
int 
dts_getBuffer (int fd, unsigned char **buf, long chunkSize, int tnum)
{
    int val = tnum + 1;
    memset (*buf, val, chunkSize);

    return (chunkSize);

    /* return ( (long) pfileRead (fd, *buf, (int) chunkSize) ); */
}


/**
 *  DTS_FILEREAD -- Read exactly "n" bytes from a file descriptor. 
 *
 *  @brief  Read exactly "n" bytes from a file descriptor. 
 *  @fn     int dts_fileRead (int fd, void *vptr, int nbytes)
 *
 *  @param  fd		file descriptor
 *  @param  vptr	data buffer to be written
 *  @param  nbytes	number of bytes to write
 *  @return		number of bytes written
 */
int
dts_fileRead (int fd, void *vptr, int nbytes)
{
    char    *ptr = vptr;
    int     nread = 0, nleft = nbytes, nb = 0;

    while (nleft > 0) {
        if ((nb = read (fd, ptr, nleft)) < 0) {
            if (errno == EINTR)
                nb = 0;             /* and call read() again */
            else
                return(-1);
        } else if (nb == 0)
            break;                  /* EOF */

        nleft -= nb;
        ptr   += nb;
        nread += nb;
    }

    return (nread);                 /* return no. of bytes read */
}


/**
 *  DTS_FILEWRITE -- Write exactly "n" bytes to a file descriptor. 
 *
 *  @brief  Write exactly "n" bytes to a file descriptor. 
 *  @fn     int dts_fileWrite (int fd, void *vptr, int nbytes)
 *
 *  @param  fd		file descriptor
 *  @param  vptr	data buffer to be written
 *  @param  nbytes	number of bytes to write
 *  @return		number of bytes written
 */
int
dts_fileWrite (int fd, void *vptr, int nbytes)
{
    char    *ptr = vptr;
    int     nwritten = 0,  nleft = nbytes, nb = 0;

    dts_setNonBlock (fd);
#ifdef LINUX
    posix_fadvise (fd, (off_t) 0, (off_t) nbytes, POSIX_FADV_SEQUENTIAL);
#endif

    while (nleft > 0) {
        if ((nb = write (fd, ptr, nleft)) <= 0) {
            if (errno == EINTR)
                nb = 0;             /* and call write() again */
            else
                return(-1);         /* error */
        }
        nleft    -= nb;
        ptr      += nb;
        nwritten += nb;
    }

    return (nwritten);
}


/**
 *  DTS_FILECOPY -- Copy and input file to an output file.
 *
 *  @brief  Copy and input file to an output file.
 *  @fn     int dts_fileCopy (char *in, char *out)
 *
 *  @param  in		input file path
 *  @param  out		output file path
 *  @return		number of bytes written
 *
#define DTS_MMAP_COPY
 */
#define DTS_FCOPY_BUF		16384

int
dts_fileCopy (char *in, char *out)
{
    int     i, ifd, ofd;
    char    ip[SZ_LINE], op[SZ_LINE], *cp;
    struct  timeval t1 = {0, 0};
    size_t  sz;
#ifdef DTS_MMAP_COPY
    void   *src, *dest;
#else
    char    buf[DTS_FCOPY_BUF];
    size_t  nread;
#endif


    if (dts_isDir (in)) {
        dts_dirCopy (in, out);
        return (OK);
    }

    /*  Check for path equality, and simply return if we don't need to copy.
     */
    memset (ip, 0, SZ_LINE);
    for (i=0, cp=&ip[0]; in[i]; i++, cp++) {
	if (in[i] == '/' && in[i+1] == '/')
	    i++;
	*cp = in[i];
    }
    memset (op, 0, SZ_LINE);
    for (i=0, cp=&op[0]; out[i]; i++, cp++) {
	if (out[i] == '/' && out[i+1] == '/')
	    i++;
	*cp = out[i];
    }
    if (strcmp (ip, op) == 0)
	return (OK);

    /*  Copy the files.
     */
    if ((ifd = open (in, O_RDONLY)) < 0) {
        fprintf (stderr, "Error opening input file '%s': (%s)\n", 
	    in, strerror(errno));
	return (ERR);
    }
    if ((ofd = open (out, O_RDWR|O_TRUNC|O_CREAT, 0666)) < 0) {
        fprintf (stderr, "Error opening output file '%s': (%s)\n", 
	    out, strerror(errno));
	return (ERR);
    }

    sz = dts_fileSize (ifd);
    lseek (ifd, 0, SEEK_SET);
    lseek (ofd, 0, SEEK_SET);

    dts_tstart (&t1);

#ifndef DTS_MMAP_COPY
#ifdef LINUX
    posix_fadvise (ifd, (off_t) 0, (off_t) sz, POSIX_FADV_SEQUENTIAL);
    posix_fadvise (ofd, (off_t) 0, (off_t) sz, POSIX_FADV_SEQUENTIAL);
#endif

    memset (buf, 0, DTS_FCOPY_BUF);
    while ((nread = read (ifd, buf, DTS_FCOPY_BUF)) > 0) {
	write (ofd, buf, nread);
        memset (buf, 0, DTS_FCOPY_BUF);
    }

#else
    ftruncate (ofd, sz);
    if ((src = mmap (0, sz, PROT_READ, MAP_SHARED, ifd, 0)) == (void *) -1) {
        fprintf (stderr, "Error mapping input file: %s: %s\n", 
	    in, strerror (errno)); 
	return (ERR);
    }
    if ((dest = mmap (0, sz, PROT_WRITE, MAP_SHARED, ofd, 0)) == (void *) -1) {
        fprintf (stderr, "Error mapping ouput file: %s: %s\n", 
	    out, strerror (errno)); 
	return (ERR);
    }

    memcpy (dest, src, sz);

    munmap (src, sz);
    munmap (dest, sz);
#endif

    if (TIME_DEBUG)
	fprintf (stderr, "fileCopy time  = %g sec\n", dts_tstop (t1));

    fsync (ofd);			/* sync to the hardware disk	*/
    close (ifd);
    close (ofd);

    return (OK);
}


/**
 *  DTS_ISDIR -- Check whether a path represents a directory.
 *
 *  @brief  Check whether a path represents a directory.
 *  @fn     int dts_isDir (char *path)
 *
 *  @param  path	pathname to be checked
 *  @return		1 (one) if path is a directory, 0 (zero) otherise
 */
int
dts_isDir (char *path)
{
    struct stat stb;
    int     res = -1;

    if ((res = stat(path, &stb)) == 0)
	return ((int) S_ISDIR (stb.st_mode));
    return (0);
}


/**
 *  DTS_ISLINK -- Check whether a path represents a symbolic link.
 *
 *  @brief  Check whether a path represents a symbolic link.
 *  @fn     int dts_isLink (char *path)
 *
 *  @param  path	pathname to be checked
 *  @return		1 (one) if path is a symbolic link, non-zero otherise
 */
int
dts_isLink (char *path)
{
    struct stat stb;

    if (! lstat(path, &stb))
        return (S_ISLNK (stb.st_mode));
    else
        return (-1);
}


/**
 *  DTS_READLINK -- Read the target of a symlink.
 *
 *  @brief  Read the target of a symlink.
 *  @fn     char *dts_readLink (char *ipath)
 *
 *  @param  ipath	input pathname to link
 *  @return		path to target of symlink
 */
char *
dts_readLink (char *ipath)
{
    char  opath[SZ_PATH];

    memset (opath, 0, SZ_PATH);
    if (readlink (ipath, opath, (size_t) SZ_PATH) > 0)
        /*return (dts_strbuf (opath));*/
        return (strdup (opath)); 	/* caller needs to free pointer */
        
    return (NULL);
}


/**
 *  DTS_MAKEPATH -- Create the specified path, one directory at a time.
 *
 *  @brief	Create the specified path, one directory at a time.
 *  @fn 	stat =  dts_makePath (char *path, int isDir)
 *
 *  @param  path	Path to create
 *  @param  isDir	Is final element a directory?
 *  @return		1 (one) if path can be created
 */
int
dts_makePath (char *path, int isDir)
{
    char *ip, *op, dir[SZ_PATH];
    struct stat st;


    memset (dir, 0, SZ_PATH);
    ip = path;
    op = dir;

    while (*ip) {
	if (*ip == '/') {
	    /*  If the directory path doesn't exist, create it.  Otherwise
	     *  make sure the final element is really a directory.
	     */
	    if (access (dir, R_OK|W_OK|X_OK) < 0)
		mkdir (dir, DTS_DIR_MODE);
	    else if ( stat (dir, &st) < 0 || (!S_ISDIR(st.st_mode)) )
		return (ERR);
	}
	*op++ = *ip++;
    }

    if (isDir) {
	if (access (dir, R_OK|W_OK|X_OK) < 0)
	    mkdir (dir, DTS_DIR_MODE);
	else if ( stat (dir, &st) < 0 || (!S_ISDIR(st.st_mode)) )
	    return (ERR);
    }

    return (OK);
}


/**
 *  DTS_PATHDIR -- Return the directory portion of a file pathname.
 *
 *  @brief  Return the directory portion of a file pathname.
 *  @fn     dir = dts_pathDir (char *path)
 *
 *  @param  path	long path name
 *  @return		parent directory
 */
char * 
dts_pathDir (char *path)
{
    char  *ip, *dp, *pp, dir[SZ_PATH];
    int    len;


    if (!path || !strchr(path, (int) '/'))	/* no directory	specified  */
	return ("./");

    len = strlen (path);
    memset (dir, 0, SZ_PATH);
    for (ip = &path[len-1]; len; len--) {
	if (*ip != '/')		/* find trailing '/'	*/
	    if (len > 1) ip--;
    }

    if (strchr (path, (int)':')) {
        for (pp=path; *pp && *pp != ':'; pp++)
	    ;
        for (dp=dir, pp++; pp != ip; )
	    *dp++ = *pp++;
    } else {
        for (dp=dir, pp=path; pp != ip; )
	    *dp++ = *pp++;
    }

    return (dir[0] ? dts_strbuf (dir) : "/");
}


/**
 *  DTS_PATHFNAME -- Return the filename portion of a file pathname.
 *
 *  @brief  Return the filename portion of a file pathname.
 *  @fn     dir = dts_pathFname (char *path)
 *
 *  @param  path	long path name
 *  @return		filename
 */
char * 
dts_pathFname (char *path)
{
    char *ip, fname[SZ_FNAME];
    int   len;


    if (!path)
	return ("");
    len = strlen (path);
    if (path[len-1] == '/')			/* no filename given	*/
	return ("");

    memset (fname, 0, SZ_FNAME);
    for (ip = &path[len-1]; len; len--) {
	if (*ip != '/')
	    ip--;
    }
    strcpy (fname, ++ip);

    return (fname[0] ? dts_strbuf (fname) : "/");
}


/**
 *  DTS_DU -- A simple 'du' function for a file or directory tree.
 *
 *  @brief  Return the size of a file or directory path in bytes.
 *  @fn     long dts_du (char *filename)
 *
 *  @param  filename	filename to be checked
 *  @return		size of file
 */
long 
dts_du (char *filename)
{
    struct stat statbuf;
    long  sum = 0L;
    int   len;


    if ((lstat (filename, &statbuf)) != 0)
	return ((long) 0L);
    else
        sum = statbuf.st_size;

    /*  Don't add in stuff pointed to by symbolic links.  Do descend
     *  directories.
     */
    if (S_ISLNK(statbuf.st_mode)) {
        if (S_ISDIR(statbuf.st_mode)) 
	    sum = 0L;
	else {
	    char *target = dts_readLink (filename);
	    sum = dts_du (target);
	}

    } else if (S_ISDIR(statbuf.st_mode)) {
	DIR *dir;
	struct dirent *entry;

	if (! (dir = opendir (filename)) )
	    return ((long) 0L);

	len = strlen (filename);
	if (filename[len - 1] == '/')
	    filename[--len] = '\0';

	while ((entry = readdir (dir))) {
	    char newfile[PATH_MAX + 1];
	    char *name = entry->d_name;

	    if ((strcmp (name, "..") == 0) || (strcmp (name, ".") == 0))
		continue;

	    if (len + strlen (name) + 1 > PATH_MAX) {
		fprintf (stderr, "Warning: pathname too long '%s'\n", name);
	        return ((long) 0L);
	    }
	    sprintf (newfile, "%s/%s", filename, name);

	    sum += dts_du (newfile);
	}
	closedir (dir);
    }

    return (sum);
}


/**
 *  DTS_UNLINK -- Remove all matching files in a directory or directory
 *  tree.  We chdir to each directory to minimize path searches.
 *
 *  @brief	Remove all matching file in a directory (tree).
 *  @fn		stat = dts_unlink (char *dir, int recurse, char *template)
 *
 *  @param  dir		directory to remove
 *  @param  recurse	descend into subdirs?
 *  @param  template	file-matching template
 *  @return		status
 */
int 
dts_unlink (char *dir, int recurse, char *template)
{
    DIR	  *dp;
    struct  dirent *entry;
    struct  stat st;
    char  newpath[SZ_PATH];


    if ((dp = opendir (dir)) == (DIR *) NULL)
        return (ERR);

    /* Descend into the subdirectory.
     *
     *  FIXME -- this changes the effetive cwd of the task, may not be safe
     *
     */
    if (strcmp (dir, ".") != 0) {
        if (chdir (dir) == ERR) {
    	    closedir (dp);
    	    return (1);
        }
    }

    /* Scan through the directory.
     */
    while ( (entry = readdir (dp)) != (struct dirent *) NULL ) {
        if (strcmp (entry->d_name, ".") == 0)
            continue;
        if (strcmp (entry->d_name, "..") == 0)
            continue;

        if (template && !dts_patMatch (entry->d_name, template))
            continue;

        (void) stat (entry->d_name, &st);
        if (S_ISDIR(st.st_mode)) {

            memset (newpath, 0, SZ_PATH);
            sprintf (newpath, "%s/%s", dir, entry->d_name);

            /* If we're recursively deleting, descend into the directory
            ** and delete the contents.
            */
            if (recurse)
                dts_unlink (newpath, recurse, "*");

            /* Delete the directory.
            */
            /* fprintf (stderr, "%s\n", newpath); */
            if (rmdir (newpath) < 0)
                return (1);

        } else {
            /* fprintf (stderr, "%s/%s\n", dir, entry->d_name); */
            if (unlink (entry->d_name) < 0)
                return (1);
        }
    }

    /*  Finished now remove the root directory.
     */
    closedir (dp);			/* clean up	*/
    if (rmdir (dir) < 0)
	return (1);

    return (0);
}


/**
 *  DTS_DIRSIZE -- Get the cumulative size of all files in a directory tree.
 *
 *  @brief	Get the cumulative size of all files in a directory tree.
 *  @fn		size = dts_dirSize (char *dir, char *template)
 *
 *  @param  dir		directory to size
 *  @param  template	file-matching template
 *  @return		status
 */
long 
dts_dirSize (char *dir, char *template)
{
    DIR	   *dp;
    struct  dirent *entry;
    struct  stat st;
    char    newpath[SZ_PATH];

    long    totsize = 0;


    /* Scan through the directory.
     */
    if ((dp = opendir (dir)) == (DIR *) NULL)
        return (ERR);

    while ( (entry = readdir (dp)) != (struct dirent *) NULL ) {

	if (strcmp (entry->d_name, ".") == 0 || 	/* skip dot dirs */
	    strcmp (entry->d_name, "..") == 0)
	        continue;

	if (template && !dts_patMatch (entry->d_name, template))
	    continue;

        (void) stat (entry->d_name, &st);
	if (S_ISDIR(st.st_mode)) {

    	    memset (newpath, 0, SZ_PATH);
    	    sprintf (newpath, "%s/%s", dir, entry->d_name);

	    /* Recursively check the subdirs.
	    */
	    totsize += dts_dirSize (newpath, template);

	} else {
	    totsize += st.st_size;
        }
    }

    closedir (dp);			/* clean up	*/
    return (totsize);
}


/**
 *  DTS_DIRCOPY -- Recursively copy a directory to a new location, preserving
 *  symlinks and file permissions.
 *
 *  @brief	Recursively copy a directory to a new location.
 *  @fn		stat = dts_dirCopy (char *in, char *out)
 *
 *  @param  in		input directory to copy
 *  @param  out		output directory path
 *  @return		status
 */
int 
dts_dirCopy (char *in, char *out)
{
    DIR	  *dp;
    struct  dirent *entry;
    struct  stat st;
    char  src[SZ_PATH], dest[SZ_PATH];


    if ((dp = opendir (in)) == (DIR *) NULL)
        return (ERR);

    /* Descend into the subdirectory.
     *
     *  FIXME -- this changes the effective cwd of the task, may not be safe
     *
     */
    if (strcmp (in, ".") != 0) {
        if (chdir (in) == ERR) {
    	    closedir (dp);
    	    return (1);
        }
    }

	    
    if (access (out, F_OK) < 0)
	mkdir (out, DTS_DIR_MODE);
		
    /* Scan through the directory.
     */
    while ( (entry = readdir (dp)) != (struct dirent *) NULL ) {

	if (strcmp (entry->d_name, ".") == 0)
	    continue;
	if (strcmp (entry->d_name, "..") == 0)
	    continue;

	memset (src, 0, SZ_PATH);
	sprintf (src, "%s/%s", in, entry->d_name);

	memset (dest, 0, SZ_PATH);
	sprintf (dest, "%s/%s", out, entry->d_name);

	/*  
	 *  FIXME -- Need to implement symlinks and modes
        (void) stat (entry->d_name, &st);
	 */
        (void) stat (src, &st);
	if (S_ISDIR(st.st_mode)) {

	    /* Descend into the directory and copy the contents.
	    */
	    if (access (dest, F_OK) < 0)
		mkdir (dest, DTS_DIR_MODE);
	    dts_dirCopy (src, dest);

	} else
	    dts_fileCopy (src, dest);
    }

    closedir (dp);			/* clean up	*/
    return (0);
}


/**
 *  DTS_STATFS -- Get file system statistics, i.e. disk space used/free on
 *  a system.
 *
 *  @brief	Get file system statistics.
 *  @fn		stat = dts_statfs (char *dir, struct statfs *fs)
 *
 *  @param  dir		directory path
 *  @param  fs		statfs structure
 *  @return		status
 */
int
#ifdef LINUX
dts_statfs (char *dir, struct statfs *fs)
#else
dts_statvfs (char *dir, struct statvfs *fs)
#endif
{
    double free = 0.0;

    if (statfs_count == 0) {
	memset (statfs_path, 0, SZ_FNAME);
	strcpy (statfs_path, dir);
#ifdef LINUX
	statfs (dir, &sf);
#else
	statvfs (dir, &sf);
#endif
    } else {
	free = ((double)sf.f_bavail/((double)sf.f_bavail + (double)sf.f_bfree));
	if (free < 0.1) {
	    /* Always run statfs() if the disk is more than 90% full so we
	     * get an accurate number.
	     */
#ifdef LINUX
	    statfs (dir, fs);
#else
	    statvfs (dir, fs);
#endif
	} else if (strncasecmp (statfs_path, dir, 6) == 0) {
#ifdef LINUX
	    memcpy (fs, &sf, sizeof (struct statfs));
#else
	    memcpy (fs, &sf, sizeof (struct statvfs));
#endif
	}
    }
    statfs_count++;
    statfs_count %= 3;

    return (OK);
}
