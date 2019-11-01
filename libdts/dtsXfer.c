/**
 *  DTSHOTTO.C -- Client-callable DTS commands to transfer files TO a remote
 *  host machine..
 *
 *  @brief      Client-callable DTS commands to transfer files TO a remote
 *  host machine..
 *
 *  @file       dtsXfer.c
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
#include <dirent.h>
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



extern  DTS *dts;
extern  int  dts_monitor;


static int  nthreads 		= 0;
static int  xfer_port 		= DEF_XFER_PORT;
      	     
static char src_host[SZ_PATH], dest_host[SZ_PATH], msg_host[SZ_PATH];
static char s_url[SZ_PATH],    d_url[SZ_PATH];
static char s_path[SZ_PATH],   d_path[SZ_PATH];
static char s_dir[SZ_PATH],    d_dir[SZ_PATH];
static char s_fname[SZ_PATH],  d_fname[SZ_PATH];




/**
 *  DTS_HOSTTO -- Move a file from A 'to' B  (i.e. a "push" or "give").
 *
 *  @brief  Push a file to a remote host.
 *  @fn     stat = dts_hostTo (char *host, int port, int method, int rate,
 *			int loPort, int hiPort,
 *			int threads, int mode, int argc, char *argv[], 
 *			xferStat *xfs);
 *
 *  @param  host	host machine name (or IP string)
 *  @param  port	local DTS command port
 *  @param  method	transport method
 *  @param  rate	transfer rate (Mbps)
 *  @param  loPort	low DTS transfer port
 *  @param  hiPort	high DTS transfer port
 *  @param  threads	number of transfer threads
 *  @param  mode	transfer mode
 *  @param  argc	argument counter
 *  @param  argv	argument vector
 *  @param  xfs		transfer stats
 *  @return		1 (one) if DTS succeeds.
 */

int
dts_hostTo (char *host, int port, int method, int rate, int loPort, int hiPort, 
		int threads, int mode, int argc, char *argv[], xferStat *xfs)
{
    char *shost = src_host, *dhost = dest_host, *mhost = msg_host;
    char *func = NULL, *path = NULL, *path_A = NULL, *path_B = NULL;
    char *localIP = dts_getLocalIP();
    int   res, fsize, fmode, len = strlen (localIP);
    int   srcLocal = 1, dstLocal = 0, isDir = 0, verbose = 0;
    struct stat st;


    memset (s_url,     0, SZ_PATH); 		/* Initialize		*/
    memset (d_url,     0, SZ_PATH); 
    memset (msg_host,  0, SZ_PATH); 
    memset (src_host,  0, SZ_PATH); 
    memset (dest_host, 0, SZ_PATH); 
    memset (s_dir,     0, SZ_PATH); 
    memset (d_dir,     0, SZ_PATH); 
    memset (s_path,    0, SZ_PATH); 
    memset (d_path,    0, SZ_PATH); 
    memset (s_fname,   0, SZ_PATH); 
    memset (d_fname,   0, SZ_PATH); 

    nthreads = threads;
    xfer_port = loPort;		/* first transfer port		*/

/*
    sprintf (src_host, "%s", dts_getLocalIP() );
    sprintf (dest_host, "%s", dts_resolveHost (host) );
*/
    sprintf (src_host, "%s:%d", dts->serverIP, dts->serverPort);
    sprintf (dest_host, "%s", host);
    sprintf (msg_host, "%s", host);


    /* Process the remaining argument vector.
    */
    dts_xferParseArgs (argc, argv, &nthreads, &port, &verbose,
	&path_A, &path_B);

    /* Sort out the pathnames on each side of the transfer.
    */
    dts_xferParsePaths (host, &path_A, &path_B, &srcLocal, &dstLocal,
	&shost, &dhost, &mhost, &path);

    strcpy (s_dir, dts_pathDir (path_A));
    strcpy (d_dir, dts_pathDir (path_B));
    strcpy (s_fname, dts_pathFname (path_A));
    strcpy (d_fname, dts_pathFname (path_B));
    sprintf (s_path, "%s/%s", s_dir, s_fname);
    sprintf (d_path, "%s/%s", d_dir, d_fname);
    isDir = dts_isDir (s_path);

    /*
    srcLocal = (strcmp (dts_getLocalIP(), src_host) == 0);
    dstLocal = (strcmp (dts_getLocalIP(), dest_host) == 0);
    */
    if (strncmp (localIP, src_host, len) != 0 && 
	strncmp (localIP, dest_host, len) != 0)
	    port = DTS_PORT;

    if (XFER_DEBUG) {
	fprintf (stderr, "    host = %s\n", host);
	fprintf (stderr, "nthreads = %d\n", nthreads);
	fprintf (stderr, "    port = %d  (%d,%d)\n", port, loPort, hiPort);
	fprintf (stderr, "xfer_ort = %d\n", xfer_port);
	fprintf (stderr, "    rate = %d\n", rate);
	fprintf (stderr, "  path_A = %s\n", path_A);
	fprintf (stderr, "  path_B = %s\n", path_B);
	fprintf (stderr, "srcLocal = %d\n", srcLocal);
	fprintf (stderr, "dstLocal = %d\n", dstLocal);
	fprintf (stderr, "   isDir = %d\n", isDir);
	fprintf (stderr, "   s_dir = %s\n", s_dir);
	fprintf (stderr, "   d_dir = %s\n", d_dir);
	fprintf (stderr, " s_fname = %s\n", s_fname);
	fprintf (stderr, " d_fname = %s\n", d_fname);
	fprintf (stderr, "  s_path = %s\n", s_path);
	fprintf (stderr, "  d_path = %s\n", d_path);
	fprintf (stderr, "    path = %s\n", path);
	fprintf (stderr, "src_host = %s\n", src_host);
	fprintf (stderr, "dst_host = %s\n", dest_host);
    }

    /*  Sanity checks.
    */
    if (argc <= 0) {
	fprintf (stderr, "Error: No filename specified\n");
	return (ERR);

    } else if (srcLocal && access (s_path, R_OK) < 0) {
	fprintf (stderr, "Error: Cannot access local file '%s'\n", s_path);
	return (ERR);

    } else if (srcLocal && stat (s_path, &st) < 0) {
	fprintf (stderr, "Error: Cannot stat() file '%s'\n", s_path);
	return (ERR);

    } else if (!src_host[0]) {
	fprintf (stderr, "Error: No source host specified\n");
	return (ERR);

    } else if (!dest_host[0]) {
	fprintf (stderr, "Error: No dest host specified\n");
	return (ERR);

    }
    stat (s_path, &st);
    fsize = (int) dts_du (s_path);
    fmode = (int) st.st_mode;

    dts_cmdInit();			/* initialize static variables	*/

    
    /* Set calling parameters.
    */
    memset (s_url, 0, SZ_PATH); 
    if (strchr (src_host, (int)':'))
        sprintf (s_url, "http://%s/RPC2", src_host);
    else
        sprintf (s_url, "http://%s:%d/RPC2", src_host, port);

    memset (d_url, 0, SZ_PATH); 
    if (strchr (dest_host, (int)':'))
        sprintf (d_url, "http://%s/RPC2", dest_host);
    else
        sprintf (d_url, "http://%s:%d/RPC2", dest_host, DTS_PORT);


    /* If mode is PUSH, we send a command to the DTSD on the source machine,
    ** otherwise the command is sent to the dest machine. 
    */
    if (mode == XFER_PUSH) {
        func   = "xferPushFile";
        sprintf (src_host, "%s:%d", dts_getLocalIP(), port );
	host = src_host;
	
        if (XFER_DEBUG)
	    fprintf (stderr, "hostTo  xferPushFile = %d  (%s)\n", port, host);
    } else {
        func   = "xferPullFile";
	host = dest_host;

        if (XFER_DEBUG)
	    fprintf (stderr, "hostTo  xferPullFile = %d  (%s)\n", port, host);
    }

    if (XFER_DEBUG) 
        dtsLog (dts, "%6.6s >  XFER: hostTo: func=%s src=%s dest=%s\n", 
	    dts_queueFromPath(s_dir), func, src_host, dest_host);


    /* Finally, do the transfer and return the result.
    */
    if (dts_isDir (path) > 0) {
	char prefix[PATH_MAX], root[PATH_MAX];

	memset (prefix, 0, PATH_MAX);		/* save static copy of prefix */
	strcpy (prefix, s_dir);
	memset (root, 0, PATH_MAX);		/* save static copy of root   */
	strcpy (root, d_dir);

        if (dts_hostMkdir (host, d_dir) == ERR) { /* create start dir         */
	    fprintf (stderr, "Cannot create host dir '%s'\n", d_dir);
	    return (ERR);
	}

        res = dts_xferDirTo (host, func, method, rate, path, root, prefix, xfs);

        if (XFER_DEBUG && res != OK)
	    fprintf (stderr, "hostTo  xferDirTo fails\n");

    } else {
        res = dts_xferFile (host, func, method, rate, path, fsize, fmode, xfs);

        if (XFER_DEBUG && res != OK)
	    fprintf (stderr, "hostTo  xferFile fails\n");
    }

    return (res);
}


/**
 *  DTS_HOSTFROM -- Pull a file from A to B.
 *
 *  @brief  Pull a file from a remote host.
 *  @fn     stat = dts_hostFrom (char *host, int port, int method, int rate,
 *			int loPort, int hiPort, 
 *			int threads, int mode, int argc, char *argv[], 
 *			xferStat *xfs);
 *
 *  @param  host	host machine name (or IP string)
 *  @param  port	local DTS command port
 *  @param  method	transport method
 *  @param  rate	transport rate (Mbps)
 *  @param  loPort	low DTS transfer port
 *  @param  hiPort	high DTS transfer port
 *  @param  threads	number of transfer threads
 *  @param  mode	transfer mode
 *  @param  argc	argument counter
 *  @param  argv	argument vector
 *  @param  xfs		transfer stats
 *  @return		1 (one) if DTS succeeds.
 */

int
dts_hostFrom (char *host, int port, int method, int rate, 
		int loPort, int hiPort, 
		int threads, int mode, int argc, char *argv[], xferStat *xfs)
{
    char  *func = NULL, *path = NULL, *path_A = NULL, *path_B = NULL;
    char  *localIP = dts_getLocalIP();
    int   res, fsize, fmode, len = strlen (localIP);
    int   srcLocal = 0, dstLocal = 0, verbose = 0;


    memset (s_url,     0, SZ_PATH); 
    memset (d_url,     0, SZ_PATH); 
    memset (msg_host,  0, SZ_PATH); 
    memset (src_host,  0, SZ_PATH); 
    memset (dest_host, 0, SZ_PATH); 
    memset (s_dir,     0, SZ_PATH); 
    memset (d_dir,     0, SZ_PATH); 
    memset (s_path,    0, SZ_PATH); 
    memset (d_path,    0, SZ_PATH); 
    memset (s_fname,   0, SZ_PATH); 
    memset (d_fname,   0, SZ_PATH); 

    nthreads = threads;
    xfer_port = loPort;		/* first transfer port		*/

    sprintf (src_host, "%s", dts_resolveHost (host) );
    sprintf (dest_host, "%s", dts_getLocalIP() );
    sprintf (msg_host, "%s", host);


    /* Process the remaining argument vector.
    */
    dts_xferParseArgs (argc, &argv[0], &nthreads, &port, &verbose,
	&path_A, &path_B);

    /* Sort out the pathnames on each side of the transfer.
    dts_xferParsePaths (host, &path_A, &path_B, &srcLocal, &dstLocal, 
        &shost, &dhost, &mhost, &path);
    */

    strcpy (s_dir, dts_pathDir (path_A));
    strcpy (d_dir, dts_pathDir (path_B));
    strcpy (s_fname, dts_pathFname (path_A));
    strcpy (d_fname, dts_pathFname (path_B));
    sprintf (s_path, "%s/%s", s_dir, s_fname);
    sprintf (d_path, "%s/%s", d_dir, d_fname);

    srcLocal = (strncmp (localIP, src_host, len) == 0);
    dstLocal = (strncmp (localIP, dest_host, len) == 0);
    if (strncmp (localIP, src_host, len) != 0 && 
        strncmp (localIP, dest_host, len) != 0)
            port = DTS_PORT;
    if (!d_fname[0])
	strcpy (d_fname, s_fname);


    if (XFER_DEBUG) {
        fprintf (stderr, "    host = %s\n", host);
        fprintf (stderr, "  path_A = %s\n", path_A);
        fprintf (stderr, "  path_B = %s\n", path_B);
        fprintf (stderr, "nthreads = %d\n", nthreads);
        fprintf (stderr, "    port = %d\n", port);
	fprintf (stderr, "xfer_ort = %d\n", xfer_port);
        fprintf (stderr, "srcLocal = %d\n", srcLocal);
        fprintf (stderr, "dstLocal = %d\n", dstLocal);
        fprintf (stderr, "   isDir = %d\n", 0);
        fprintf (stderr, "srcLocal = %d  dstLocal = %d  isDir = %d\n", 
						srcLocal, dstLocal, 0);
        fprintf (stderr, "   s_dir = %s\n", s_dir);
        fprintf (stderr, "   d_dir = %s\n", d_dir);
        fprintf (stderr, " s_fname = %s\n", s_fname);
        fprintf (stderr, " d_fname = %s\n", d_fname);
        fprintf (stderr, "  s_path = %s\n", s_path);
        fprintf (stderr, "  d_path = %s\n", d_path);
        fprintf (stderr, "    path = %s\n", path);
	fprintf (stderr, "src_host = %s\n", src_host);
	fprintf (stderr, "dst_host = %s\n", dest_host);
	fprintf (stderr, "msg_host = %s\n", msg_host);
    }

    if (!path)
	path = s_path;
    


    /*  Sanity checks.
    */
    if (argc <= 0) {
	fprintf (stderr, "Error: No filename specified\n");
	return (ERR);

    } else if ( dts_hostAccess (msg_host, s_path, R_OK) != OK) {
	fprintf (stderr, "Error: Cannot access file '%s'\n", path);
	return (ERR);

    } else if ( (fsize = dts_hostFSize (msg_host, s_path)) < 0) {
	fprintf (stderr, "Error: Cannot get filesize for '%s'\n", path);
	return (ERR);

    } else if ( (fmode = dts_hostFMode (msg_host, s_path)) < 0) {
	fprintf (stderr, "Error: Cannot get file mode for '%s'\n", path);
	return (ERR);
    }

    dts_cmdInit();			/* initialize static variables	*/

    if (XFER_DEBUG) 
	dtsLog (dts, "dts_hostPull: %s:%s -> .\n", dts_resolveHost(host), path);


    /* Set calling parameters.
    */
    memset (s_url, 0, SZ_PATH); 
    if (strchr (src_host, (int)':'))
        sprintf (s_url, "http://%s/RPC2", src_host);
    else
        sprintf (s_url, "http://%s:%d/RPC2", src_host, DTS_PORT);

    memset (d_url, 0, SZ_PATH); 
    if (strchr (dest_host, (int)':'))
        sprintf (d_url, "http://%s/RPC2", dest_host);
    else
        sprintf (d_url, "http://%s:%d/RPC2", dest_host, port);



    /* If mode is PUSH, we send a command to the DTSD on the source machine,
    ** otherwise the command is sent to the dest machine. 
    */
    if (mode == XFER_PUSH) {
        func   = "xferPushFile";
        host = src_host;
    } else {
        func   = "xferPullFile";
        // sprintf (dest_host, "%s:%d", dts_getLocalIP(), port );  FIXME -
        // for remote->remote xfers
        host = dest_host;
    }

    if (XFER_DEBUG) 
        dtsLog (dts, "%6.6s <  XFER: hostFrom: func=%s src=%s dest=%s\n", 
	    dts_queueFromPath(d_dir), func, src_host, dest_host);


    /* Finally, do the transfer and return the result.
    */
    if (dts_hostIsDir (src_host, path) > 0) {
        char prefix[PATH_MAX];

        memset (prefix, 0, PATH_MAX);
	strcpy (prefix, d_dir);

	if (dstLocal) { 			/* create start dir 	      */
	    if (dts_localMkdir (d_path) == ERR)
		return (ERR);
	} else {
            strcpy (prefix, s_dir);           /* save static copy of prefix */
	    if (dts_hostMkdir (dest_host, d_dir) == ERR)
		return (ERR);
	}

        res = dts_xferDirFrom (src_host, dest_host, dstLocal,
	    func, method, rate, path, s_dir, prefix, xfs);

    } else {
        res = dts_xferFile (host, func, method, rate, path, fsize, fmode, xfs);
    }

    return (res);
}


/**
 *  DTS_XFERDIRTO -- Recursively transfer a directory between hosts.
 *
 *  @brief  Recursively transfer a directory between hosts.
 *  @fn     stat = dts_xferDirTo (char *host, char *func, int method,
 *			int rate, char *path, char *root, char *prefix, 
 *			xferStat *xs)
 *
 *  @param  host	host to call
 *  @param  func	RPC method to call
 *  @param  method	transport method
 *  @param  rate	transfer rate
 *  @param  path	path to file
 *  @param  root	destination root
 *  @param  prefix	source prefix dir
 *  @param  xfs		transfer stats
 *  @return		1 (one) if DTS succeeds.
 */
int
dts_xferDirTo (char *host, char *func, int method, int rate, char *path, 
	char *root, char *prefix, xferStat *xs)
{
    DIR   *dp;
    struct dirent *entry;
    char   rpath[PATH_MAX];
    char   newfile[PATH_MAX + 1];
    int    len , plen = strlen (prefix), fsize, fmode;


    /*  Open the directory.
    */
    if (! (dp = opendir (path)) ) {
        dtsErrLog (NULL, "Cannot open dir '%s'\n", path);
        return (ERR);
    }

    len = strlen (path);
    if (plen && path[len - 1] == '/')
        path[--len] = '\0';

    while ((entry = readdir (dp))) {
        char *name = entry->d_name;

        if ((strcmp (name, "..") == 0) || (strcmp (name, ".") == 0))
            continue;

        memset (newfile, 0, PATH_MAX);
        memset (rpath, 0, PATH_MAX);

        sprintf (newfile, "%s/%s", path, name);
        sprintf (rpath, "%s/%s", root, &newfile[plen]);

	if (dts_isDir (newfile) > 0) {
	    if (dts_xferDirTo (host, func, method, rate, newfile, root, 
		prefix, xs) == ERR) {
		    closedir (dp);
		    fprintf (stderr, 
			"xferDirTo: isDir xferDirTo() fails on '%s'\n",
		        newfile);
		    return (ERR);
	    }

	} else {
	    /* Update the transfer parameters.
	    */
	    strcpy (s_dir, dts_pathDir (newfile));
	    strcpy (d_dir, dts_pathDir (rpath));
	    strcpy (s_fname, dts_pathFname (newfile));
	    strcpy (d_fname, dts_pathFname (rpath));
	    sprintf (s_path, "%s", newfile);
	    sprintf (d_path, "%s", rpath);

	    /* Create leading directory components.
	    */
	    if (dts_hostMkdir (host, d_dir) == ERR) {
		closedir (dp);
		fprintf (stderr, "hostMkdir(): xferFile() fails\n");
		return (ERR);
	    }
        
	    fsize = (int) dts_nameSize (newfile);
	    fmode = (mode_t) dts_nameMode (newfile);
            if (dts_xferFile (host, func, method, rate, newfile, 
		fsize, fmode, xs) == ERR) {
		    closedir (dp);
		    fprintf (stderr, "xferDirTo: xferFile() fails on '%s'\n",
		        newfile);
		    return (ERR);
	    }

#ifdef DTS_CHMOD
	    /* Set the remote file mode
	    */
	    if (dts_hostChmod (host, newfile, fmode) == ERR) {
		closedir (dp);
		fprintf (stderr, "xferDirTo: hostChmod() fails '%s' to %d\n",
		    newfile, fmode);
		return (ERR);
	    }
#endif
	}
    }
    closedir (dp);

    return (OK);
}


/**
 *  DTS_XFERDIRFROM -- Recursively transfer a directory between hosts.
 *
 *  @brief  Recursively transfer a directory between hosts.
 *  @fn     stat = dts_xferDirFrom (char *srcHost, char *dstHost,
 *  			int dstLocal, char *func, int method, int rate,
 *			char *path, char *root, char *prefix, xferStat *xs)
 *
 *  @param  srcHost	source host
 *  @param  dstHost	destination host
 *  @param  dstLocal	is dest the local machine?
 *  @param  func	RPC method to call
 *  @param  method	transport method
 *  @param  rate	transfer rate
 *  @param  path	path to file
 *  @param  root	destination root
 *  @param  prefix	destination path prefix
 *  @param  xfs		transfer stats
 *  @return		1 (one) if DTS succeeds.
 */
int
dts_xferDirFrom (char *srcHost, char *dstHost, int dstLocal, char *func, 
		int method, int rate, char *path, char *root, 
		char *prefix, xferStat *xs)
{
    char  *list = NULL, *ip, *op, name[SZ_FNAME];
    char  newfile[PATH_MAX];
    char  lpath[PATH_MAX];
    int   isDir, len;


    /* Get a directory listing from the source host.
    */
    if ( (list = dts_hostDir (srcHost, path, 0)) == NULL ) {
	dtsErrLog (NULL, "Warning: Can't list '%s'\n", path);
	if (list) free (list);
	return (ERR);
    }
    len = strlen (list);

    for (ip=list; *ip && (ip - len) <= list; ) {
	memset ((op = name), 0, SZ_FNAME);

	while (*ip && *ip == '\n')
	    ip++;
	while (*ip && (*ip != '/' && *ip != '\n'))
	    *op++ = *ip++;

	isDir = ((*ip && *ip == '/') ? 1 : 0);
	if (*ip == '/')
	    ip++;

	memset (newfile, 0, PATH_MAX);
	memset (lpath, 0, PATH_MAX);
        sprintf (newfile, "%s/%s", path, name);
        sprintf (lpath, "%s%s/%s", prefix, path, name);

	if (isDir) {
	    if (XFER_DEBUG)
	        fprintf (stderr, "dir\t%s -> %s\n", newfile, lpath);
	    dts_xferDirFrom (srcHost, dstHost, dstLocal, func, 
			method, rate, newfile, root, prefix, xs);
	} else {
	    int  fsize = dts_hostFSize (srcHost, newfile);
	    mode_t  fmode = (mode_t) dts_hostFMode (srcHost, newfile);


	    /* Update the transfer parameters.
	    */
	    strcpy (s_dir, dts_pathDir (newfile));
	    strcpy (d_dir, dts_pathDir (lpath));
	    strcpy (s_fname, dts_pathFname (newfile));
	    strcpy (d_fname, dts_pathFname (lpath));
	    sprintf (s_path, "%s", newfile);
	    sprintf (d_path, "%s", lpath);

	    if (XFER_DEBUG) {
	        fprintf (stderr, "file\t%s -> %s\n", newfile, lpath);
	        fprintf (stderr, "fsize\t%d -> fmode %d\n", fsize, (int) fmode);
	    }

            if (dts_xferFile (
		((strcmp (func, "xferPushFile") == 0) ? srcHost : dstHost),
		func, method, rate, newfile, 
		fsize, fmode, xs) == ERR) {
		    if (list) free (list);
		    return (ERR);
            }

	    if (dstLocal)			/* restore permissions */
		(void) chmod (lpath, fmode);
	}
    }

    if (list) free (list);
    return (OK);
}


/**
 *  DTS_XFERFILE -- Transfer a single file between hosts.
 *
 *  @brief  Transfer a single file between hosts.
 *  @fn     stat = dts_xferFile (char *host, char *func, int method, 
 *			int rate, char *path,
 *			int fsize, mode_t fmode, xferStat *xs)
 *
 *  @param  host	client host
 *  @param  func	RPC method to call
 *  @param  method	transport method
 *  @param  rate	transfer rate (Mbps)
 *  @param  path	path to file
 *  @param  fsize	file size
 *  @param  fmode	file protection mode
 *  @param  xfs		transfer stats
 *  @return		1 (one) if DTS succeeds.
 */
int
dts_xferFile (char *host, char *func, int method, int rate, char *path, 
		int fsize, mode_t fmode, xferStat *xs)
{
    int  sec, usec, res = OK;
    int  client = dts_getClient (host);
    char dtscp_local = 0;


    // FIXME -- should not hardwire the port number
    dtscp_local = (strstr(d_url,":2997") != NULL);
    if (! fsize) {
        if (dtscp_local) {
            if (! dts_isDir (d_dir))
                dts_localMkdir (d_dir);
            dts_localTouch (d_path);
        } else {        

            if (! dts_hostIsDir (dest_host,d_dir)) {
                char holder[SZ_PATH];
                /*  FIXME -- MKDIR WONT WORK UNLESS IT HAS A ./
		 */
                memset (holder, 0, PATH_MAX);    
                strcpy (holder,d_dir);
                memmove (&holder[1], &holder[0], strlen(holder));
                holder[0] = '.';
                dts_hostMkdir (dest_host,holder);
            }

            res = dts_hostTouch (dest_host,d_path);
        }
        return (OK); //Re add in touch.
    }

    if (XFER_DEBUG) {
	fprintf (stderr, "------------ dts_xferFile ------------------\n");
	fprintf (stderr, "      src = '%s'\n", src_host);
	fprintf (stderr, "     dest = '%s'\n", dest_host);
	fprintf (stderr, "     path = '%s'   size = %d\n", path, fsize);
	fprintf (stderr, "     func = %s\n", func);
	fprintf (stderr, "     rate = %d\n", rate);
	fprintf (stderr, "   client = %d\n", client);
	fprintf (stderr, "    fmode = %d\n", fmode);
	fprintf (stderr, " nthreads = %d\n", nthreads);
	fprintf (stderr, "xfer_port = %d\n", xfer_port);
	fprintf (stderr, "    s_url = %s\n", s_url);
	fprintf (stderr, "    d_url = %s\n", d_url);
	fprintf (stderr, "    s_dir = %s\n", s_dir);
	fprintf (stderr, "    d_dir = %s\n", d_dir);
	fprintf (stderr, "   s_path = %s\n", s_path);
	fprintf (stderr, "   d_path = %s\n", d_path);
	fprintf (stderr, "  s_fname = %s\n", s_fname);
	fprintf (stderr, "  d_fname = %s\n", d_fname);
	fprintf (stderr, "--------------------------------------------\n");
    }

    /* Set the calling params.
    */
    xr_initParam (client);
    xr_setStringInParam (client, "dtsCmd");
    xr_setStringInParam (client, dts_cfgQMethodStr (method));
    xr_setStringInParam (client, path);
    xr_setIntInParam    (client, (int) fsize);
    xr_setIntInParam    (client, nthreads);
    xr_setIntInParam    (client, rate);
    xr_setIntInParam    (client, xfer_port);
    xr_setStringInParam (client, src_host);
    xr_setStringInParam (client, dest_host);
    xr_setStringInParam (client, s_url);
    xr_setStringInParam (client, d_url);
    xr_setStringInParam (client, s_dir);
    xr_setStringInParam (client, d_dir);
    xr_setStringInParam (client, s_fname);
    xr_setStringInParam (client, d_fname);

    if (xr_callSync (client, func) == OK) {

        char  str_res[SZ_CONFIG+1], *sres = str_res;

        xr_getStringFromResult (client, &sres);
	sscanf (sres, "%d %d %d", &sec, &usec, &res);

	xs->fsize   = fsize;
	xs->tput_mb = transferMb (fsize,sec,usec);
	xs->tput_MB = transferMB(fsize,sec,usec);
	xs->time    = ((double) sec + (double) usec / 1000000.0);
	xs->sec     = sec;
	xs->usec    = usec;
	xs->stat    = res; 

        if (XFER_DEBUG) {
	    fprintf (stderr, "dts_xferFile:  res = %d\n", res);
	    fprintf (stderr, 
		"File: '%s' (%d bytes %d.%d sec)\n    src = '%s'  dest = '%s'",
        	path, (int) fsize, sec, usec, src_host, dest_host);
	    fprintf (stderr, "    %g Mb/s   %g MB/s   %d.%d sec\n",
        	transferMb(fsize,sec,usec), transferMB(fsize,sec,usec), 
		sec, usec);
	}
	dts_closeClient (client);


#ifdef DTS_CHMOD
        /*  Set the remote file mode
         */
        fmode = (mode_t) dts_nameMode (path);
	for (i=3; i > 0; i--) {
            if (dts_hostChmod (dest_host, d_path, fmode) == ERR) {
		if (i == 1) {
    	            fprintf (stderr, "Error: Chmod of '%s' fails 3 retries\n", 
			d_path);
	            return (ERR);
		} else 
		    dtsSleep (SOCK_PAUSE_TIME);
            } else 
		break;
	}
#endif

        return (res);
    }
    dts_closeClient (client);

    if (XFER_DEBUG) 
	fprintf (stderr, "Remote method '%s' fails\n", func);

    dtsErrLog (NULL, "Error: Remote method '%s' fails\n", func);
    return (ERR);
}


/**
 *  DTS_XFERPARSEARGS -- Parse the arguments used in the push/pull and
 *  give/take commands.
 *
 *  @brief	Parse arguments used by the transfer commands.
 *  @fn 	stat = dts_xferParseArgs (int argc, char *argv[], 
 *		   	int *nthreads, int *port, int *verbose, 
 *		   	char **path_A, char **path_B)
 *
 *  @param  argc	arg count
 *  @param  argv	arg vector
 *  @param  nthreads	number of processing threads
 *  @param  verbose	verbose flag
 *  @param  port	base transfer port
 *  @param  path_A	src path specification
 *  @param  path_B	dest path specification
 *  @returns		status code
 */
int
dts_xferParseArgs (int argc, char *argv[], int *nthreads, int *port, 
	int *verbose, char **path_A, char **path_B)
{
    register int  i, j, len;


    /* Process the remaining argument vector.
    */
    for (i=0; i < argc; i++) {
        if (argv[i][0] == '-' && !(isdigit(argv[i][1]))) {
            len = strlen (argv[i]);
            for (j=1; j < len; j++) {
                switch (argv[i][j]) {
                case 'n':
		    *nthreads = atoi (argv[++i]);
		    if (*nthreads > MAX_THREADS) {
			fprintf (stderr, 
			    "Warning, too many threads: %d\n", *nthreads);
			*nthreads = MAX_THREADS;
		    }
		    break;
                case 'p': *port = atoi (argv[++i]); break;
                case 'v': *verbose = 1;		    break;
		default:
		    fprintf (stderr, "Unknown flag '%c'\n", argv[i][j]);
		    return (ERR);
		}
	    }

	} else {
	    if (*path_A) 
		*path_B = argv[i];
	    else
		*path_A = argv[i];
	}
    }

    return (OK);
}


/**
 *  DTS_XFERPARSEPATHS -- Parse the paths used in the transfer.
 *
 *  @brief	Parse the paths used by the transfer commands.
 *  @fn 	stat = dts_xferParsePaths (char *host, char **path_A, 
 *			char **path_B, int *srcLocal, int *dstLocal, 
 *			char **src_host, char **dest_host, 
 *			char **msg_host, char **path)
 *
 *  @param  host	host name
 *  @param  path_A	src path specification
 *  @param  path_B	dest path specification
 *  @param  srcLocal	is src local machine?
 *  @param  dstLocal	is dest local machine?
 *  @param  srcHost	source host name
 *  @param  dstHost	dest host name
 *  @param  msgHost	message host name
 *  @param  path	path name
 *  @returns		status code
 */
int
dts_xferParsePaths (char *host, char **path_A, char **path_B, 
	int *srcLocal, int *dstLocal, char **src_host, char **dest_host, 
	char **msg_host, char **path)
{
    char  *ip;
    int   port = 0;


    if (XFER_DEBUG)
	fprintf (stderr, "pA='%s'  pB='%s'\n",
	    (*path_A ? *path_A : " "), (*path_B ? *path_B : " "));


    /*  If we have both hosts specified, use the first as the source machine
     *  and override the host passed in.
     *
     *  e.g.   dtsh push foo:test.fits bar:test.fits
     */
    if (*path_A && *path_B) {
	char *ip = *path_A, *op = *msg_host;

	memset (*msg_host, 0, SZ_PATH);
	while (*ip && *ip != ':') 
	    *op++ = *ip++;

	*srcLocal = 0;
    }

    if (*path_A && ! *path_B) {
	if ((ip = strchr (*path_A, (int)':'))) {
	    /*  Argument has a remote file location, push it to the 'host'
	     *
	     *         dtsh -t foo push bar:test.fits bar:
	     */
	    sprintf (*dest_host, "%s", dts_resolveHost (host) );

	    /* src_host is resolved below */

	} else {
	    /*  Push a local file to the named host.
	     */
	    *srcLocal = 1;
	}
    }


    /* See if we're include host names in the path spec.
    */
    if (*path_B) {
	if ((ip = strchr (*path_B, (int)':'))) {
	    *path = ip + 1;
	    if (isdigit (**path)) {
		char host[SZ_FNAME];

		port = atoi (ip+1);
		if ((ip = strchr (*path, (int)':'))) {
	    	    *path = ip + 1;
	            *ip = '\0';
		}

		memset (host, 0, SZ_FNAME);
		strcpy (host, *path_B);
		if ((ip = strchr (host, (int)':')))
		    *ip = '\0';
	        sprintf (*dest_host, "%s:%d", dts_resolveHost (host), port);

	    } else {
	        *ip = '\0';
	         sprintf (*dest_host, "%s", dts_resolveHost (*path_B));
	    }
	    *path_B = *path;
	    /*port = DTS_PORT;*/
	}
   	*dstLocal = (strcmp (dts_getLocalIP(), *dest_host) == 0);
    } else
	*dstLocal = 1;

    if (*path_A) {
	if ((ip = strchr (*path_A, (int)':'))) {
	    *path = ip + 1;
	    if (isdigit (**path)) {
		char host[SZ_FNAME];

		port = atoi (ip+1);
		if ((ip = strchr (*path, (int)':'))) {
	    	    *path = ip + 1;
		    *ip = '\0';
		}

		memset (host, 0, SZ_FNAME);
		strcpy (host, *path_A);
		if ((ip = strchr (host, (int)':')))
		    *ip = '\0';
	        sprintf (*src_host, "%s:%d", dts_resolveHost (host), port);

	    }
	    *ip = '\0';
	     sprintf (*src_host, "%s", dts_resolveHost (*path_A));
	    *path_A = *path;
	    /*port = DTS_PORT;*/
	}
   	*srcLocal = (strcmp (dts_getLocalIP(), *src_host) == 0);
    }


    if (XFER_DEBUG) {
	fprintf (stderr, "xferParsePaths:  src_host='%s'  dest_host='%s'\n",
	    (*src_host ? *src_host : " "), (*dest_host ? *dest_host : " "));
    }

    return (OK);
}
