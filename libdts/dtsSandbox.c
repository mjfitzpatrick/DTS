/**
 *  DTSSANDBOX.C --  DTS file system sandbox routines.
 *
 *  @brief      DTS file system sandbox routines.
 *
 *  @file       dtsSandbox.c
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

#include "dts.h"


extern  DTS  *dts;
extern  int   dts_monitor;


#define	DEBUG		0

static char *dts_pathFixup (char *root, char *in);



/**
 *  DTS_SANDBOXPATH -- Return the sandboxed path name.
 *
 *  @brief  Return the sandboxed path name.
 *  @fn     path = dts_sandboxPath (char *path)
 *
 *  @param  path	input path name (not chrooted)
 *  @return		sandboxed path (caller must free ptr)
 */
char *
dts_sandboxPath (char *in)
{
    char  root[SZ_FNAME];
    char  *res = in, *ip, *op;


    /*  Check for an escape.
    */
    if (strncmp (in, DEF_PASSWD, 6) == 0)
	return (strdup (&in[6]));

    if (strcmp (in, "./") == 0)
	return (strdup ("./"));


    /* 	Get the root path for the server.
    */
    memset (root, 0, SZ_FNAME);
    strcpy (root, dts_getLocalRoot ());

    /* No input path defaults to the root.
    */
    if (!in || in[0] == '\0')
        return ( strdup (root) );

    if ( strstr (in, "..") ) {
	char *np = dts_pathFixup (root, in);

        return ( strdup (np) );

    } else {
	int rlen = strlen (root);

	/*  Fixup the paths so we won't have double '/' chars. 
	*/
	if ((char) root[rlen-1] == '/') {
	    if (in[0] == '/')
		root[rlen-1] = '\0';
	} else {
	    if (in[0] != '/')
		strcat (root, "/");
	}

	for (ip=root, op=in; *ip && *op && *ip == *op; ) 
	    ip++, op++;

	if (*ip == '\0') {
	    /* Input path contains the root.
	     */
	    memset (root, 0, SZ_FNAME);
	    res = strcpy (root, in);
	} else
	    /* Input path is relative to the root, just concatenate.
	     */
	    res = strcat (root, in);
    }


    /*  For symlinks in the sandbox, follow the path to the actual file.
     */
    if (access (root, F_OK) == 0) {
        if (dts_isLink (root) && !dts_isDir (root)) 
	    return (dts_readLink (root));
    }

    return ( strdup (root) );
}


/**
 *  DTS_PATHFIXUP -- Fix an incoming path with ".." elements so we can't
 *  use those to escape the root directory.
 */

#define  MAX_DEPTH	64

static char *
dts_pathFixup (char *root, char *in)
{
    register int   i;
    char  *ip, *op;
    char  dirs[MAX_DEPTH][SZ_PATH];
    char  path[SZ_PATH];


    /* Break the input path into individual directory elements.  We'll
    ** reconstruct the new directory afterwards.
    */
    memset (dirs, 0, (MAX_DEPTH * SZ_PATH));
    for (i=-1, ip=in; *ip; ) {
	if (*ip == '/')
	    ip++;
	if (*ip && strncmp (ip, "..", 2) == 0) {
	    if (i >= 0) memset (dirs[i], 0, SZ_PATH);
	    i = ( ((i-1) < 0) ? -1 : (i-1) );
	    ip += 2;
	    continue;
	}

	for (op=dirs[++i]; *ip && *ip != '/'; ip++)
	    *op++ = *ip;
	*op = '\0';
	
	if (*ip && *ip == '/')
	    ip++;
	if (*ip && strncmp (ip, "..", 2) == 0) {
	    if (i >= 0) memset (dirs[i], 0, SZ_PATH);
	    i = ( ((i-1) < 0) ? -1 : (i-1) );
	    ip += 2;
	}
    }

    memset (path, 0, SZ_PATH);
    strcpy (path, "/");
    for (i=0; i < MAX_DEPTH; i++) {
	if (dirs[i][0] == '\0')
	    break;
        if (i > 0) 
	    strcat (path, "/");
        strcat (path, dirs[i]);
    }

    return (dts_strbuf (path));
}

