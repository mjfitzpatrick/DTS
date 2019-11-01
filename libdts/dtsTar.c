/**
 *  DTSTAR.C -- Unpack or list a UNIX tar format file containing .....
 *
 *  @brief	DTS interface for reading tar files (e.g. bundles)
 *
 *  @file       dtsTar.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       11/10/09
 *
 *  Switches:
 *		b	generate only C style binary byte stream output
 *			  files (default is to write a text file when
 *			  the input stream is text).
 *		d	print debug messages
 *		e	exclude, rather than include, listed files
 *		f	read from named file rather than stdin
 *		l	do not try to resolve links by a file copy
 *		m	do not restore file modify times
 *		n	do not strip tailing blank lines from text files
 *		o	omit binary files (e.g. when foreign host has
 *			  incompatible binary file format)
 *		p	omit the given pathname prefix when creating files
 *		r	replace existing file at extraction
 *		t	print name of each file matched
 *		u	do not attempt to restore user id
 *		v	verbose; print full description of each file
 *		x	extract files (extract everything if no files
 *			  listed or if -e is set)
 *
 * Switches must be given in a group, in any order, e.g.:
 *
 *	rtar -xetvf tarfile sys/osb sys/os lib/config.h$
 *
 * would extract all files from tarfile with names not beginning with sys/os
 * or sys/osb or with names not equal to lib/config.h, printing a verbose
 * description of each file extracted.  If an exclude filename does not end
 * with a $ all files with the given string as a prefix are excluded.
 *
 *****************************************************************************/

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <fcntl.h>
#include <strings.h>
#include <string.h>
#include <ctype.h>

#include "dts.h"
#include "dtsTar.h"


static int stripblanks  = 1;	/* strip blank padding at end of file	*/
static int binaryout	= 0;	/* make only binary byte stream files	*/
static int extract	= 0;	/* Extract files from the tape		*/
static int replace	= 0;	/* Replace existing files		*/
static int exclude	= 0;	/* Excluded named files			*/
static int printfnames  = 0;	/* Print file names			*/
static int links	= 0;	/* Defeat copy to resolve link		*/
static int opt_setmtime	= 1;	/* Restore file modify times		*/
static int opt_setuid	= 1;	/* Restore file user id			*/
static int nblocks	= 0;
static int len_prefix 	= 0;
/*static int in		= EOF;*/ 
static int out 		= EOF;

static int eof		= 0;
static int nerrs	= 0;
static int debug 	= 0;	/* Print debugging messages		*/
static int verbose      = 0;	/* Print everything			*/

static char *pathprefix = NULL;
static char  tapeblock[SZ_TAPEBUFFER];


static struct fheader *curfil;

static char  *nextblock 	= NULL;
static char	tapeblock[SZ_TAPEBUFFER];



static void  dts_PutFiles (char *dir, int out, char *path);
static void  dts_TarfileOut (char *fname, int out, int ftype, char *path);
static int   dts_PutHeader (struct fheader *fh, int out);

static int   dts_GetHeader (int in, struct fheader *fh);
static void  dts_StripBlanks (int in, int out, long nbytes);
static void  dts_SkipFile (int in, struct fheader *fh);
static int   dts_MatchFile (char *fname, char **files);
static int   dts_Chksum (char *p, int nbytes);
static void  dts_PrintHeader (FILE *out, struct fheader *fh, int verbose);
static int   dts_FileType (int in, struct fheader *fh);
static void  dts_CopyFromTar (int in, int out, struct fheader *fh, int ftype);
static void  dts_CopyToTar (char *fname, struct fheader *fh, int ftype,int out);
static int   dts_CheckDir (char *path, int mode, int uid, int gid);
static int   dts_NewFile (char *fname, int mode, int uid, int gid, int type);
static char *dts_GetBlock ();
static int   dts_PutBlock (int out, char *buf);
static void  dts_EndTar (int out);
static char *dts_DName (char  *dir);




/* RTAR -- "rtar [xtvlef] [names]".  The default operation is to extract all
 * files from the tar format standard input in quiet mode.
 */
int dts_rtar (
    int argc, 
    char **argv
)
{
	struct	fheader fh;
	char	**argp;
	char	*ip;
	int	in = 0, out;
	int	ftype;
	int	ch;


	binaryout	= 0;
	links		= 0;
	extract		= 0;
	replace		= 0;
	exclude		= 0;
	printfnames	= 0;


	/* Get parameters.  Argp is left pointing at the list of files to be
	 * extracted (default all if no files named).
	 */
	argp = &argv[0];
	if (argc <= 1)
	    extract++;
	else {
	    while (*argp && **argp == '-') {
		ip = *argp++ + 1;
		while ((ch = *ip++) != EOS) {
		    switch (ch) {
		    case 'n': stripblanks = 0; 		break;
		    case 'x': extract++; 		break;
		    case 'b': binaryout++; 		break;
		    case 'e': exclude++; 		break;
		    case 'r': replace++; 		break;
		    case 't': printfnames++; 		break;
		    case 'l': links++; 			break;
		    case 'm': opt_setmtime = 0;		break;
		    case 'u': opt_setuid = 0; 		break;

		    case 'p':
			if (*argp != NULL) {
			    pathprefix = *argp++;
			    len_prefix = strlen (pathprefix);
			}
			break;

		    case 'f':
			if (*argp == NULL) {
			    fprintf (stderr, "missing filename argument\n");
			    exit (OK+1);
			}
			in = open (*argp, 0);
			if (in == ERR) {
			    fprintf (stderr, "cannot open `%s'\n", *argp);
			    exit (OK+1);
			}
			argp++;
			break;

		    case 'd': debug++; 			break;
		    case 'v': printfnames++, verbose++; break;

		    default:
			fprintf (stderr, "Warning: unknown switch `%c'\n", ch);
			fflush (stderr);
			break;
		    }
		}
	    }
	}


	/* Step along through the tar format file.  Read file header and if
	 * file is in list and extraction is enabled, extract file.
	 */
	while (dts_GetHeader (in, &fh) != EOF) {
	    curfil = &fh;

	    if (dts_MatchFile (fh.name, argp) == exclude) {
		if (debug)
		    fprintf (stderr, "skip file `%s'\n", fh.name);
		dts_SkipFile (in, &fh);
		continue;
	    }

	    if (printfnames) {
		dts_PrintHeader (stdout, &fh, verbose);
		fflush (stdout);
	    }

	    if (fh.linkflag == LF_SYMLINK || fh.linkflag == LF_LINK) {
		/* No file follows header if file is a link.  Try to resolve
		 * the link by copying the original file, assuming it has been
		 * read from the tape.
		 */
		if (extract) {
		    if (fh.linkflag == LF_SYMLINK) {
			if (replace)
			    unlink (fh.name);
			if (symlink (fh.linkname, fh.name) != 0) {
			    fprintf (stderr,
				"Cannot make symbolic link %s -> %s\n",
				fh.name, fh.linkname);
			}
		    } else if (fh.linkflag == LF_LINK && !links) {
			if (replace)
			    unlink (fh.name);
			if (dts_localCopy (fh.linkname, fh.name) == ERR) {
			    fprintf (stderr, "Copy `%s' to `%s' fails\n",
				fh.linkname, fh.name);
			} else {
			    chmod (fh.name, fh.mode);
			    if (opt_setuid)
				chown (fh.name, fh.uid, fh.gid);
			    if (opt_setmtime)
				dts_localMtime (fh.name, fh.mtime);
			}
		    } else {
			fprintf (stderr,
			    "Warning: cannot make link `%s' to `%s'\n",
			    fh.name, fh.linkname);
		    }
		}
		continue;
	    }

	    if (extract) {
		ftype = dts_FileType (in, &fh);

		out = dts_NewFile (fh.name, fh.mode, fh.uid, fh.gid, ftype);
		if (out == ERR) {
		    fprintf (stderr, "cannot create file `%s'\n", fh.name);
		    dts_SkipFile (in, &fh);
		    continue;
		}
		if (!fh.isdir) {
		    dts_CopyFromTar (in, out, &fh, ftype);
		    close (out);
		}
		chmod (fh.name, fh.mode);
		if (opt_setuid)
		    chown (fh.name, fh.uid, fh.gid);
		if (opt_setmtime)
		    dts_localMtime (fh.name, fh.mtime);
	    } else
		dts_SkipFile (in, &fh);
	}

	/* End of TAR file normally occurs when a zero tape block is read;
	 * this is not the same as the physical end of file, leading to
	 * problems when reading from sequential devices (e.g. pipes and
	 * magtape).  Advance to the physical end of file before exiting.
	 */
	if (!eof)
	    while (read (in, tapeblock, SZ_TAPEBUFFER) > 0)
		;
	if (in)
	    close (in);

	exit (OK);
}


/**
 *  DTSWWTAR -- Write a UNIX tar format file (on disk or to stdout)
 *
 *  @brief      DTS interface for writing tar files (e.g. bundles)
 *
 *  Switches:
 *		f	write to named file, otherwise write to stdout
 *		t	print name of each file written
 *		v	verbose; print full description of each file
 *		d	print debug messages
 *		o	omit binary files (e.g. when foreign host has
 *			  incompatible binary file format)
 *
 *****************************************************************************/



/* WTAR -- "wtar [-tvdo] [-f tarfile] [files]".  If no files are listed the
 * current directory tree is used as input.  If no output file is specified
 * output is to the standard output.
 */
int dts_wtar (
  int   argc,
  char *argv[]
)
{
    static char	*def_flist[2] = { ".", NULL };
    char  *argp, **flist;
    int	   argno, ftype, i;


    flist       = def_flist;
    printfnames = debug;
    verbose     = debug;

    if (debug) {
	printf ("wtar called with %d arguments:", argc);
	for (argno=0;  (argp = argv[argno]) != NULL;  argno++)
	    printf (" %s", argp);
	printf ("\n");
    }

    /* Process the argument list.
     */
    for (argno=0;  (argp = argv[argno]) != NULL;  argno++) {
	if (*argp != '-') {
	    flist = &argv[argno];
	    break;

	} else {
	    for (argp++;  *argp;  argp++) {
		switch (*argp) {
		case 'd':
		    debug++;
		    printfnames++;
		    verbose++;
		    break;
		case 't':
		    printfnames++;
		    break;
		case 'v':
		    printfnames++;
		    verbose++;
		    break;

		case 'f':
		    if (argv[argno+1]) {
			argno++;
			if (debug)
			    printf ("open output file `%s'\n", argv[argno]);
			out = open (argv[argno],O_WRONLY|O_CREAT,DTS_FILE_MODE);
			if (out < 0) {
			    fflush (stdout);
			    fprintf (stderr, "cannot open `%s'\n", argv[argno]);
			    exit (OK+1);
			}
		    }
		    break;

		default:
		    fflush (stdout);
		    fprintf (stderr, "Warning: unknown switch -%c\n", *argp);
		    fflush (stderr);
		}
	    }
	}
    }

    /* Write to the standard output if no output file specified.
     * The filename "stdin" is reserved.
     */
    if (out == ERR) {
	if (debug)
	    printf ("output defaults to stdout\n");
	out = open ("stdout", 1);
    }

    nextblock = tapeblock;
    nblocks = 0;

    /* Put each directory and file listed on the command line to 
     * the tarfile.
     */
    for (i=0;  (argp = flist[i]) != NULL;  i++) {
	ftype = (dts_isDir (argp) ? TY_DIRECTORY : TY_FILE);
	if (ftype == TY_DIRECTORY)
	    dts_PutFiles (argp, out, "");
	else
	    dts_TarfileOut (argp, out, ftype, "");
    }

    /* Close the tarfile.
     */
    dts_EndTar (out);
    close (out);

    exit (OK);
}


/*****************************************************************************
 *  Private Interface Routines
 ****************************************************************************/

/**
 *  DTS_PUTFILES -- Put the named directory tree to the output tarfile.  We
 *  chdir to each subdirectory to minimize path searches and speed up 
 *  execution.
 */
static void
dts_PutFiles (
    char *dir,				/* directory name		*/
    int	  out,				/* output file			*/
    char *path 				/* pathname of curr. directory	*/
)
{
    struct dirent *entry;
    char newpath[SZ_PATH+1], oldpath[SZ_PATH+1];
    int	 ftype;
    DIR  *dp;


    if (debug)
	printf ("dts_PutFiles (%s, %d, %s)\n", dir, out, path);

    /* Put the directory file itself to the output as a file.
     */
    dts_TarfileOut (dir, out, TY_DIRECTORY, path);

    if ((dp = opendir (dir)) == (DIR *) NULL) {
	fflush (stdout);
	fprintf (stderr, "cannot open subdirectory `%s%s'\n", path, dir);
	fflush (stderr);
	return;
    }

    strcpy (oldpath, dts_localCwd());
    sprintf (newpath, "%s%s", dts_DName(path), dir);
    strcpy (newpath, dts_DName(newpath));

    if (debug)
	printf ("change directory to %s\n", newpath);
    if (chdir (dir) == ERR) {
	closedir (dp);
	fflush (stdout);
	fprintf (stderr, "cannot change directory to `%s'\n", newpath);
	fflush (stderr);
	return;
    }

    /* Put each file in the directory to the output file.  Recursively
     * read any directories encountered.
     */
    /*
    while (os_gfdir (dp, fname, SZ_PATH) > 0) {
	ftype = dts_isDir (fname);
	if (readlink (fname, 0, 0))
	    dts_TarfileOut (fname, out, LF_SYMLINK, newpath);
	else if (ftype == TY_DIRECTORY)
	    dts_PutFiles (fname, out, newpath);
	else
	    dts_TarfileOut (fname, out, ftype, newpath);
    }
    */
    while ((entry = readdir (dp))) {
        char *name = entry->d_name;

	/* Skip the '.' and '..' entries.
	*/
        if ((strcmp (name, "..") == 0) || (strcmp (name, ".") == 0))
            continue;

	ftype = dts_isDir (name);
	if (dts_isLink (name))
	    dts_TarfileOut (name, out, LF_SYMLINK, newpath);
	else if (ftype == TY_DIRECTORY)
	    dts_PutFiles (name, out, newpath);
	else
	    dts_TarfileOut (name, out, ftype, newpath);
    }


    if (debug)
	printf ("return from subdirectory %s\n", newpath);
    if (chdir (oldpath) == ERR) {
	fflush (stdout);
	fprintf (stderr, "cannot return from subdirectory `%s'\n", newpath);
	fflush (stderr);
    }

    closedir (dp);
}


/**
 *  DTS_TARFILEOUT -- Write the named file to the output in tar format.
 */
static void
dts_TarfileOut (
    char *fname,			/* file to be output	*/
    int	  out,				/* output stream	*/
    int	  ftype,			/* file type		*/
    char *path				/* current path		*/
)
{
    struct  stat fi;
    struct  fheader fh;


    if (debug)
        printf ("put file `%s', type %d\n", fname, ftype);

    /* Get info on file to make file header.
     */
    if (stat (fname, &fi) < 0) {
        fflush (stdout);
        fprintf (stderr, "Warning: can't get info on file `%s'\n", fname);
        fflush (stderr);
        return;
    }

    /* Format and output the file header.
     */
    memset (&fh, 0, sizeof(fh));
    strcpy (fh.name, path);
    strcat (fh.name, fname);
    strcpy (fh.linkname, "");
    fh.linkflag = 0;

    if (ftype == TY_DIRECTORY) {
	strcpy (fh.name, dts_DName(fh.name));
	fh.size  = 0;
	fh.isdir = 1;
	fh.linkflag = LF_DIR;
    } else {
	fh.size  = fi.st_size;
	fh.isdir = 0;
    }

    fh.uid   = fi.st_uid;
    fh.gid   = fi.st_gid;
    fh.mode  = fi.st_mode;
    fh.mtime = fi.st_mtime;

    if (S_ISLNK(fi.st_mode)) {
	struct  stat nfi;
	lstat (fname, &nfi);

	/* Set attributes of symbolic link, not file pointed to. */
	fh.uid   = nfi.st_uid;
	fh.gid   = nfi.st_gid;
	fh.mode  = nfi.st_mode;
	fh.mtime = nfi.st_mtime;
	fh.size  = 0;

	fh.linkflag = LF_SYMLINK;
	readlink (fname, fh.linkname, NAMSIZ);
    }

    if (dts_PutHeader (&fh, out) == EOF)  {
	fflush (stdout);
	fprintf (stderr, 
		"Warning: could not write file header for `%s'\n", fname);
	fflush (stderr);
	return;
    }

    /* Copy the file data.
     */
    if (fh.size > 0 && !fh.isdir && !fh.linkflag)
	dts_CopyToTar (fname, &fh, ftype, out);

    if (printfnames) {
	dts_PrintHeader (stdout, &fh, verbose);
	fflush (stdout);
    }
}


/**
 *  DTS_PUTHEADER -- Encode and write the file header to the output tarfile.
 */
static int
dts_PutHeader (
    struct fheader *fh,			/* (input) file header		*/
    int	   out				/* output file descriptor	*/
)
{
    register char	*ip;
    register int	n;
    union	hblock	hb;
    char	chksum[10];

    /* Clear the header block. */
    for (n=0;  n < TBLOCK;  n++)
        hb.dummy[n] = '\0';

    /* Encode the file header.
     */
    strcpy  (hb.dbuf.name,  fh->name);
    sprintf (hb.dbuf.mode,  "%6o ",   fh->mode);
    sprintf (hb.dbuf.uid,   "%6o ",   fh->uid);
    sprintf (hb.dbuf.gid,   "%6o ",   fh->gid);
    sprintf (hb.dbuf.size,  "%11lo ", fh->size);
    sprintf (hb.dbuf.mtime, "%11lo ", fh->mtime);

    switch (fh->linkflag) {
    case LF_SYMLINK:
	hb.dbuf.linkflag = '2';
	break;
    case LF_DIR:
	hb.dbuf.linkflag = '5';
	break;
    default:
	hb.dbuf.linkflag = '0';
	break;
    }
    strcpy (hb.dbuf.linkname, fh->linkname);

    /* Encode the checksum value for the file header and then
     * write the field.  Calculate the checksum with the checksum
     * field blanked out. Compute the actual checksum as the sum of 
     * all bytes in the header block.  A sum of zero indicates the 
     * end of the tar file.
     */
    for (n=0;  n < 8;  n++)
        hb.dbuf.chksum[n] = ' ';

    sprintf (chksum, "%6o", dts_Chksum (hb.dummy, TBLOCK));
    for (n=0, ip=chksum;  n < 8;  n++)
        hb.dbuf.chksum[n] = *ip++;

    if (debug) {
	printf ("File header:\n");
	printf ("      name = %s\n", hb.dbuf.name);
	printf ("      mode = %s\n", hb.dbuf.mode);
	printf ("       uid = %s\n", hb.dbuf.uid);
	printf ("       gid = %s\n", hb.dbuf.gid);
	printf ("      size = %-12.12s\n", hb.dbuf.size);
	printf ("     mtime = %-12.12s\n", hb.dbuf.mtime);
	printf ("    chksum = %s\n", hb.dbuf.chksum);
	printf ("  linkflag = %c\n", hb.dbuf.linkflag);
	printf ("  linkname = %s\n", hb.dbuf.linkname);
	fflush (stdout);
    }

    /* Write the header to the tarfile.
     */
    return (dts_PutBlock (out, hb.dummy));
}


/**
 *  DTS_MATCHFILE -- Search the filelist for the named file.  If the file
 *  list is empty anything is a match.  If the list element ends with a $ an
 *  exact match is required (excluding the $), otherwise we have a match if
 *  the list element is a prefix of the filename.
 */
static int
dts_MatchFile (
    char  *fname,		/* filename to be compared to list	*/
    char  **files 		/* pointer to array of fname pointers	*/
)
{
    register char *fn, *ln;
    register int firstchar;

    if (*files == NULL)
        return (1);

    firstchar = *fname;
    do {
	if (**files++ == firstchar) {
	    for (fn=fname, ln = *(files-1);  *ln && *ln == *fn++;  )
	        ln++;
	    if (*ln == EOS)
	        return (1);
	    else if (*ln == '$' && *(fn-1) == EOS)
	        return (1);
	}
    } while (*files);

    return (0);
}


/**
 *  DTS_GETHEADER -- Read the next file block and attempt to interpret it as
 *  a file header.  A checksum error on the file header is fatal and usually
 *  indicates that the tape is not positioned to the beginning of a file.
 *  If we have a legal header, decode the character valued fields into binary.
 */
static int
dts_GetHeader (
    int	in,			/* input file			*/
    struct fheader *fh		/* decoded file header (output)	*/
)
{
    register char *ip, *op;
    register int n;
    union	hblock *hb;
    int	checksum, ntrys;

    for (ntrys=0;  ;  ntrys++) {
	if ((hb = (union hblock *)dts_GetBlock  (in)) == NULL)
	    return (EOF);

	/* Decode the checksum value saved in the file header and then
	 * overwrite the field with blanks, as the field was blank when
	 * the checksum was originally computed.  Compute the actual
	 * checksum as the sum of all bytes in the header block.  If the
	 * sum is zero this indicates the end of the tar file, otherwise
	 * the checksums must match.
	 */
	if (*hb->dbuf.chksum == '\0' && 
	    dts_Chksum ((char *)hb, TBLOCK) == 0)
	        return (EOF);
	else
	    sscanf (hb->dbuf.chksum, "%o", &checksum);

	for (ip=hb->dbuf.chksum, n=8;  --n >= 0;  )
	    *ip++ = ' ';
	if (dts_Chksum ((char *)hb, TBLOCK) != checksum) {
	    /* If a checksum error occurs try to advance to the next
	     * header block.
	     */
	    if (ntrys == 0) {
		fprintf (stderr,
		    "rtar: file header checksum error %o != %o\n",
	    	    dts_Chksum ((char *)hb, TBLOCK), checksum);
	    } else if (ntrys >= MAX_TRYS) {
		fprintf (stderr, "cannot recover from checksum error\n");
		exit (OK+1);
	    }
	} else
	    break;
    }

    if (ntrys > 1)
        fprintf (stderr, "found next file following checksum error\n");

    /* Decode the ascii header fields into the output file header
     * structure.
     */
    for (ip=hb->dbuf.name, op=fh->name;  (*op++ = *ip++);  )
        ;
    fh->isdir = (*(op-2) == '/');

    sscanf (hb->dbuf.mode,     "%o",  &fh->mode);
    sscanf (hb->dbuf.uid,      "%o",  &fh->uid);
    sscanf (hb->dbuf.gid,      "%o",  &fh->gid);
    sscanf (hb->dbuf.size,     "%lo", &fh->size);
    sscanf (hb->dbuf.mtime,    "%lo", &fh->mtime);

    n = hb->dbuf.linkflag;
    if (n >= '0' && n <= '9')
        fh->linkflag = n - '0';
    else
        fh->linkflag = 0;

    if (fh->linkflag)
        strcpy (fh->linkname, hb->dbuf.linkname);
	
    return (TBLOCK);
}


/**
 *  DTS_CHKSUM -- Compute the checksum of a byte array.
 */
static int
dts_Chksum (
    char  *p,
    int	  nbytes
)
{
    register int	sum;

    for (sum=0;  --nbytes >= 0;  )
        sum += *p++;

    return (sum);
}


/**
 *  DTS_PRINTHDR -- Print the file header in either short or long
 *  format, e.g.:
 *
 *	drwxr-xr-x  9 fitz         1024 Nov  3 17:53 .
 */
static void
dts_PrintHeader (
    FILE  *out,				/* output file			*/
    struct fheader *fh,			/* file header struct		*/
    int	   verbose 			/* long format output		*/
)
{
    register struct _modebits *mp;
    char  *tp, *ctime();

    if (!verbose) {
        fprintf (out, "%s\n", fh->name);
        return;
    }

    for (mp=modebits;  mp->code;  mp++)
        fprintf (out, "%c", mp->code & fh->mode ? mp->ch : '-');

    tp = ctime (&fh->mtime);
    fprintf (out, "%3d %4d %2d %8ld %-12.12s %-4.4s %s",
        fh->linkflag,
        fh->uid,
        fh->gid,
        fh->size,
        tp + 4, tp + 20,
        fh->name);

    if (fh->linkflag && *fh->linkname)
        fprintf (out, " -> %s\n", fh->linkname);
    else
        fprintf (out, "\n");
}


/* DTS_FILETYPE -- Determine the file type (text, binary, or directory) of the
 * next file on the input stream.  Directory files are easy; the tar format
 * identifies directories unambiguously.  Discriminating between text and
 * binary files is not possible in general because UNIX does not make such
 * a distinction, but in practice we can apply a heuristic which will work
 * in nearly all cases.  This can be overriden, producing only binary byte
 * stream files as output, by a command line switch.
 */
static int
dts_FileType (
    int	   in,			/* input file			*/
    struct fheader *fh 		/* decoded file header		*/
)
{
    register char	*cp;
    register int	n, ch;
    int	newline_seen, nchars;

    /* Easy cases first.
     */
    if (fh->isdir)
        return (TY_DIRECTORY);
    else if (fh->size == 0 || binaryout)
        return (TY_BINARY);

    /* Get a pointer to the first block of the input file and set the
     * input pointers back so that the block is returned by the next
     * call to dts_GetBlock .
     */
    if ((cp = dts_GetBlock  (in)) == NULL)
        return (TY_BINARY);
    nextblock -= TBLOCK;
    nblocks++;

    /* Examine the data to see if it is text.  The simple heuristic
     * used requires that all characters be either printable ascii
     * or common control codes.
     */
    nchars = (fh->size < TBLOCK) ? fh->size : TBLOCK;
    n = nchars;
    for (newline_seen=0;  --n >= 0;  ) {
	ch = *cp++;
	if (ch == '\n')
	    newline_seen++;
	else if (!isprint(ch) && !isspace(ch) && !ctrlcode(ch))
	    break;
    }

    if (n >= 0 || (nchars > MAX_LINELEN && !newline_seen))
        return (TY_BINARY);
    else
        return (TY_TEXT);
}


/* DTS_NEWFILE -- Try to open a new file for writing, creating the new file
 * with the mode bits given.  Create all directories leading to the file if
 * necessary (and possible).
 */
static int
dts_NewFile (
    char  *fname,			/* pathname of file		*/
    int	  mode,				/* file mode bits		*/
    int	  uid, 	int gid,		/* file owner, group codes	*/
    int	  type 				/* text, binary, directory	*/
)
{
    int	fd;
    char	*cp;


    if (len_prefix && strncmp(fname,pathprefix,len_prefix) == 0)
        fname += len_prefix;

    if (debug)
        fprintf (stderr, "dts_NewFile `%s':\n", fname);

    if (dts_CheckDir (fname, mode, uid, gid) == ERR)
        return (ERR);

    if (type == TY_DIRECTORY) {
	cp = rindex (fname, (int)'/');
	if (cp && *(cp+1) == EOS)
	    *cp = EOS;
	fd = mkdir (fname, mode);

	/* Ignore any error creating directory, as this may just mean
	 * that the directory already exists.  If the directory does
	 * not exist and cannot be created, there will be plenty of
	 * other errors when we try to write files into it.
	 */
	fd = OK;

    } else {
	if (replace)
	    unlink (fname);
	fd = creat (fname, mode);
    }

    return (fd);
}


/**
 *  DTS_CHECKDIR -- Verify that all the directories in the pathname of a
 *  file exist.  If they do not exist, try to create them.
 */
static int
dts_CheckDir (
    char *path,
    int	  mode,
    int	  uid, 
    int   gid
)
{
    register char  *cp;


    /* Quick check to see if the directory exists.
     */
    if ((cp = rindex (path, (int)'/')) == NULL)
        return (OK);

    *cp = EOS;
    if (dts_isDir (path) > 0) {
        *cp = '/';
        return (OK);
    }
    *cp = '/';

    /* The directory cannot be accessed.  Try to make all directories
     * in the pathname.  If the file is itself a directory leave its
     * creation until later.
     */
    for (cp=path;  *cp;  cp++) {
        if (*cp != '/')
    	    continue;
        if (*(cp+1) == EOS)
    	    return (OK);

        *cp = EOS;
        if (dts_isDir (path) < 0) {
	    if (mkdir (path, RWXR_XR_X) == ERR) {
		fprintf (stderr, "cannot create directory `%s'\n", path);
		*cp = '/';
		return (ERR);
	    } else
		chown (path, uid, gid);
        }
        *cp = '/';
    }

    return (OK);
}


/**
 *  DTS_COPYFROMTAR -- Copy bytes from the input (tar) file to the output file.
 *  Each file consists of a integral number of TBLOCK size blocks on the
 *  input file.
 */
static void
dts_CopyFromTar (
    int	    in,				/* input file			*/
    int	    out,			/* output file			*/
    struct  fheader *fh,		/* file header structure	*/
    int	    ftype 			/* text or binary file		*/
)
{
    long	nbytes = fh->size;
    int	nblocks = 0, maxpad;
    char	*bp;

    /* Link files are zero length on the tape. */
    if (fh->linkflag)
        return;

    if (ftype == TY_BINARY || !stripblanks)
        maxpad = 0;
    else
        maxpad = SZ_PADBUF;

    /* Copy all but the last MAXPAD characters if the file is a text file
     * and stripping is enabled.
     */
    while (nbytes > maxpad && (bp = dts_GetBlock  (in)) != NULL) {
        if (write (out, bp, nbytes<TBLOCK ? (int)nbytes:TBLOCK) == ERR) {
    	    fprintf (stderr, "Warning: file write error on `%s'\n",
    	        curfil->name);
    	    if (nerrs++ > MAX_ERR) {
    	        fprintf (stderr, "Too many errors\n");
    	        exit (OK+1);
    	    }
        } else {
    	    nbytes -= TBLOCK;
    	    nblocks++;
        }
    }

    /* Strip whitespace at end of file added by WTAR when the archive was
     * created.
     */
    if (nbytes > 0)
        dts_StripBlanks (in, out, nbytes);

    if (debug)
        fprintf (stderr, "%d blocks written\n", nblocks);
}


/* COPYFILE -- Copy bytes from the input file to the output file.  Each file
 * consists of a integral number of TBLOCK size blocks on the output file.
 */
static void
dts_CopyToTar (
    char   *fname,                 /* file being read from         */
    struct fheader *fh,            /* file header structure        */
    int    ftype,                  /* file type, text or binary    */
    int    out                     /* output file                  */
)
{
    register char   *bp;
    register int    i;
    int     nbytes, nleft, blocks, fd, count, total, ch;
    char    buf[TBLOCK*2];

    bp     = buf;
    total  = 0;
    nbytes = 0;
    blocks = (fh->size + TBLOCK - 1 ) / TBLOCK;

    if ((fd = open (fname, 0)) == ERR) {
        fflush (stdout);
        fprintf (stderr, "Warning: cannot open file `%s'\n", fname);
        fflush (stderr);
        goto pad_;
    }

    while (blocks > 0) {
        if ((count = read (fd, bp, TBLOCK)) == ERR || count > TBLOCK) {
            fflush (stdout);
            fprintf (stderr, "Warning: file read error on `%s'\n", fname);
            fflush (stderr);
            if (nerrs++ > MAX_ERR) {
                fprintf (stderr, "Too many errors\n");
                exit (OK+1);
            }
        } else {
            /* Buffer input to TBLOCK blocks.
             */
            if (count == 0)         			/* EOF */
                break;
            else if ((nbytes += count) < TBLOCK)
                bp += count;
            else {
                dts_PutBlock (out, buf);
                blocks--;

                /* Copy overflow back to beginning... */
                if (nbytes > TBLOCK) {
                    nleft = nbytes - TBLOCK;
                    memcpy (buf, &buf[TBLOCK], nbytes - TBLOCK);
                } else
                    nleft = 0;

                bp = (char *) ((long)buf + nleft);
                total += nbytes;
                nbytes = nleft;
            }
        }
    }

    close (fd);

    /* Fill current block and subsequent full blocks until the number of
     * bytes specified in the file header have been output.  All files
     * occupy an integral number of 512 byte blocks on tape.  For text
     * files, pad with spaces, otherwise pad with nulls.  Also, for text
     * files, add newlines to avoid excessively long lines.
     */
pad_:
    ch = (ftype == TY_TEXT) ? ' ' : '\0';
    while (blocks > 0) {
        for (i=nbytes;  i < TBLOCK;  i++)
            if (ftype == TY_TEXT && i % 64 == 0)
                buf[i] = '\n';
            else
                buf[i] = ch;

        if (ftype == TY_TEXT)
            buf[TBLOCK-1] = '\n';

        dts_PutBlock (out, buf);
        blocks--;
        nbytes = 0;
    }
}


/* DTS_STRIPBLANKS -- Read the remaining file data into the pad buffer.
 * Write out the remaining data, minus any extra blanks or empty blank lines
 * at the end of the file.  Some versions of WTAR (e.g., VMS) do not know
 * the actual size of a text file and have to pad with blanks at the end to
 * make the file the size noted in the file header.
 */
static void
dts_StripBlanks (
    int	  in, 
    int   out,
    long  nbytes
)
{
    register char *ip, *op;
    char  padbuf[SZ_PADBUF+10], *lastnl;
    int	  n;


    /* Fill buffer.
     */
    op = padbuf;
    while (nbytes > 0 && (ip = dts_GetBlock (in)) != NULL) {
	 n = nbytes < TBLOCK ? (int)nbytes : TBLOCK;
	 memcpy (op, ip, n + 1);
	 nbytes -= n;
	 op += n;
    }

    /* Backspace from the end of the buffer until the last nonblank line
     * is found.
     */
    lastnl = op - 1;
    for (ip=lastnl;  ip > padbuf;  --ip) {
	if (*ip == '\n')
	    lastnl = ip;
	else if (*ip != ' ')
	    break;
    }

    /* Write out everything up to and including the newline at the end of
     * the last line containing anything but blanks.
     */
    write (out, padbuf, lastnl - padbuf + 1);
}


/**
 *  DTS_SKIPFILE -- Skip the indicated number of bytes on the input tar file.
 */
static void
dts_SkipFile (
    int	   in,				/* input file			*/
    struct fheader *fh 			/* file header			*/
)
{
    register long nbytes = fh->size;

    /* Link files are zero length on the tape. */
    if (! fh->linkflag) {
	while (nbytes > 0 && dts_GetBlock  (in) != NULL)
	    nbytes -= TBLOCK;
    }
}


/**
 *  DTS_GETBLOCK -- Return a pointer to the next file block of size TBLOCK
 *  bytes in the input file.
 */
static char *
dts_GetBlock  (
    int	in				/* input file			*/
)
{
    char	*bp;
    int	nbytes;

    for (;;) {
	if (eof)
	    return (NULL);
	else if (--nblocks >= 0) {
	    bp = nextblock;
	    nextblock += TBLOCK;
	    return (bp);
	}

	if ((nbytes = read (in, tapeblock, SZ_TAPEBUFFER)) < TBLOCK) {
	    eof++;
	} else {
	    nblocks = (nbytes + TBLOCK-1) / TBLOCK;
	    nextblock = tapeblock;
	}
    }
}



/* DTS_PUTBLOCK -- Write a block to tape (buffered).
 */
static int
dts_PutBlock (
    int   out,
    char *buf
)
{
    int nbytes = 0;

    if (buf) {
        memcpy (nextblock, buf, TBLOCK);
        nextblock += TBLOCK;
        if (++nblocks == NBLOCK)
            nbytes = SZ_TAPEBUFFER;
    } else if (nblocks > 0)
        nbytes = SZ_TAPEBUFFER;

    if (nbytes > 0) {
        if (write (out, tapeblock, nbytes) < nbytes) {
            fflush (stdout);
            fprintf (stderr, "Warning: write error on tarfile\n");
            fflush (stderr);
        }   

        nextblock = tapeblock;
        nblocks = 0;
    }   

    return (TBLOCK);
}


/* DTS_ENDTAR -- Write the end of the tar file, i.e., two zero blocks.
 */
static void
dts_EndTar (int out)
{
    register int  i;
    union    hblock hb;

    if (debug)
        printf ("write end of tar file\n");

    for (i=0;  i < TBLOCK;  i++)
        hb.dummy[i] = '\0';

    dts_PutBlock (out, hb.dummy);           /* write 2 null blocks */
    dts_PutBlock (out, hb.dummy);
    dts_PutBlock (out, 0);                  /* flush tape buffer */
}


/* DTS_DNAME -- Normalize a directory pathname.   For unix, this means convert
 * an // sequences into a single /, and make sure the directory pathname ends
 * in a single /.
 */
static char *
dts_DName (
    char  *dir
)
{
    register char       *ip, *op;
    static      char path[SZ_PATH+1];

    for (ip=dir, op=path;  *ip;  *op++ = *ip++)
        while (*ip == '/' && *(ip+1) == '/')
            ip++;

    if (op > path && *(op-1) != '/')
        *op++ = '/';
    *op = EOS;

    return (path);
}
