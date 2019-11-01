/**
 *  DTSTAR.H -- Include file for the DTS tar operations.
 *
 *  @brief	Include file for the DTS tar operations.
 *
 *  @file       dtsTar.h
 *  @author     Mike FItzpatrick
 *  @date       11/10/09
 *
 *****************************************************************************/


#define TBLOCK		8192
#define NBLOCK		20
#define NAMSIZ		100

#define	MAX_ERR		20
#define	MAX_TRYS	100
#define	MAX_LINELEN	256

#define	SZ_PADBUF	8196
#define	SZ_TAPEBUFFER	(TBLOCK * NBLOCK)

#define	EOS		'\0'
#define	RWXR_XR_X	0755
#define ctrlcode(c)	((c) >= '\007' && (c) <= '\017')

#define	LF_LINK		1
#define	LF_SYMLINK	2
#define	LF_DIR		5

#define	TY_DIRECTORY	1		/* object types			*/
#define	TY_FILE		2
#define	TY_TEXT		3
#define	TY_BINARY	4


/**
 *  File header structure.  One of these precedes each file on the tape.
 *  Each file occupies an integral number of TBLOCK size logical blocks
 *  on the tape.  The number of logical blocks per physical block is variable,
 *  with at most NBLOCK logical blocks per physical tape block.  Two zero
 *  blocks mark the end of the tar file.
 */
union hblock {
    char dummy[TBLOCK];
    struct header {
	char name[NAMSIZ];		/* NULL delimited		*/
	char mode[8];			/* octal, ascii			*/
	char uid[8];
	char gid[8];
	char size[12];
	char mtime[12];
	char chksum[8];
	char linkflag;
	char linkname[NAMSIZ];
    } dbuf;
};


/* Decoded file header.
 */
struct fheader {
    char name[NAMSIZ];
    int	 mode;
    int	 uid, gid;
    int	 isdir;
    long size, mtime, chksum;
    int	 linkflag;
    char linkname[NAMSIZ];
};



/* Map TAR file mode bits into characters for printed output.
 */
struct _modebits {
        int     code;
        char    ch;
} modebits[] = {
        { 040000, 'd'},
        { 0400,   'r'},
        { 0200,   'w'},
        { 0100,   'x'},
        { 040,    'r'},
        { 020,    'w'},
        { 010,    'x'},
        { 04,     'r'},
        { 02,     'w'},
        { 01,     'x'},
        { 0,        0}
};
