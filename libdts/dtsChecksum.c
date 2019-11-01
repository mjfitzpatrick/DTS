/**
 *  DTSCHECKSUM.C -- Checksum utility routines for the DTS.
 *
 *
 *           md5 = dts_fileMD5 (char *fname)
 *         crc = dts_fileCRC32 (char *fname)
 *      sum = dts_fileChecksum (char *fname, int do_sysv)
 *   sum = dts_fileCRCChecksum (char *fname, unsigned int *crc)

 *    valid = dts_fileValidate (char *fname, uint sum32, uint crc, char *md5)
 *
 * 	        checksum (uchar *data, int length, ushort *sum16, uint *sum32)
 *      sum = addcheck32 (uchar *array, int length)
 *
 *
 *  @brief	DTS Checksum utility methods
 *
 *  @file  	dtsUtil.c
 *  @author  	Mike Fitzpatrick, NOAO
 *  @date	6/15/09
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/fcntl.h>

#include "dts.h"


#ifdef  BUFSIZ
#undef  BUFSIZ
#define BUFSIZ          262144
#endif

extern DTS	*dts;


/**
 *  CRC-32 static table (originating polynomial = 0xEDB88320L)
 */
static unsigned int crc_tab[] = {
    0x0, 
    0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f, 0xe963a535,
    0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988, 0x09b64c2b,
    0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2, 0xf3b97148,
    0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7, 0x136c9856,
    0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9, 0xfa0f3d63,
    0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172, 0x3c03e4d1,
    0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6,
    0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59, 0x26d930ac,
    0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423, 0xcfba9599,
    0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924, 0x2f6f7c87,
    0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106, 0x98d220bc,
    0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433, 0x7807c9a2,
    0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d, 0x91646c97,
    0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e, 0x6c0695ed,
    0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea,
    0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65, 0x4db26158,
    0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7, 0xa4d1c46d,
    0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0, 0x44042d73,
    0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa, 0xbe0b1010,
    0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f, 0x5edef90e,
    0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81, 0xb7bd5c3b,
    0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a, 0xead54739,
    0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84, 0x0d6d6a3e,
    0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1, 0xf00f9344,
    0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb, 0x196c3671,
    0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc, 0xf9b9df6f,
    0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4,
    0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b, 0xd80d2bda,
    0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55, 0x316e8eef,
    0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236, 0xcc0c7795,
    0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92,
    0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d, 0x9b64c2b0,
    0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f, 0x72076785,
    0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38, 0x92d28e9b,
    0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8,
    0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777, 0x88085ae6,
    0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69, 0x616bffd3,
    0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2, 0xa7672661,
    0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc, 0x40df0b66,
    0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9, 0xbdbdf21c,
    0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693, 0x54de5729,
    0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94, 0xb40bbe37,
    0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d, 0x0,        0x0,        0x0
};



#ifdef DTS_CHECKSUM_TESTS
int main(int argc, char *argv[])
{
    unsigned int crc = 0, sum32 = 0;
    char *md5 = NULL;


    if (access (argv[1], R_OK)) {
	fprintf (stderr, "Cannot open file '%s'\n", argv[1]);
	return (1);
    }

    md5 = dts_fileMD5 (argv[1]);
    fprintf (stderr, " MD5 Checksum:  %s\n", (md5 ? md5 : "ERROR"));

    sum32 = dts_fileChecksum (argv[1], DTS_BSD_SUM32);
    fprintf (stderr, " BSD Checksum:  %d\n", sum32);

    sum32 = dts_fileChecksum (argv[1], DTS_SYSV_SUM32);
    fprintf (stderr, "SysV Checksum:  %d\n", sum32);

    crc = dts_fileCRC32 (argv[1]);
    fprintf (stderr, "       CRC-32:  %d\n", crc);

    sum32 = dts_fileCRCChecksum (argv[1], &crc);
    fprintf (stderr, "  CRCChecksum:  sum=%d   crc=%d\n", sum32, crc);

    if (md5) free ((void *) md5);

    return (0);
}
#endif




/*****************************************************************************/

/**
 *  DTS_FILEMD5 -- Calculate the MD5 hash of a file.
 *
 *  @fn md5 = dts_fileMD5 (char *fname)
 *
 *  @param  fname	file name
 *  @returns		MD5 hash
 */

#define	SZ_MD5BUF	36

static int md5_stream (FILE *stream, void *resblock);

char *
dts_fileMD5 (char *fname)
{
    register int  i, j;
    unsigned char res[SZ_MD5BUF];
    char         *md5 = NULL;
    FILE  *fd = (FILE *) NULL;


    if (access (fname, R_OK) == 0) {
        memset (res, 0, SZ_MD5BUF);

        if ((fd = fopen (fname, "r")) ) {
            if (md5_stream (fd, res) > 0) {
                fclose (fd);			/* error computing MD5 */
                return (NULL);
	    }
	}

	/*  Convert the binary result to a hex string.
  	 */
	md5 = calloc (1, SZ_MD5BUF);
        for (i=j=0; i< 16; i++, j+=2)
            sprintf (&md5[j], "%02x", (unsigned char) res[i]);

        fclose (fd);

	/*  Return the pointer to the string.  The caller is responsible 
	 *  for freeing the pointer.
	 */
        return ((char *) md5);
    }
    return (NULL);
}


/**
 *  DTS_memMD5 -- Calculate the MD5 hash of a memory buffer.
 *
 *  @fn md5 = dts_memMD5 (unsigned char *buf, size_t len)
 *
 *  @param  buf 	memory buffer to compute
 *  @param  len 	length of buffer (in bytes)
 *  @returns		MD5 hash
 */

char *
dts_memMD5 (unsigned char *buf, size_t len)
{
    register int  i, j;
    unsigned char res[SZ_MD5BUF];
    char         *md5 = NULL;


    if (buf) {
	extern void *md5_buffer (const char *buffer, size_t len, void *rblock);

        memset (res, 0, SZ_MD5BUF);

	/*  Use the in-memory version....
	 */
        (void) md5_buffer ((const char *)buf, (size_t) len, res);

	/*  Convert the binary result to a hex string.
  	 */
	md5 = calloc (1, SZ_MD5BUF);
        for (i=j=0; i< 16; i++, j+=2)
            sprintf (&md5[j], "%02x", (unsigned char) res[i]);

	/*  Return the pointer to the string.  The caller is responsible 
	 *  for freeing the pointer.
	 */
        return ((char *) md5);
    }

    return (NULL);
}


/**
 *  DTS_FILECRC32 -- Calculate the checksum of a file.
 *
 *  @fn crc = dts_fileCRC32 (char *fname)
 *
 *  @param  fname	file name
 *  @returns		crc32 checksum
 */

unsigned int
dts_fileCRC32 (char *fname)
{
    int   i, fd, nb = 0;
    unsigned long  crc = 0xFFFFFFFF;
    unsigned char *bp, buffer[BUFSIZ];


    /*  Open the file and compute the CRC32 sum.
     */
    if ((fd = open (fname, O_RDONLY)) > 0) {
	memset (buffer, 0, BUFSIZ);
        while ((nb = read(fd, (char *) buffer, BUFSIZ)) > 0) {
            for (i=0, bp=buffer; i < nb; i++, bp++)
                crc = ((crc >> 8) & 0x00FFFFFF) ^ crc_tab[(crc ^ (*bp)) & 0xFF];
	    memset (buffer, 0, BUFSIZ);
        }
        close(fd);		/* clean up	*/
    }

    return ((crc = crc ^ 0xFFFFFFFF));
}


/**
 *  DTS_MEMILECRC32 -- Calculate the checksum of a memory buffer.
 *
 *  @fn crc = dts_memCRC32 (unsigned char *buf, size_t len)
 *
 *  @param  buf		memory buffer to calculate
 *  @param  len		length of buffer (in bytes)
 *  @returns		crc32 checksum
 */

unsigned int
dts_memCRC32 (unsigned char *buf, size_t len)
{
    int   i;
    unsigned long  crc = 0xFFFFFFFF;
    unsigned char *bp;


    /*  Open the file and compute the CRC32 sum.
     */
    if (buf)
        for (i=0, bp=buf; i < len; i++, bp++)
            crc = ((crc >> 8) & 0x00FFFFFF) ^ crc_tab[(crc ^ (*bp)) & 0xFF];

    return ((crc = crc ^ 0xFFFFFFFF));
}


/**
 *  DTS_FILECHECKSUM -- Calculate the checksum of a file.
 *
 *  @fn sum = dts_fileChecksum (char *fname, int do_sysv)
 *
 *  @param  fname	file name
 *  @param  do_sysv	do SYSV checksum?
 *  @returns		sum of all bytes, modulo (UINT_MAX + 1)
 */

unsigned int
dts_fileChecksum (char *fname, int do_sysv)
{
    int  fd = 0;
    unsigned int  r = 0;
    unsigned int  s = 0;     /* sum of all input bytes, modulo (UINT_MAX + 1) */
    unsigned int  bytes_read = 0;
    unsigned char buf[BUFSIZ];


    if ((fd = open (fname, O_RDONLY)) < 0)
        return 0;

    while (1) {
	memset (buf, 0, BUFSIZ);
        bytes_read = read (fd, buf, BUFSIZ);

        if (bytes_read <= 0) {
            r = (fd && close(fd) != 0);
            if (!bytes_read && !r)
                break; 		/* no error */
            return (0);
        }

        if (do_sysv) {
            do s += buf[--bytes_read]; while (bytes_read);
        } else {
            r = 0;
            do {
                s = (s >> 1) + ((s & 1) << 15);
                s += buf[r++];
                s &= 0xffff; /* Keep it within bounds. */
            } while (--bytes_read);
        }
    }

    if (do_sysv) {
        r = (s & 0xffff) + ((s & 0xffffffff) >> 16);
        s = (r & 0xffff) + (r >> 16);
    }

    return (s);
}


/**
 *  DTS_MEMCHECKSUM -- Calculate the checksum of a memory buffer.
 *
 *  @fn sum = dts_memChecksum (unsigned char *buf, size_t len, int do_sysv)
 *
 *  @param  buf		memory buffer to calculate
 *  @param  len		length of buffer (in bytes)
 *  @param  do_sysv	do SYSV checksum?
 *  @returns		sum of all bytes, modulo (UINT_MAX + 1)
 */

unsigned int
dts_memChecksum (unsigned char *buf, size_t len, int do_sysv)
{
    unsigned int  s = 0, r = 0;
    unsigned int  nbytes = len;


    if (buf) {
        if (do_sysv) {
            do s += buf[--nbytes]; while (nbytes);
        } else {
            r = 0;
            do {
                s = (s >> 1) + ((s & 1) << 15);
                s += buf[r++];
                s &= 0xffff; /* Keep it within bounds. */
            } while (--nbytes);
        }
    }

    if (do_sysv) {
        r = (s & 0xffff) + ((s & 0xffffffff) >> 16);
        s = (r & 0xffff) + (r >> 16);
    }

    return (s);
}


/**
 *  DTS_FILECRCCHECKSUM -- Calculate the combined CRC and checksum of a file.
 *
 *  @fn sum = dts_fileCRCChecksum (char *fname, unsigned int *crc)
 *
 *  @param  fname	file name
 *  @param  crc		CRC-32 value
 *  @returns		sysv checksum of all bytes, modulo (UINT_MAX + 1)
 */

unsigned int
dts_fileCRCChecksum (char *fname, unsigned int *crc_out)
{
    int  fd = 0;
    unsigned int  i, r = 0, index = 0;
    unsigned long crc = 0xFFFFFFFF;
    unsigned int  s = 0;     /* sum of all input bytes, modulo (UINT_MAX + 1) */
    unsigned int  bytes_read = 0;
    unsigned char buf[BUFSIZ];


    if ((fd = open (fname, O_RDONLY)) < 0)
        return 0;

    while (1) {
	memset (buf, 0, BUFSIZ);
        bytes_read = read (fd, buf, BUFSIZ);

        if (bytes_read <= 0) {
            r = (fd && close(fd) != 0);
            if (!bytes_read && !r)
                break; 		/* no error */
            return (0);
        }

	/* CRC and checksum in one pass.
 	 */
        for (i=0; i < bytes_read && i < BUFSIZ; i++) {
            index = (crc ^ buf[i]) & 0xFF;
            crc = ((crc >> 8) & 0x00FFFFFF) ^ crc_tab[index];
            s += buf[i];
	}
    }

    r = (s & 0xffff) + ((s & 0xffffffff) >> 16);
    s = (r & 0xffff) + (r >> 16);

    *crc_out = (crc ^ 0xFFFFFFFF);

    return (s);
}


/**
 * DTS_FILEVALIDATE -- Validate a file against one or more checksum values.
 *
 *  @fn valid = dts_fileValidate (char *fname, uint sum32, uint crc, char *md5)
 *
 *  @param  fname	data buffer
 *  @param  sum32	32-bit checksum
 *  @param  crc		32-bit CRC value
 *  @param  md5		128-bit MD5 hash
 *  @returns		1 if file matches given values.
 */
int
dts_fileValidate (char *fname, unsigned int sum32, unsigned int crc, char *md5)
{
    unsigned int f_sum32 = 0, f_crc = 0;
    char  *f_md5 = NULL;
    int    status = OK;
    struct timeval  t1, t2, t3, t4;


    memset (&t1, 0, sizeof (struct timeval));	/* initialize timers	*/
    memset (&t2, 0, sizeof (struct timeval));
    memset (&t3, 0, sizeof (struct timeval));
    memset (&t4, 0, sizeof (struct timeval));

    if (TIME_DEBUG || PERF_DEBUG)
        gettimeofday (&t1, NULL);

    if (DTS_SUM_ALL) {
        if (crc > 0 && sum32 > 0) {
	    /*  If we have both a CRC and SUM32 compute them in a single pass.
	     */
            f_sum32 = dts_fileCRCChecksum (fname, &f_crc);
            if (crc != f_crc) {
	        dtsLog (dts, "Error: CRC failed for '%s', %d != %d\n", 
		    fname, crc, f_crc);
	        status = ERR;
	    }
            if (sum32 != f_sum32) {
	        dtsLog (dts, "Error: SUM32 failed for '%s', %d != %d\n", 
		    fname, sum32, f_sum32);
	        status = ERR;
	    }

            if (TIME_DEBUG || PERF_DEBUG)
                gettimeofday (&t2, NULL);
        } else {
	    /*  Otherwise, compute them individually.
	     */
            if (crc > 0 && (crc != dts_fileCRC32 (fname))) {
	        dtsLog (dts, "Error: CRC failed for '%s', %d != %d\n", 
		    fname, crc, f_crc);
	        status = ERR;
	    }
            if (sum32 > 0 && (sum32!=dts_fileChecksum(fname,DTS_SUM32_TYPE))) {
	        dtsLog (dts, "Error: SUM32 failed for '%s', %d != %d\n", 
		    fname, sum32, f_sum32);
	        status = ERR;
	    }

            if (TIME_DEBUG || PERF_DEBUG)
                gettimeofday (&t3, NULL);
        }
    }

    /* Now check the MD5 hash if we were given one.
     */
    if (md5 && md5[0] && (f_md5 = dts_fileMD5 (fname))) {
	if (strcmp (md5, f_md5) != 0) {
	    dtsLog (dts, "Error: MD5 failed for '%s', %s != %s\n", 
		fname, md5, f_md5);
	    status = ERR;
	}
	if (f_md5) free ((char *) f_md5);
    }


    gettimeofday (&t4, NULL);
    if (dts->verbose > 2)
	dtsLog (dts, "%6.6s <  XFER: file validation time: %g\n", "",
	    dts_timediff (t1, t4));

    if (TIME_DEBUG || PERF_DEBUG) {
	dtsLog (dts, "fileValidate: t4-t1=%g\n", dts_timediff (t1, t4));
        if (DTS_SUM_ALL) {
          if (crc > 0 && sum32 > 0)
	    dtsLog (dts, "fileValidate: t2-t1=%g\n", dts_timediff (t1, t2));
	  else
	    dtsLog (dts, "fileValidate: t3-t1=%g\n", dts_timediff (t1, t3));
	}
    }

    return ( status );
}


/**
 * CHECKSUM -- Increment the checksum of a character array.  The
 * calling routine must zero the checksum initially.  Shorts are
 * assumed to be 16 bits, ints 32 bits.
 *
 *  @fn checksum (unsigned char *data, int length, ushort *sum16, uint *sum32)
 *
 *  @param  data	data buffer
 *  @param  length	length of data buffer
 *  @param  sum16	16-bit checksum (output)
 *  @param  sum32	32-bit checksum (output)
 */

void
checksum (unsigned char *data, int length, ushort *sum16, uint *sum32)
{
	int	 	len, remain, i;
	unsigned int	hi, lo, hicarry, locarry, tmp16;
	unsigned char   *buf = data;


	*sum16 = 0;
	*sum32 = 0;
	len = 4*(length / 4);	/* make sure len is a multiple of 4 */
	remain = length % 4;	/* add remaining bytes below */

	/* Extract the hi and lo words - the 1's complement checksum
	 * is associative and commutative, so it can be accumulated in
	 * any order subject to integer and short integer alignment.
	 * By separating the odd and even short words explicitly, both
	 * the 32 bit and 16 bit checksums are calculated (although the
	 * latter follows directly from the former in any case) and more
	 * importantly, the carry bits can be accumulated efficiently
	 * (subject to short integer overflow - the buffer length should
	 * be restricted to less than 2**17 = 131072).
	 */
	hi = (*sum32 >> 16);
	lo = *sum32 & 0xFFFF;

	if (len > 131072) {
	    fprintf (stderr, "Error: checksum buffer length > 131072\n");
	    return;
	}

	for (i=0; i < len; i+=4) {
	    hi += (buf[i]   << 8) + buf[i+1];
	    lo += (buf[i+2] << 8) + buf[i+3];
	}

	/* any remaining bytes are zero filled on the right
	 */
	if (remain) {
	    if (remain >= 1)
		hi += buf[2*len] * 0x100;
	    if (remain >= 2)
		hi += buf[2*len+1];
	    if (remain == 3)
		lo += buf[2*len+2] * 0x100;
	}

	/* fold the carried bits back into the hi and lo words
	 */
	hicarry = hi >> 16;
	locarry = lo >> 16;

	while (hicarry || locarry) {
	    hi = (hi & 0xFFFF) + locarry;
	    lo = (lo & 0xFFFF) + hicarry;
	    hicarry = hi >> 16;
	    locarry = lo >> 16;
	}

	/* simply add the odd and even checksums (with carry) to get the
	 * 16 bit checksum, mask the two to reconstruct the 32 bit sum
	 */
	tmp16 = hi + lo;
	while (tmp16 >> 16)
	    tmp16 = (tmp16 & 0xFFFF) + (tmp16 >> 16);

	*sum16 = tmp16;
	*sum32 = (hi << 16) + lo;
}


/**
 *  ADDCHECK32 -- Internet checksum, 32 bit unsigned integer version.
 *
 *  @fn   unsigned int addcheck32 (unsigned char *array, int length)
 *
 *  @param  array	data buffer
 *  @param  length	length of data buffer (bytes)
 *  @return 		32-bit checksum value
 *
 */

unsigned int 
addcheck32 (unsigned char *array, int length)
{
    register int i;
    unsigned int *iarray, sum = 0;
    int      len, carry=0, newcarry=0;

    iarray = (unsigned int *) array;
    len = length / 4;

    for (i=0; i<len; i++) {
        if (iarray[i] > ~ sum)
            carry++;

        sum += iarray[i];
    }

    while (carry) {
        if (carry > ~ sum)
            newcarry++;
        sum += carry;
        carry = newcarry;
        newcarry = 0;
    }

    return (sum);
}

/*****************************************************************************/


/******************************************************************************
 *
 *  MD5 Checksum routines.  Source used verbatim from the Linux 'md5sum'
 *  utility.
 *
 ******************************************************************************/


/* Declaration of functions and data types used for MD5 sum computing
   library functions.
   Copyright (C) 1995-1997, 1999-2001, 2004-2006, 2008-2010 Free Software
   Foundation, Inc.
   This file is part of the GNU C Library.

   This program is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by the
   Free Software Foundation; either version 3, or (at your option) any
   later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.  */

#ifndef _MD5_H
#define _MD5_H 1

#include <stdio.h>
#include <stdint.h>

#define MD5_DIGEST_SIZE 16
#define MD5_BLOCK_SIZE 64

#ifndef __GNUC_PREREQ
# if defined __GNUC__ && defined __GNUC_MINOR__
#  define __GNUC_PREREQ(maj, min)                                       \
  ((__GNUC__ << 16) + __GNUC_MINOR__ >= ((maj) << 16) + (min))
# else
#  define __GNUC_PREREQ(maj, min) 0
# endif
#endif

#ifndef __THROW
# if defined __cplusplus && __GNUC_PREREQ (2,8)
#  define __THROW       throw ()
# else
#  define __THROW
# endif
#endif

#ifndef _LIBC
# define __md5_buffer md5_buffer
# define __md5_finish_ctx md5_finish_ctx
# define __md5_init_ctx md5_init_ctx
# define __md5_process_block md5_process_block
# define __md5_process_bytes md5_process_bytes
# define __md5_read_ctx md5_read_ctx
# define __md5_stream md5_stream
#endif

# ifdef __cplusplus
extern "C" {
# endif

/* Structure to save state of computation between the single steps.  */
struct md5_ctx
{
  uint32_t A;
  uint32_t B;
  uint32_t C;
  uint32_t D;

  uint32_t total[2];
  uint32_t buflen;
  uint32_t buffer[32];
};

/*
 * The following three functions are build up the low level used in
 * the functions `md5_stream' and `md5_buffer'.
 */

/* Initialize structure containing state of computation.
   (RFC 1321, 3.3: Step 3)  */
extern void __md5_init_ctx (struct md5_ctx *ctx) __THROW;

/* Starting with the result of former calls of this function (or the
   initialization function update the context for the next LEN bytes
   starting at BUFFER.
   It is necessary that LEN is a multiple of 64!!! */
extern void __md5_process_block (const void *buffer, size_t len,
                                 struct md5_ctx *ctx) __THROW;

/* Starting with the result of former calls of this function (or the
   initialization function update the context for the next LEN bytes
   starting at BUFFER.
   It is NOT required that LEN is a multiple of 64.  */
extern void __md5_process_bytes (const void *buffer, size_t len,
                                 struct md5_ctx *ctx) __THROW;

/* Process the remaining bytes in the buffer and put result from CTX
   in first 16 bytes following RESBUF.  The result is always in little
   endian byte order, so that a byte-wise output yields to the wanted
   ASCII representation of the message digest.  */
extern void *__md5_finish_ctx (struct md5_ctx *ctx, void *resbuf) __THROW;


/* Put result from CTX in first 16 bytes following RESBUF.  The result is
   always in little endian byte order, so that a byte-wise output yields
   to the wanted ASCII representation of the message digest.  */
extern void *__md5_read_ctx (const struct md5_ctx *ctx, void *resbuf) __THROW;


/* Compute MD5 message digest for bytes read from STREAM.  The
   resulting message digest number will be written into the 16 bytes
   beginning at RESBLOCK.  */
extern int __md5_stream (FILE *stream, void *resblock) __THROW;

/* Compute MD5 message digest for LEN bytes beginning at BUFFER.  The
   result is always in little endian byte order, so that a byte-wise
   output yields to the wanted ASCII representation of the message
   digest.  */
extern void *__md5_buffer (const char *buffer, size_t len,
                           void *resblock) __THROW;

# ifdef __cplusplus
}
# endif

#endif /* md5.h */



/********************************** md5.c  **********************************/

/* Functions to compute MD5 message digest of files or memory blocks.
   according to the definition of MD5 in RFC 1321 from April 1992.
   Copyright (C) 1995, 1996, 1997, 1999, 2000, 2001, 2005, 2006, 2008, 2009,
   2010 Free Software Foundation, Inc.
   This file is part of the GNU C Library.

   This program is free software; you can redistribute it and/or modify it
   under the terms of the GNU General Public License as published by the
   Free Software Foundation; either version 3, or (at your option) any
   later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.  */

/* Written by Ulrich Drepper <drepper@gnu.ai.mit.edu>, 1995.  */


#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#if USE_UNLOCKED_IO
# include "unlocked-io.h"
#endif

#ifdef _LIBC
# include <endian.h>
# if __BYTE_ORDER == __BIG_ENDIAN
#  define WORDS_BIGENDIAN 1
# endif
/* We need to keep the namespace clean so define the MD5 function
   protected using leading __ .  */
# define md5_init_ctx __md5_init_ctx
# define md5_process_block __md5_process_block
# define md5_process_bytes __md5_process_bytes
# define md5_finish_ctx __md5_finish_ctx
# define md5_read_ctx __md5_read_ctx
# define md5_stream __md5_stream
# define md5_buffer __md5_buffer
#endif

#ifdef WORDS_BIGENDIAN
# define SWAP(n)                                                        \
    (((n) << 24) | (((n) & 0xff00) << 8) | (((n) >> 8) & 0xff00) | ((n) >> 24))
#else
# define SWAP(n) (n)
#endif

#define BLOCKSIZE 32768
#if BLOCKSIZE % 64 != 0
# error "invalid BLOCKSIZE"
#endif

/* This array contains the bytes used to pad the buffer to the next
   64-byte boundary.  (RFC 1321, 3.1: Step 1)  */
static const unsigned char fillbuf[64] = { 0x80, 0, 0, 0, 0, 0, 0, 0,
					      0, 0, 0, 0, 0, 0, 0, 0, 
					      0, 0, 0, 0, 0, 0, 0, 0, 
					      0, 0, 0, 0, 0, 0, 0, 0, 
					      0, 0, 0, 0, 0, 0, 0, 0, 
					      0, 0, 0, 0, 0, 0, 0, 0, 
					      0, 0, 0, 0, 0, 0, 0, 0, 
					      0, 0, 0, 0, 0, 0, 0, 0 };

/* Initialize structure containing state of computation.
   (RFC 1321, 3.3: Step 3)  */
void
md5_init_ctx (struct md5_ctx *ctx)
{
  ctx->A = 0x67452301;
  ctx->B = 0xefcdab89;
  ctx->C = 0x98badcfe;
  ctx->D = 0x10325476;

  ctx->total[0] = ctx->total[1] = 0;
  ctx->buflen = 0;
  memset (ctx->buffer, 0, (sizeof(uint32_t) * 32));
}

/* Copy the 4 byte value from v into the memory location pointed to by *cp,
   If your architecture allows unaligned access this is equivalent to
   * (uint32_t *) cp = v  */
static inline void
set_uint32 (char *cp, uint32_t v)
{
  memcpy (cp, &v, sizeof v);
}

/* Put result from CTX in first 16 bytes following RESBUF.  The result
   must be in little endian byte order.  */
void *
md5_read_ctx (const struct md5_ctx *ctx, void *resbuf)
{
  char *r = resbuf;
  set_uint32 (r + 0 * sizeof ctx->A, SWAP (ctx->A));
  set_uint32 (r + 1 * sizeof ctx->B, SWAP (ctx->B));
  set_uint32 (r + 2 * sizeof ctx->C, SWAP (ctx->C));
  set_uint32 (r + 3 * sizeof ctx->D, SWAP (ctx->D));

  return resbuf;
}

/* Process the remaining bytes in the internal buffer and the usual
   prolog according to the standard and write the result to RESBUF.  */
void *
md5_finish_ctx (struct md5_ctx *ctx, void *resbuf)
{
  /* Take yet unprocessed bytes into account.  */
  uint32_t bytes = ctx->buflen;
  size_t size = (bytes < 56) ? 64 / 4 : 64 * 2 / 4;

  /* Now count remaining bytes.  */
  ctx->total[0] += bytes;
  if (ctx->total[0] < bytes)
    ++ctx->total[1];

  /* Put the 64-bit file length in *bits* at the end of the buffer.  */
  ctx->buffer[size - 2] = SWAP (ctx->total[0] << 3);
  ctx->buffer[size - 1] = SWAP ((ctx->total[1] << 3) | (ctx->total[0] >> 29));

  memcpy (&((char *) ctx->buffer)[bytes], fillbuf, (size - 2) * 4 - bytes);

  /* Process last bytes.  */
  md5_process_block (ctx->buffer, size * 4, ctx);

  return md5_read_ctx (ctx, resbuf);
}

/* Compute MD5 message digest for bytes read from STREAM.  The
   resulting message digest number will be written into the 16 bytes
   beginning at RESBLOCK.  */
static int
md5_stream (FILE *stream, void *resblock)
{
  struct md5_ctx ctx;
  size_t sum;

  char *buffer = calloc (1, BLOCKSIZE + 72);
  if (!buffer)
    return 1;

  /* Initialize the computation context.  */
  md5_init_ctx (&ctx);

  /* Iterate over full file contents.  */
  while (1) {
      /* We read the file in blocks of BLOCKSIZE bytes.  One call of the
         computation function processes the whole buffer so that with the
         next round of the loop another block can be read.  */
      size_t n;
      sum = 0;

      /* Read block.  Take care for partial reads.  */
      while (1) {
          n = fread (buffer + sum, 1, BLOCKSIZE - sum, stream);

          sum += n;

          if (sum == BLOCKSIZE)
            break;

          if (n == 0) {
              /* Check for the error flag IFF N == 0, so that we don't
                 exit the loop after a partial read due to e.g., EAGAIN
                 or EWOULDBLOCK.  */
              if (ferror (stream)) {
                  free (buffer);
                  return 1;
              }
              goto process_partial_block;
            }

          /* We've read at least one byte, so ignore errors.  But always
             check for EOF, since feof may be true even though N > 0.
             Otherwise, we could end up calling fread after EOF.  */
          if (feof (stream))
            goto process_partial_block;
        }

      /* Process buffer with BLOCKSIZE bytes.  Note that
         BLOCKSIZE % 64 == 0
       */
      md5_process_block (buffer, BLOCKSIZE, &ctx);
    }

process_partial_block:

  /* Process any remaining bytes.  */
  if (sum > 0)
    md5_process_bytes (buffer, sum, &ctx);

  /* Construct result in desired memory.  */
  md5_finish_ctx (&ctx, resblock);
  free ((char *) buffer);
  return 0;
}

/* Compute MD5 message digest for LEN bytes beginning at BUFFER.  The
   result is always in little endian byte order, so that a byte-wise
   output yields to the wanted ASCII representation of the message
   digest.  */
void *
md5_buffer (const char *buffer, size_t len, void *resblock)
{
  struct md5_ctx ctx;

  /* Initialize the computation context.  */
  md5_init_ctx (&ctx);

  /* Process whole buffer but last len % 64 bytes.  */
  md5_process_bytes (buffer, len, &ctx);

  /* Put result in desired memory area.  */
  return md5_finish_ctx (&ctx, resblock);
}


void
md5_process_bytes (const void *buffer, size_t len, struct md5_ctx *ctx)
{
  /* When we already have some bits in our internal buffer concatenate
     both inputs first.  */
  if (ctx->buflen != 0)
    {
      size_t left_over = ctx->buflen;
      size_t add = 128 - left_over > len ? len : 128 - left_over;

      memcpy (&((char *) ctx->buffer)[left_over], buffer, add);
      ctx->buflen += add;

      if (ctx->buflen > 64)
        {
          md5_process_block (ctx->buffer, ctx->buflen & ~63, ctx);

          ctx->buflen &= 63;
          /* The regions in the following copy operation cannot overlap.  */
          memcpy (ctx->buffer,
                  &((char *) ctx->buffer)[(left_over + add) & ~63],
                  ctx->buflen);
        }

      buffer = (const char *) buffer + add;
      len -= add;
    }

  /* Process available complete blocks.  */
  if (len >= 64)
    {
#if !_STRING_ARCH_unaligned
# define alignof(type) offsetof (struct { char c; type x; }, x)
# define UNALIGNED_P(p) (((size_t) p) % alignof (uint32_t) != 0)
      if (UNALIGNED_P (buffer))
        while (len > 64)
          {
            md5_process_block (memcpy (ctx->buffer, buffer, 64), 64, ctx);
            buffer = (const char *) buffer + 64;
            len -= 64;
          }
      else
#endif
        {
          md5_process_block (buffer, len & ~63, ctx);
          buffer = (const char *) buffer + (len & ~63);
          len &= 63;
        }
    }

  /* Move remaining bytes in internal buffer.  */
  if (len > 0)
    {
      size_t left_over = ctx->buflen;

      memcpy (&((char *) ctx->buffer)[left_over], buffer, len);
      left_over += len;
      if (left_over >= 64)
        {
          md5_process_block (ctx->buffer, 64, ctx);
          left_over -= 64;
          memcpy (ctx->buffer, &ctx->buffer[16], left_over);
        }
      ctx->buflen = left_over;
    }
}


/* These are the four functions used in the four steps of the MD5 algorithm
   and defined in the RFC 1321.  The first function is a little bit optimized
   (as found in Colin Plumbs public domain implementation).  */
/* #define FF(b, c, d) ((b & c) | (~b & d)) */
#define FF(b, c, d) (d ^ (b & (c ^ d)))
#define FG(b, c, d) FF (d, b, c)
#define FH(b, c, d) (b ^ c ^ d)
#define FI(b, c, d) (c ^ (b | ~d))

/* Process LEN bytes of BUFFER, accumulating context into CTX.
   It is assumed that LEN % 64 == 0.  */

void
md5_process_block (const void *buffer, size_t len, struct md5_ctx *ctx)
{
  uint32_t correct_words[16];
  const uint32_t *words = buffer;
  size_t nwords = len / sizeof (uint32_t);
  const uint32_t *endp = words + nwords;
  uint32_t A = ctx->A;
  uint32_t B = ctx->B;
  uint32_t C = ctx->C;
  uint32_t D = ctx->D;

  /* First increment the byte count.  RFC 1321 specifies the possible
     length of the file up to 2^64 bits.  Here we only compute the
     number of bytes.  Do a double word increment.  */
  ctx->total[0] += len;
  if (ctx->total[0] < len)
    ++ctx->total[1];

  /* Process all bytes in the buffer with 64 bytes in each round of
     the loop.  */
  while (words < endp)
    {
      uint32_t *cwp = correct_words;
      uint32_t A_save = A;
      uint32_t B_save = B;
      uint32_t C_save = C;
      uint32_t D_save = D;

      /* First round: using the given function, the context and a constant
         the next context is computed.  Because the algorithms processing
         unit is a 32-bit word and it is determined to work on words in
         little endian byte order we perhaps have to change the byte order
         before the computation.  To reduce the work for the next steps
         we store the swapped words in the array CORRECT_WORDS.  */

#define OP(a, b, c, d, s, T)                                            \
      do                                                                \
        {                                                               \
          a += FF (b, c, d) + (*cwp++ = SWAP (*words)) + T;             \
          ++words;                                                      \
          CYCLIC (a, s);                                                \
          a += b;                                                       \
        }                                                               \
      while (0)

      /* It is unfortunate that C does not provide an operator for
         cyclic rotation.  Hope the C compiler is smart enough.  */
#define CYCLIC(w, s) (w = (w << s) | (w >> (32 - s)))

      /* Before we start, one word to the strange constants.
         They are defined in RFC 1321 as

         T[i] = (int) (4294967296.0 * fabs (sin (i))), i=1..64

         Here is an equivalent invocation using Perl:

         perl -e 'foreach(1..64){printf "0x%08x\n", int (4294967296 * abs (sin $_))}'
       */

      /* Round 1.  */
      OP (A, B, C, D, 7, 0xd76aa478);
      OP (D, A, B, C, 12, 0xe8c7b756);
      OP (C, D, A, B, 17, 0x242070db);
      OP (B, C, D, A, 22, 0xc1bdceee);
      OP (A, B, C, D, 7, 0xf57c0faf);
      OP (D, A, B, C, 12, 0x4787c62a);
      OP (C, D, A, B, 17, 0xa8304613);
      OP (B, C, D, A, 22, 0xfd469501);
      OP (A, B, C, D, 7, 0x698098d8);
      OP (D, A, B, C, 12, 0x8b44f7af);
      OP (C, D, A, B, 17, 0xffff5bb1);
      OP (B, C, D, A, 22, 0x895cd7be);
      OP (A, B, C, D, 7, 0x6b901122);
      OP (D, A, B, C, 12, 0xfd987193);
      OP (C, D, A, B, 17, 0xa679438e);
      OP (B, C, D, A, 22, 0x49b40821);

      /* For the second to fourth round we have the possibly swapped words
         in CORRECT_WORDS.  Redefine the macro to take an additional first
         argument specifying the function to use.  */
#undef OP
#define OP(f, a, b, c, d, k, s, T)                                      \
      do                                                                \
        {                                                               \
          a += f (b, c, d) + correct_words[k] + T;                      \
          CYCLIC (a, s);                                                \
          a += b;                                                       \
        }                                                               \
      while (0)

      /* Round 2.  */
      OP (FG, A, B, C, D, 1, 5, 0xf61e2562);
      OP (FG, D, A, B, C, 6, 9, 0xc040b340);
      OP (FG, C, D, A, B, 11, 14, 0x265e5a51);
      OP (FG, B, C, D, A, 0, 20, 0xe9b6c7aa);
      OP (FG, A, B, C, D, 5, 5, 0xd62f105d);
      OP (FG, D, A, B, C, 10, 9, 0x02441453);
      OP (FG, C, D, A, B, 15, 14, 0xd8a1e681);
      OP (FG, B, C, D, A, 4, 20, 0xe7d3fbc8);
      OP (FG, A, B, C, D, 9, 5, 0x21e1cde6);
      OP (FG, D, A, B, C, 14, 9, 0xc33707d6);
      OP (FG, C, D, A, B, 3, 14, 0xf4d50d87);
      OP (FG, B, C, D, A, 8, 20, 0x455a14ed);
      OP (FG, A, B, C, D, 13, 5, 0xa9e3e905);
      OP (FG, D, A, B, C, 2, 9, 0xfcefa3f8);
      OP (FG, C, D, A, B, 7, 14, 0x676f02d9);
      OP (FG, B, C, D, A, 12, 20, 0x8d2a4c8a);

      /* Round 3.  */
      OP (FH, A, B, C, D, 5, 4, 0xfffa3942);
      OP (FH, D, A, B, C, 8, 11, 0x8771f681);
      OP (FH, C, D, A, B, 11, 16, 0x6d9d6122);
      OP (FH, B, C, D, A, 14, 23, 0xfde5380c);
      OP (FH, A, B, C, D, 1, 4, 0xa4beea44);
      OP (FH, D, A, B, C, 4, 11, 0x4bdecfa9);
      OP (FH, C, D, A, B, 7, 16, 0xf6bb4b60);
      OP (FH, B, C, D, A, 10, 23, 0xbebfbc70);
      OP (FH, A, B, C, D, 13, 4, 0x289b7ec6);
      OP (FH, D, A, B, C, 0, 11, 0xeaa127fa);
      OP (FH, C, D, A, B, 3, 16, 0xd4ef3085);
      OP (FH, B, C, D, A, 6, 23, 0x04881d05);
      OP (FH, A, B, C, D, 9, 4, 0xd9d4d039);
      OP (FH, D, A, B, C, 12, 11, 0xe6db99e5);
      OP (FH, C, D, A, B, 15, 16, 0x1fa27cf8);
      OP (FH, B, C, D, A, 2, 23, 0xc4ac5665);

      /* Round 4.  */
      OP (FI, A, B, C, D, 0, 6, 0xf4292244);
      OP (FI, D, A, B, C, 7, 10, 0x432aff97);
      OP (FI, C, D, A, B, 14, 15, 0xab9423a7);
      OP (FI, B, C, D, A, 5, 21, 0xfc93a039);
      OP (FI, A, B, C, D, 12, 6, 0x655b59c3);
      OP (FI, D, A, B, C, 3, 10, 0x8f0ccc92);
      OP (FI, C, D, A, B, 10, 15, 0xffeff47d);
      OP (FI, B, C, D, A, 1, 21, 0x85845dd1);
      OP (FI, A, B, C, D, 8, 6, 0x6fa87e4f);
      OP (FI, D, A, B, C, 15, 10, 0xfe2ce6e0);
      OP (FI, C, D, A, B, 6, 15, 0xa3014314);
      OP (FI, B, C, D, A, 13, 21, 0x4e0811a1);
      OP (FI, A, B, C, D, 4, 6, 0xf7537e82);
      OP (FI, D, A, B, C, 11, 10, 0xbd3af235);
      OP (FI, C, D, A, B, 2, 15, 0x2ad7d2bb);
      OP (FI, B, C, D, A, 9, 21, 0xeb86d391);

      /* Add the starting values of the context.  */
      A += A_save;
      B += B_save;
      C += C_save;
      D += D_save;
    }

  /* Put checksum in context given as argument.  */
  ctx->A = A;
  ctx->B = B;
  ctx->C = C;
  ctx->D = D;
}
