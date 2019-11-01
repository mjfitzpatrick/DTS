
/*
 * sum -- checksum and count the blocks in a file
 *     Like BSD sum or SysV sum -r, except like SysV sum if -s option is given.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


unsigned int buf[BUFSIZ];

enum { 
    SUM_BSD, 
    PRINT_NAME, 
    SUM_SYSV 
};

/* BSD: calculate and print the rotated checksum and the size in 1K blocks
   The checksum varies depending on sizeof (int). */
/* SYSV: calculate and print the checksum and the size in 512-byte blocks */
/* Return 1 if successful.  */

static unsigned 
sum_file (const char *file, int do_sysv)
{
    unsigned int total_bytes = 0;
    int fd, r;

    /* The sum of all the input bytes, modulo (UINT_MAX + 1).  */
    unsigned int s = 0;


    fd = open (file, O_RDONLY);
    if (fd == -1)
        return 0;

    while (1) {
        size_t bytes_read = read (fd, buf, BUFSIZ);

        if ((size_t) bytes_read <= 0) {
            r = (fd && close(fd) != 0);
            if (!bytes_read && !r)
                /* no error */
                break;
            return (0);
        }

        total_bytes += bytes_read;
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
        printf("%d %d %s\n", s, (total_bytes + 511) / 512, file);
    } else
        printf("%05d %d %s\n", s, (total_bytes + 1023) / 1024, file);
                
    return 1;
}


int 
	    
main(int argc, char **argv)
{
    unsigned int n, do_sysv = 1;

	/*
	 */
    if (!argv[0]) {
        /* Do not print the name */
        n = sum_file("-", do_sysv);

    } else {
        /*  Need to print the name if either
         *    - more than one file given
         *    - doing sysv 
	     */
        n = 1;
	    ++argv;
        do {
            n &= sum_file(*argv, do_sysv);
        } while (*++argv);
    }

    return !n;
}
