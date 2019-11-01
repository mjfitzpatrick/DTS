
#include <stdio.h>
#include <stdlib.h>
#include <sys/file.h>

int main (int argc, char *argv[]) 
{
    FILE *fd = (FILE *) NULL;
    char  buf[1024];
    int   nval = 0;


    memset (buf, 0, 1024);
    if ((fd = fopen ("/tmp/test", "r+")) == (FILE *) NULL) {
	fprintf (stderr, "Cannot open /tmp/test\n");
	exit (1);
    }

fprintf (stdout, "reading file ....\n", atoi (buf));
    flock (fileno(fd), LOCK_EX);
    fgets (buf, 1024, fd);
fprintf (stdout, "got %d\n", atoi (buf));
sleep (10);

    nval = atoi (buf) + 1;
    rewind (fd);
    fprintf (fd, "%d\n", nval);
    fflush (fd);
    flock (fileno(fd), LOCK_UN);
    fclose (fd);

fprintf (stderr, "wrote %d\n", nval);

}

