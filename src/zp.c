#include <stdio.h>
#include <string.h>
#include <string.h>



int main (int argc, char *argv[])
{
    const char *path = "/tmp/foo.fits.fz";
    char  s[1024];
    extern char *dts_pathFname(char *s);

    memset (s, 0, 1024);
    strcpy (s, path);

    fprintf (stderr, "'%s' -> '%s'\n", s, dts_pathFname (s));
}
