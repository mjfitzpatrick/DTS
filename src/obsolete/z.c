
#include <stdio.h>

int main (int argc, char **argv)
{
    char *s1 = "2009-11-07T11:57:50";
    char *s2 = "2009-12-07T11:57:50";
    char *s3 = "2009-12-07T11:57:51";

    fprintf (stderr, "s1/s2 = %d\n", strcmp (s1, s2));
    fprintf (stderr, "s1/s3 = %d\n", strcmp (s1, s3));
    fprintf (stderr, "s2/s3 = %d\n", strcmp (s2, s3));

    fprintf (stderr, "s2/s1 = %d\n", strcmp (s2, s1));
    fprintf (stderr, "s3/s1 = %d\n", strcmp (s3, s1));
    fprintf (stderr, "s3/s2 = %d\n", strcmp (s3, s2));
}
