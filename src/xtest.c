#include <stdio.h>
#include "dts.h"

DTS *dts = (DTS *) NULL;

int main (int argc, char *argv[])
{
    /*
    dts_fileCopy (
	"/usr/local/var/spool/dts/spool/podi/0/b20120825T134253.0",
	"/usr/local/ODI_incoming_raw/zztest");
    */

    printf ("local host = '%s'\n", dts_getLocalHost());
}
