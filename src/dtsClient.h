/**
 *  DTSCLIENT.H -- Client-side DTS data structures and macro definitions.
 *
 */


/*  Default DTSD and DTSQ config files.  Can be modified to produce binaries
 *  for specific projects.
 */
char *def_dtsConfig = {
#   include "def_dtsConfig.h"
};

char *def_dtsqConfig = {
#   include "def_dtsqConfig.h"
};


extern int	use_builtin_config;
