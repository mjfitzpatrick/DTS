/** 
 *  DTSCLIENT.C
 *
 *  Client-side methods.
 *
 *  @brief  DTS Client-side methods.
 *
 *  @file       dtsClient.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/10/09
 */
/*****************************************************************************/


#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

#include <xmlrpc-c/base.h>
#include <xmlrpc-c/client.h>
#include <xmlrpc-c/server.h>
#include <xmlrpc-c/server_abyss.h>

#include "dts.h"


