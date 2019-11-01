/** 
 *  DTSASYNC.C
 * 
 *  Methods used in Asynchronous commands.
 *
 *  @brief  	DTS asynchronous command methods.
 *
 *  @file       dtsASync.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/10/09
 */
/*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include "dts.h"



/**
 *  DTS_NULLRESPONSE -- Asyncronous null handler.
 *
 *  @brief  Asyncronous null handler.
 *  @fn     int dts_nullResponse (void *data)
 *
 *  @param  data	xml caller data
 *  @return		status code
 */
int 
dts_nullResponse (void *data)
{
    /* Not yet implemented
    xr_setIntInResult (data, OK);
     */
    return (OK);
}
