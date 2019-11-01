/**
 *
 *  DTSCONSOLE.C
 *
 *  DTS Console Interface - These are the methods implemented for he 
 *  XML-RPC interace.  These command are used from a variety of locations
 *  during the operation of the DTS (i.e. the queueing, logging or
 *  interactive environments.
 *
 *	monAttach	- attach logging to the specified dtsmonitor
 *
 *
 *  @brief      DTS Console Interface
 *
 *  @file       dtsConsole.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/15/09
 */
/*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <ctype.h>

#include "dts.h"


extern  DTS  *dts;
extern  int   dts_monitor;



/*******************************************************************************
**	monAttach	- attach logging to the specified dtsmonitor
**	monConsole	- enable remote console
**	monDetach	- detach from current dtsmonitor
*/

/**
 *  DTS_MONATTACH -- Attach the DTS logging output to the dtsmon at the
 *  specified host.
 *
 *  @fn int dts_monAttach (void *data)
 *
 *  @param  data	calling parameter data
 #  @return  		status (OK|ERR)
 */
int 
dts_monAttach (void *data)
{
    register int client;
    char    *monUrl = xr_getStringFromParam (data, 0);


    if (dts->mon_fd >= 0) {
        dtsLog (dts, "Switching to monitor at '%s'", monUrl);
	xr_asyncWait ();
	xr_closeClient (dts->mon_fd);
    }

    if ((client = xr_initClient (monUrl, dts->whoAmI, DTS_VERSION)) < 0)
        dts->mon_fd = dts_monitor = client;

    dtsLog (dts, "Attaching to monitor at '%s'", monUrl);

    xr_setIntInResult (data, OK);       /* No useful result returned .... */
    free ((char *) monUrl);

    xr_closeClient (client);

    return (OK);
}


/**
 *  DTS_MODETACH -- Detach the DTS logging output to the current dtsmon.
 *
 *  @fn int dts_monDetach (void *data)
 *
 *  @param  data	calling parameter data
 #  @return  		status (OK|ERR)
 */
int 
dts_monDetach (void *data)
{
    register int client;
    char  monUrl[SZ_PATH], host[SZ_FNAME];


    if (dts->mon_fd >= 0) {
        dtsLog (dts, "Detach from monitor");
	xr_asyncWait ();
	xr_closeClient (dts->mon_fd);
    } else {
        xr_setIntInResult (data, ERR);
	return (OK);
    }

    /*  Try to reconnect to a local DTSmon.
    */
    memset (host, 0, SZ_FNAME);
    memset (monUrl, 0, SZ_PATH);

    gethostname (host, SZ_FNAME);

    sprintf (monUrl, "http://%s:2999/RPC2", host);
    if ((client = xr_initClient (monUrl, dts->whoAmI, DTS_VERSION)) < 0) {
        dts->mon_fd = dts_monitor = client;
        dtsLog (dts, "Attach to local monitor at '%s'", monUrl);
    } else
        dtsLog (dts, "Cannot attach to monitor at '%s'", monUrl);

    xr_setIntInResult (data, OK);       /* No useful result returned .... */

    xr_closeClient (client);

    return (OK);
}


/**
 *  DTS_MONCONSOLE -- Request connection as a controlling console.  We
 *  provide a (minimal security) password that must be supplied to grant
 *  access.
 *
 *  @fn int dts_monConsole (void *data)
 *
 *  @param  data	calling parameter data
 #  @return  		status (OK|ERR)
 */

int 
dts_monConsole (void *data)
{
    register int client;
    char *monUrl = xr_getStringFromParam (data, 0);
    char *passwd = xr_getStringFromParam (data, 1);


    if (strcmp (passwd, DEF_PASSWD)) {
        xr_setIntInResult (data, ERR);
	return (OK);
    }

    if (dts->mon_fd >= 0) {
        dtsLog (dts, "Switching to monitor at '%s'", monUrl);
	xr_asyncWait ();
	xr_closeClient (dts->mon_fd);
    }

    if ((client = xr_initClient (monUrl, dts->whoAmI, DTS_VERSION)) < 0)
        dts->mon_fd = dts_monitor = client;

    
    dtsLog (dts, "Creating console at '%s'", monUrl);

    xr_setIntInResult (data, OK);       /* No useful result returned .... */
    free ((void *) monUrl);
    free ((void *) passwd);

    xr_closeClient (client);

    return (OK);
}
