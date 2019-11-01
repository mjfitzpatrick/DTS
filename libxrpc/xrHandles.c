/**
 *  XRHANDLE.C  -- Utility methods to convert client pointers to user handles.
 *
 *  @brief      Utility methods to convert client pointers to user handles.
 *
 *  @file       xrHandles.c
 *  @author     Mike Fitzpatrick
 *  @date       April 2014
 */

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

#include "xrpcP.h"



#define	MAX_HANDLES		8192
#define HANDLE_DEBUG		0
#define START_H			3


int      numXrHandles         	= START_H;
handle_t xrHandles[MAX_HANDLES];

static int  init_handles	= 0;





/**
 *  XR_INITHANDLES -- Initialize the handles structure.
 * 
 *  @brief  Initialize the handles structure.
 *  @fn	    void xr_initHandles (void)
 *
 *  @return		nothing
 */
void
xr_initHandles (void)
{
    /*  Initialize the handle-to-ptr converter the first time we're called,
     *  or whenever we've restarted.
     */
//fprintf (stderr, "initHandles:  num = %d\n", numXrHandles);
    if (numXrHandles == START_H)
	memset (&xrHandles[START_H], 0, 
	    sizeof (xrHandles) - ((START_H * sizeof(handle_t))) );
}


/**
 *  XR_CLEANUPHANDLES -- Cleanup the handles structure.
 * 
 *  @brief  Cleanup the handles structure.
 *  @fn	    void xr_cleanupHandles (void)
 *
 *  @return		nothing
 */
void
xr_cleanupHandles (void)
{
    register int i;

    for (i=numXrHandles; i >= START_H; i--) {
	if (xrHandles[i]) {
	    free ((void *) xrHandles[i]);
	    xrHandles[i] = 0;
 	}
    }
    numXrHandles = START_H;
}


/*  Utility routines for keep track of handles.
*/

/**
 *  XR_SETHANDLE -- Set the handle manually.
 * 
 *  @brief  Set the handle manually.
 *  @fn	    handle_t xr_setHandle (int handle, void *ptr)
 *
 *  @param  handle	handle to be used
 *  @param  ptr		pointer to object to be stored
 *  @return		new object handle
 */
handle_t
xr_setHandle (int handle, void *ptr)
{
    if (! init_handles++)
	xr_initHandles ();

    /*  Explicitly set the handle-to-ptr value.
     */
    if (handle >= 0)
        xrHandles[handle] = (long) ptr;

    return (handle);
}


/**
 *  XR_NEWHANDLE -- Get an unused object handle.
 * 
 *  @brief  Get an unused object handle
 *  @fn	    handle_t xr_newHandle (void *ptr)
 *
 *  @param  ptr		pointer to object to be stored
 *  @return		new object handle
 */
handle_t
xr_newHandle (void *ptr)
{
    if (! init_handles++)
	xr_initHandles ();

    /*  Initialize the handle-to-ptr converter the first time we're called,
     *  or whenever we've restarted.
     */
    if (numXrHandles == START_H)
	memset (&xrHandles[START_H], 0, 
	    sizeof (xrHandles) - ((START_H * sizeof(handle_t))) );
    xrHandles[++numXrHandles] = (long) ptr;

    return (numXrHandles);
}


/**
 *  XR_FREEHANDLE -- Free the handle for later re-use.
 *
 *  @brief  Free the handle for later re-use.
 *  @fn     xr_freeHandle (handle_t handle)
 *
 *  @param  handle	object handle
 *  @return 		nothing
 */
void
xr_freeHandle (handle_t handle)
{
    register int i, j;
    void *ptr = xr_H2P (handle);


    if (handle <= START_H) {
	fprintf (stderr, "Error: Attempt to free zero handle!\n");
	return;
    }

    for (i=0; i < MAX_HANDLES; i++) {
	if ((void *) ptr == (void *) xrHandles[i]) {
	    for (j=i+1; j < MAX_HANDLES; j++) {
		if (xrHandles[j])
		    xrHandles[i++] = xrHandles[j];
		else
		    break;
	    }
	    numXrHandles = ((numXrHandles-1) >= START_H ? 
		(numXrHandles-1) : START_H);
	    break;
 	}
    }
}


/**
 *  XR_P2H -- Convert a pointer to a handle
 *
 *  @brief  Convert a pointer to a handle
 *  @fn     handle_t xr_P2H (void *ptr)
 *
 *  @param  ptr		pointer to object
 *  @return 		handle to object, < 0 on error
 */
handle_t	
xr_P2H (void *ptr)
{
    register int i;

    for (i=0; i < MAX_HANDLES; i++) {
	if ((void *) ptr == (void *) xrHandles[i])
	    return ((int) i);
    }

    return (0);
}


/**
 *  XR_H2P -- Convert a handle to a pointer
 *
 *  @brief  Convert a handle to a pointer
 *  @fn     void *xr_H2P (int handle) 
 *
 *  @param  handle	object handle
 *  @return 		pointer to object or NULL
 */
void *
xr_H2P (handle_t handle) 
{ 
    return ((void *) xrHandles[handle]); 
}
