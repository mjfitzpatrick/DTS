/**
 *  XRCLIENT.C
 *
 *  Client-side procedures used by an application to make a call to a
 *  remote service.  In this interface, the calling application must
 *  first initialize a client context for a specific service, then
 *  create a parameter array of arguments before invoking the function
 *  either synchronously or asynchronously.  Once the call completes,
 *  API procedures are used to extract the result values.
 *
 *  Client methods:
 *							// call services
 *             cnum = xr_initClient (char *url, char *name, char *version)
 *                      xr_callSync (cnum, char *name)
 *                     xr_callASync (cnum, char *name, void *func, .....)
 *                   xr_closeClient (cnum)
 *
 *                     xr_initParam (cnum)		// init params
 *                    xr_setVerbose (verbose)		// verbose flag
 *                      xr_setDebug (debug)		// debug flag
 *
 *                 xr_setIntInParam (cnum, int ival)	// scalar param
 *             xr_setpDoubleInParam (cnum, double dval)
 *               xr_setpBoolInParam (cnum, int bval)
 *             xr_setpStringInParam (cnum, char *str)
 *           xr_setpDatetimeInParam (cnum, char *date)
 *             xr_setpStructInParam (cnum, int snum)
 *              xr_setpArrayInParam (cnum, int anum)
 *
 *       stat = xr_getIntFromResult (cnum, int *ival)	// get results
 *    stat = xr_getDoubleFromResult (cnum, double *dval)
 *      stat = xr_getBoolFromResult (cnum, int *bval)
 *    stat = xr_getStringFromResult (cnum, char **value)
 *  stat = xr_getDatetimeFromResult (cnum, char **date)
 *    stat = xr_getStructFromResult (cnum)
 *     stat = xr_getArrayFromResult (cnum)
 * 
 *                      xr_envClean (cnum)		// cleanup
 *                     xr_freeParam (cnum)
 *                    xr_freeResult (cnum)
 *                 xr_clientCleanup (cnum)
 *
 * 
 *         stat = xr_resultArrayInt (int anum, int index, int *value)
 *      stat = xr_resultArrayDouble (int anum, int index, int *value)
 *        stat = xr_resultArrayBool (int anum, int index, int *value)
 *      stat = xr_resultArrayString (int anum, int index, char **value)
 *    stat = xr_resultArrayDatetime (int anum, int index, char **value)
 *      stat = xr_resultArrayStruct (int anum, int index, int *value)
 *  
 *
 *  @brief      Client-side procedures
 *
 *  @file       xrClient.c
 *  @author     Mike Fitzpatrick
 *  @date       6/10/09
 */



#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <stdarg.h>
#include <string.h>
#include <assert.h>

#include <xmlrpc-c/base.h>
#include <xmlrpc-c/client.h>
#include <xmlrpc-c/server.h>
#include <xmlrpc-c/server_abyss.h>

#include "xrpcP.h"

#define	 DEBUG		        0
/*
#define  REUSE_CLIENT 		1
#define  CLIENT_RING		1
#define  ALLOC_CLIENTS		1
#define  CLIENT_LOCK		1
*/
#define  USE_DEFAULT_TRANSPORT	1	// 4/10/14 added back
#define  ALLOC_ARRAY		1
#define  CLIENT_START 		3	/* slot zero reserved for monitor   */


int	client_errstat		= OK;
int	num_asynch_threads 	= 0;
int	client_debug		= 0;
int	client_verbose		= 0;



pthread_mutex_t async_mutex  = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t client_mutex = PTHREAD_MUTEX_INITIALIZER;


#ifndef ALLOC_CLIENTS
#endif
//static Client   clientArray[MAX_CLIENTS];
#ifdef ALLOC_ARRAY
Client   *clientArray 	= (Client *) NULL;
#else
Client   clientArray[MAX_CLIENTS];
#endif
#ifdef CLIENT_RING
static int 	nclients 	= CLIENT_START;
#else
static int 	nclients 	= -1;
#endif
static int 	array_init 	= 0;

int 		global_init 	= 0;

typedef struct {
    int           cnum;
    char         *name;
    Client       *client;
    xmlrpc_value *param;
    xmlrpc_value *result;
    void         *ret_handler;
} ASynch, *ASynchP;


static void *xr_asynchRunner (void *arg);

static void xr_defaultAsynchHandler (char *serverUrl, 
   char *methodName,
   xmlrpc_value *paramArrayP, void *user_data, xmlrpc_env *faultP,
   xmlrpc_value *resultP);

static Client *xr_getClientByNum (int cnum);



/* ***********************************************************************
** INITCLIENT - Initialize the client-side environment
*************************************************************************/

void
xr_reallocClients ()
{
#ifdef ALLOC_ARRAY
    Client *new = (Client *) calloc (MAX_CLIENTS, sizeof (Client));

    memcpy (new, clientArray, (MAX_CLIENTS * sizeof(Client)));
    if (clientArray)
  	free (clientArray);
    clientArray = new;
#endif
}


int
xr_newASync (int cnum)
{
    int      aclient;
    ClientP  client = xr_getClientByNum (cnum);

    aclient = xr_initClient (client->url, "async", "v1.0");
    xr_initParam (aclient);

    return (aclient);
}

int
xr_freeASync (int cnum)
{
    ClientP  client = xr_getClientByNum (cnum);

    if (cnum < 0)
	return (ERR);

    if ( client->param )
        xr_freeParam (cnum);
    if ( client->result )
        xr_freeResult (cnum);

    /* Clean up our error-handling environment.
     */
    xmlrpc_env_init (&client->env);
    xmlrpc_env_clean (&client->env);    

    return (OK);
}


int
xr_setClient (int cnum, char *url)
{
    ClientP  client = xr_getClientByNum (cnum);
    int   client_num = cnum;

    if (array_init == 0) {
#ifdef ALLOC_ARRAY
        clientArray = (Client *) calloc (MAX_CLIENTS, sizeof (Client));
#else
	memset (clientArray, 0, sizeof (clientArray));
#endif
	array_init++;
    }

#ifdef ALLOC_CLIENTS
    if (! cnum) {
	client = calloc (1, sizeof (Client));
	client->in_use++;
        strcpy (client->url, url);
	client_num = xr_setHandle ( cnum, (void *) client );
//fprintf (stderr, "setClient: cnum=%d  ptr=0x%x\n", cnum, xr_H2P ((long) cnum));

    } else {
        memset (client, 0, sizeof(Client));
        strcpy (client->url, url);
    }

#else
    memset (client, 0, sizeof(Client));
    strcpy (client->url, url);
#endif
//fprintf (stderr, "setClient: cnum=%d  ptr=0x%x\n", cnum, client);

    return (client_num);
}


int
xr_initClient (char *url, char *name, char *version)
{
    int     client_num = CLIENT_START;
#ifdef ALLOC_CLIENTS
    int     new_client = 0;
#endif
    struct  xmlrpc_clientparms clientParms;
    ClientP client = (ClientP) NULL;


    /*  Ensure a single client.
     */
#ifdef CLIENT_LOCK
    int lock = pthread_mutex_lock (&client_mutex);
#endif

    if (array_init == 0) {
#ifdef ALLOC_ARRAY
        clientArray = (Client *) calloc (MAX_CLIENTS, sizeof (Client));
#else
	memset (clientArray, 0, sizeof (clientArray));
#endif
	array_init++;
    }


#ifdef ALLOC_CLIENTS
    client = calloc (1, sizeof (Client));
    new_client++;
    client->in_use++;
    strcpy (client->url, url);	/* save the service url		*/
    nclients++;

#else						// ALLOC_CLIENTS

    /*  Initialize XML-RPC interface.  We create a private client object so
    **  we can use asynchronous calls to get the response.
    */
    memset (&clientParms, 0, sizeof(clientParms));
        
#ifdef CLIENT_RING
    //client_num = (nclients + 1) % MAX_CLIENTS;
    client_num = nclients % MAX_CLIENTS;
    client = xr_getClientByNum (client_num);
    memset (client, 0, sizeof (Client) );
    client->in_use = 1;
    strcpy (client->url, url);		/* save the service url		*/
    client_errstat = OK;
    nclients++;
//fprintf (stderr, "ring: cnum = %d  client = 0x%x  nclients = %d\n",
//    client_num, client, nclients);

#else						// CLIENT_RING
    for (client_num=CLIENT_START; client_num < MAX_CLIENTS; client_num++) {
        client = xr_getClientByNum (client_num);

#ifdef REUSE_CLIENT
	/*  If we've already seen this client before, we've already created
	 *  the RPC client.
	 */
	if (client->url[0] && strcmp (url, client->url) == 0) {
	    if (client->in_use) {
		continue;
	    } else {
    	        client->in_use = 1;
    	        client_errstat = OK;
	        return (client_num);
	    }
	} 
#endif
	if (! client->in_use) {
	    memset (client, 0, sizeof (Client) );
    	    client->in_use++;		/* increment counters		*/
    	    strcpy (client->url, url);	/* save the service url		*/
    	    nclients++;
	    break;
	}
    }
#endif						// CLIENT_RING
#endif						// ALLOC_CLIENTS


    xmlrpc_env_init (&client->env);
    if (! global_init++)
        xmlrpc_client_setup_global_const (&client->env);
#ifdef CLIENT_LOCK
    if (client->env.fault_occurred)
        lock = pthread_mutex_unlock (&client_mutex);
#endif
    die_on_error (&client->env);

#ifdef USE_DEFAULT_TRANSPORT
    xmlrpc_client_create (&client->env, XMLRPC_CLIENT_NO_FLAGS, name, version, 
                          &clientParms, 0, 
			  &client->rpc_client);
#else
    clientParms.transport = "curl";
    xmlrpc_client_create (&client->env, XMLRPC_CLIENT_NO_FLAGS, name, version, 
                          &clientParms, XMLRPC_CPSIZE(transport), 
			  &client->rpc_client);
#endif
#ifdef CLIENT_LOCK
    if (client->env.fault_occurred)
        lock = pthread_mutex_unlock (&client_mutex);
#endif
    die_on_error (&client->env);

#ifdef ALLOC_CLIENTS
    client_num = xr_newHandle ( (void *) client );
#endif
    client_errstat = OK;

#ifdef CLIENT_LOCK
    lock = pthread_mutex_unlock (&client_mutex);
#endif

//fprintf (stderr, "initClient: cnum=%d  ptr=0x%x url='%s'\n", client_num, client, url);
    return (client_num);
}


int
xr_closeClient (int cnum)
{
    ClientP client = xr_getClientByNum (cnum);


    if (cnum < 0 || client == (ClientP) NULL)
	return (ERR);

#ifdef CLIENT_LOCK
    int lock = pthread_mutex_lock (&client_mutex);
#endif
    if ( client->param )
        xr_freeParam (cnum);
    if ( client->result )
        xr_freeResult (cnum);

    /* Clean up our error-handling environment.
     */
    xmlrpc_env_init (&client->env);
    xmlrpc_env_clean (&client->env);    

    /*  Close down the client.
     */
    if (client->rpc_client)
        xmlrpc_client_destroy (client->rpc_client);
    /*
    xmlrpc_client_teardown_global_const ();
    global_init    = 0;
    */


    /*  If we're allocating the clients individually, free it here.
     *  Otherwise, clear the structure in the static clientArray.
     */
    memset (client, 0, sizeof(Client));
#ifdef ALLOC_CLIENTS
    free ((void *) client);
    xr_freeHandle (cnum);
#endif
#ifndef CLIENT_RING
    nclients--;
#endif

#ifdef CLIENT_LOCK
    lock = pthread_mutex_unlock (&client_mutex);
#endif

//fprintf (stderr, "closeClient: cnum=%d  ptr=0x%x\n", cnum, client);
    return (OK);
}


int xr_pause () { int i = 0;  i++;  return (i); }


/*  CALLSYNC -- Make a synchronous service call.  The result value is parsed 
**  in the caller via the API and using the 'result' pointer gotten here.
*/
int
xr_callSync (int cnum, char *method_name)
{
    xmlrpc_value *result = (xmlrpc_value *) NULL;
    ClientP client = xr_getClientByNum (cnum);
    int     ntries = 0;
    //char    inc_call_sync = 1;

    xmlrpc_server_info *serverInfoP = 
	xmlrpc_server_info_new (&client->env, client->url);


    if (XRPC_DEBUG)
	fprintf (stderr, "callSync: cnum = %d  %s\n", cnum, method_name);
    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    client_errstat = OK;
    xmlrpc_env_init (&client->env);

try_again_:
    xmlrpc_client_call2 (&client->env, client->rpc_client, serverInfoP,
		method_name, client->param, &result);

    if (client->env.fault_occurred) {
	client_errstat = ERR;
	if (client_verbose)
            warn_on_error (&client->env);
	if (strcmp (method_name, "ping")   != 0 &&
	    strcmp (method_name, "poke")   != 0 &&
	    strcmp (method_name, "dtslog") != 0 &&
	    strcmp (method_name, "dtstat") != 0) {
		char fault[2048];

	        xr_pause();
		memset (fault, 0, SZ_LINE);
		if (client->env.fault_string);
		    strcpy (fault, client->env.fault_string);
                xmlrpc_env_init (&client->env);
	        if (ntries++ < 30) {
	           sleep (3);
	           fprintf (stderr, "callSync(%s) retry: '%s'\n", 
			method_name, fault);
	           goto try_again_;
	        }
	}
    } else
	client_errstat = OK;

    // Travis added server stat counting the number of succesful Callsyncs
    //    FIXME -- This isn't thread safe when operating multiple queues.
    //xr_setStat (inc_call_sync);


    /*  The result is a <params> array with a single <param> entry whose
    **  <value> will be one of the base XML-RPC types.  In the case of an
    **  <array> or <struct> we need to extract data from those sepearately.
    */
    client->result = result;

    /* We have the call results, so free the parameters.
    xr_freeParam (cnum);
    */
    xmlrpc_server_info_free (serverInfoP);	/* clean up		*/

    return (client_errstat);
}



/*  CALLASYNC -- Make an asynchronous service call.  We do this by spawning 
**  a thread to make the call and handle the result.  Treat each asynch
**  message as a new client connection.
*/

int
xr_callASync (int cnum, char *name, void *ret_handler)
{
    ClientP client = xr_getClientByNum (cnum);
    pthread_attr_t attr;                /* wait thread attribute      */
    pthread_t      tnum;               	/* wait thread                */
    ASynch  *as = calloc (1, sizeof(ASynch));


    /* Get a new client connection for the thread. 	-- FIXME
   
    tcnum = xr_initClient (client->url, "xrASync", "v1.0");
    tclient = xr_getClientByNum (tcnum);

    memcpy (&clientArray[tcnum], &clientArray[cnum], sizeof(Client) );
    xmlrpc_INCREF (tclient->param);
    */


    /* Create a detatched thread in which to run.
    */
    pthread_attr_init (&attr);
    pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);

    as->name = name;
    as->cnum = cnum;
    as->client = client;
    as->ret_handler = ret_handler;

    /* Start the asynch wait thread.
    */
    num_asynch_threads++;
    if (pthread_create (&tnum, &attr, (void *)xr_asynchRunner, (void *)as)) {
        perror ("Cannot start asynch wait thread");
        exit (-1);
    }

    return (OK);
}


static void *
xr_asynchRunner ( void *arg )
{
    xmlrpc_env  env;
    xmlrpc_value *result = (xmlrpc_value *) NULL;
    xmlrpc_server_info *serverInfoP;
    xmlrpc_client *clientP;

    ASynch  *as = (ASynch *)(arg);
    ClientP client = as->client;
    int client_num = as->cnum;
    char *url = as->client->url;


    xmlrpc_env_init (&env);
    serverInfoP = xmlrpc_server_info_new (&env, url);

    /* Setup.
    */
    xmlrpc_client_setup_global_const (&env);
    xmlrpc_client_create (&env, XMLRPC_CLIENT_NO_FLAGS, XR_NAME, XR_VERSION,
	NULL, 0, &clientP);

    client_errstat = OK;
    as->client->handlerFunc = as->ret_handler;

    /* Make the asynchronous call and send it off by calling the finish
    ** routine.
    */
    xmlrpc_env_init (&env);
/*
    xmlrpc_client_start_rpc (&env, clientP, serverInfoP, 
	as->name, client->param,
	(xmlrpc_response_handler) xr_defaultAsynchHandler, 
	(void *)&client_num);
*/
    xmlrpc_client_start_rpc (&env, clientP, serverInfoP, 
	as->name, client->param, xr_defaultAsynchHandler, (void *)&client_num);

    if (env.fault_occurred) {
	client_errstat = ERR;
        if (client_verbose)
	    warn_on_error (&client->env);
        xmlrpc_env_init (&client->env);
    }

    xmlrpc_client_event_loop_finish (clientP);

    /*
    **  The result is a <params> array with a single <param> entry whose
    **  <value> will be one of the base XML-RPC types.  In the case of an
    **  <array> or <struct> we need to extract data from those sepearately.
    */
    as->client->result = as->result = result;

    xmlrpc_server_info_free (serverInfoP);	/* clean up		*/
    client->in_use = 0;
    xr_closeClient (client_num);

    free ((void *)as);

    pthread_exit (&client_errstat);		/* exit the thread	*/
}


/* Default asynchronous response handler.
*/
static void
xr_defaultAsynchHandler (char *serverUrl, char *methodName,
   xmlrpc_value *paramArrayP,
   void *user_data,
   xmlrpc_env *faultP,
   xmlrpc_value *resultP)
{
    int *cnum = (int *)user_data;
    ClientP client = xr_getClientByNum (*cnum);
    int  lock;


    if (resultP) {
        xmlrpc_INCREF(resultP);
        client->result = resultP;

        if ( (*(PFI)(client->handlerFunc))((void *)cnum) )
            fprintf (stderr, "Error calling asynch handler function\n");

    } else if (faultP->fault_occurred) {
        fprintf (stderr, "Error in asynch (%s):  %s\n", 
	    methodName, faultP->fault_string);
	client->env = *faultP;
    }

    /*  Decrement the open thread counter.
    */
    lock = pthread_mutex_lock (&async_mutex);
    num_asynch_threads--;
    if (num_asynch_threads < 0)
	num_asynch_threads = 0;
    lock = pthread_mutex_unlock (&async_mutex);
}


/*  XR_ASYNCWAIT -- Wait for all pending asyncronous requests to complete.
**
*/
int
xr_asyncWait ()
{
    register int wait = 0;


    while (1) {
        pthread_mutex_lock (&async_mutex);
	wait = num_asynch_threads;
        pthread_mutex_unlock (&async_mutex);

	if (wait)
	    sleep (1);
	else
	    break;
    }

    return (OK);
}




/***************************************************************************
**  Set parameter values.  This code is used by the programming calling 
**  the service, i.e. the 'Client'.  As such we 'set' the parameters for the
**  call.  It is in the method implementation on the server side that we
**  'get' the params from the call.
****************************************************************************/

/*  Initialize the parameter calling array. 
**
*/
void
xr_initParam (int cnum)
{
    ClientP client = xr_getClientByNum (cnum);


    if (XRPC_DEBUG)
	fprintf (stderr, "initParam: cnum = %d\n", cnum);
    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    /* If we're initializing the calling parameters, free up an previous
    ** params, results or status flags.
    */
    if ( client->param )
	xr_freeParam ( cnum );
    if ( client->result )
	xr_freeResult ( cnum );
    client_errstat = OK;


    /* Allocate an array for the parameters. 
    */
    xmlrpc_env_init (&client->env);
    client->param = xmlrpc_array_new (&client->env);

    if (client_verbose)
	warn_on_error (&client->env);
}


/*
**  Flag utilities.
*/
void xr_setVerbose (int verbose) { client_verbose = verbose; }
void xr_setDebug   (int debug  ) { client_debug   = debug;   }



/* Set parameters of various types.  If the param was initialized as an
** an array simply append the new value, otherwise overwrite the param
** with the specified value after freeing it.
*/
void
xr_setIntInParam (int cnum, int value)
{
    ClientP client = xr_getClientByNum (cnum);
    xmlrpc_value *v = (xmlrpc_value *) NULL;


    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    xmlrpc_env_init (&client->env);		/* initialize env	*/
    v = xmlrpc_int_new (&client->env, value);

    if (! client->param )
        client->param = xmlrpc_array_new (&client->env);
  
    xmlrpc_array_append_item (&client->env, client->param, v);
    if (client_verbose)
	warn_on_error (&client->env);
    xmlrpc_DECREF(v);				/* free temp value	*/
}


void
xr_setDoubleInParam (int cnum, double value)
{
    ClientP client = xr_getClientByNum (cnum);
    xmlrpc_value *v = (xmlrpc_value *) NULL;


    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    xmlrpc_env_init (&client->env);
    v = xmlrpc_double_new (&client->env, value);

    if (! client->param)	/* safety initialization	*/
	client->param = xmlrpc_array_new (&client->env);

    xmlrpc_array_append_item (&client->env, client->param, v);
    if (client_verbose)
	warn_on_error (&client->env);
    xmlrpc_DECREF(v);
}

void
xr_setBoolInParam (int cnum, int value)
{
    ClientP client = xr_getClientByNum (cnum);
    xmlrpc_value *v = (xmlrpc_value *) NULL;


    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    xmlrpc_env_init (&client->env);
    v = xmlrpc_bool_new (&client->env, (xmlrpc_bool) value);

    if (! client->param)	/* safety initialization	*/
	client->param = xmlrpc_array_new (&client->env);

    xmlrpc_array_append_item (&client->env, client->param, v);
    if (client_verbose)
	warn_on_error (&client->env);
    xmlrpc_DECREF(v);
}

void
xr_setStringInParam (int cnum, char *str)
{
    ClientP client = xr_getClientByNum (cnum);
    xmlrpc_value *v = (xmlrpc_value *) NULL;


    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    xmlrpc_env_init (&client->env);
    v = xmlrpc_string_new (&client->env, str);

    if (! client->param)	/* safety initialization	*/
	client->param = xmlrpc_array_new (&client->env);

    xmlrpc_array_append_item (&client->env, client->param, v);
    if (client_verbose)
	warn_on_error (&client->env);
    xmlrpc_DECREF(v);
}

void
xr_setDatetimeInParam (int cnum, char *str)
{
    ClientP client = xr_getClientByNum (cnum);
    xmlrpc_value *v = (xmlrpc_value *) NULL;


    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    xmlrpc_env_init (&client->env);
    v = xmlrpc_datetime_new_str (&client->env, (const char *)str);

    if (! client->param)	/* safety initialization	*/
	client->param = xmlrpc_array_new (&client->env);

    xmlrpc_array_append_item (&client->env, client->param, v);
    if (client_verbose)
	warn_on_error (&client->env);
    xmlrpc_DECREF(v);
}


void
xr_setStructInParam (int cnum, int snum)
{
    ClientP client = xr_getClientByNum (cnum);


    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (! client->param)        		/* safety initialization      */
        client->param = xmlrpc_array_new (&client->env);

    xmlrpc_env_init (&client->env);
    xmlrpc_array_append_item (&client->env, client->param,
	xr_getSParam(snum));
    if (client_verbose)
	warn_on_error (&client->env);
}

void
xr_setArrayInParam (int cnum, int anum)
{
    ClientP client = xr_getClientByNum (cnum);


    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (! client->param)        		/* safety initialization      */
        client->param = xmlrpc_array_new (&client->env);

    xmlrpc_env_init (&client->env);
    xmlrpc_array_append_item (&client->env, client->param,
	xr_getAElement(anum));
    if (client_verbose)
	warn_on_error (&client->env);
}




/***************************************************************************
**  Get/Set result values.
****************************************************************************/

int
xr_getIntFromResult (int cnum, int *value)
{
    ClientP client = xr_getClientByNum (cnum);

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (client_errstat == OK) {
        xmlrpc_env_init (&client->env);
	xmlrpc_read_int (&client->env, client->result, value); 

        return (client->env.fault_occurred ? ERR : OK);
    } else
	return (ERR);
}


int
xr_getDoubleFromResult (int cnum, double *value)
{
    ClientP client = xr_getClientByNum (cnum);

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (client_errstat == OK) {
	xmlrpc_read_double (&client->env, client->result, value);
        return (client->env.fault_occurred ? ERR : OK);
    } else
	return (ERR);
}


int
xr_getBoolFromResult (int cnum, int *value)
{
    ClientP client = xr_getClientByNum (cnum);

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (client_errstat == OK) {
        xmlrpc_env_init (&client->env);
	xmlrpc_read_bool (&client->env, client->result, (xmlrpc_bool *) value);
        return (client->env.fault_occurred ? ERR : OK);
    } else
	return (ERR);
}

int
xr_getStringFromResult (int cnum, char **value)
{
    ClientP client = xr_getClientByNum (cnum);
    const char *str = (char *) NULL;

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (client_errstat == OK) {
        xmlrpc_env_init (&client->env);
	xmlrpc_read_string (&client->env, client->result, &str);
	if (str) {
            if (*value) 
		strcpy (*value, str);
	    else
		*value = strdup (str);
	    free ((char *) str);
	}
        return (client->env.fault_occurred ? ERR : OK);

    } else
	return (ERR);
}

int
xr_getDatetimeFromResult (int cnum, char **date)
{
    ClientP client = xr_getClientByNum (cnum);
    const char *str = (char *) NULL;

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (client_errstat == OK) {
        xmlrpc_env_init (&client->env);
	xmlrpc_read_datetime_str (&client->env, client->result, &str);
        strcpy (*date, str);
	if (str) free ((char *) str);
        return (client->env.fault_occurred ? ERR : OK);

    } else
	return (ERR);
}

int
xr_getStructFromResult (int cnum, int *snum)
{
    ClientP client = xr_getClientByNum (cnum);

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (client_errstat == OK) {
	*snum = xr_newStruct ();		/* FIXME	*/
	xr_setSParam (*snum, client->result);

        return (client->env.fault_occurred ? ERR : OK);
    } else
	return (ERR);
}

int
xr_getArrayFromResult (int cnum, int *anum)
{
    ClientP client = xr_getClientByNum (cnum);

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (client_errstat == OK) {
	*anum = xr_newArray ();
	xr_setAElement (*anum, client->result);

        return (client->env.fault_occurred ? ERR : OK);
    } else
	return (ERR);
}


/***************************************************************************
**  Utility procedures.
****************************************************************************/

/**
 *  XR_GETERRMSG -- Get the error message from the last call.
 */
char *
xr_getErrMsg (int cnum)
{
    ClientP client = xr_getClientByNum (cnum);

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    return (client->env.fault_string);
}


/**
 *  XR_GETERRCODE -- Get the error code from the last call.
 */
int
xr_getErrCode (int cnum)
{
    ClientP client = xr_getClientByNum (cnum);

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    return (client->env.fault_occurred);
}



/***************************************************************************
**  Cleanup procedures.
****************************************************************************/

/**
 *  XR_ENVCLEAN -- Clean the environment of the specified client.
 */
void
xr_envClean (int cnum)
{
    ClientP client = xr_getClientByNum (cnum);

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    xmlrpc_env_clean (&client->env);		/* cleanup error-handling env */
}


/**
 *  XR_FREEPARAM -- Free the Client parameter xmlrpc_value.
 */
void
xr_freeParam (int cnum)
{
    ClientP client = xr_getClientByNum (cnum);
    int    refcount = 0;
    extern int xmlrpc_refcount();

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (client->param) {
	/*
        int len = xmlrpc_array_size (&client->env, client->param);
	xmlrpc_value *v;

	while (len >= 0) {
	    xmlrpc_array_read_item (&client->env, client->param, --len, &v);
            warn_on_error (&client->env);
	    xmlrpc_DECREF (v);
	}
	*/
        refcount = xmlrpc_refcount (client->param) - 1;
        if (refcount > 0)
	    xmlrpc_DECREF (client->param);
    	/*
	xmlrpc_env_clean(&client->env);
	*/
    }

    if (refcount == 0)
	client->param = (xmlrpc_value *) NULL;
}


/**
 *  Free the Client result xmlrpc_value.
 */
void
xr_freeResult (int cnum)
{
    ClientP client = xr_getClientByNum (cnum);
    int    refcount = 0;
    extern int xmlrpc_refcount();

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    if (client->result) {
	/*
        int len = xmlrpc_array_size (&client->env, client->result);
	xmlrpc_value *v;

	while (len >= 0) {
	    xmlrpc_array_read_item (&client->env, client->result, --len, &v);
            warn_on_error (&client->env);
	    xmlrpc_DECREF (v);
	}
	*/
        refcount = xmlrpc_refcount (client->result) - 1;
        if (refcount > 0)
	    xmlrpc_DECREF (client->result);
    	/*
	xmlrpc_env_clean (&client->env);
	*/
    }

    if (refcount == 0)
	client->result = (xmlrpc_value *) NULL;
}


/**
 *  Shutdown our XML-RPC client library.
 */
void
xr_clientCleanup (int cnum)
{
    assert (cnum < MAX_CLIENTS);		/* validate the client number */
/*
    if (client->rpc_client)		
        xmlrpc_client_cleanup ();
*/
    xr_envClean (cnum);				/* cleanup error-handling env */
}


void
printClient (int cnum)
{
    ClientP client = xr_getClientByNum (cnum);

    assert (cnum < MAX_CLIENTS);		/* validate the client number */

    fprintf (stderr, "Client %d\n", cnum);
    fprintf (stderr, "     url = '%s'\n",  client->url);
    fprintf (stderr, "   param = 0x%x\n", (int) client->param);
    fprintf (stderr, "  result = 0x%x\n", (int) client->result);
    fprintf (stderr, "  client = 0x%x\n", (int) client->rpc_client);
    fprintf (stderr, "  in_use = %d\n",   client->in_use);
}


/***************************************************************************/
/****  Private Procedures                                               ****/
/***************************************************************************/


static Client *
xr_getClientByNum (int cnum)
{
    ClientP client = (ClientP) NULL;

    if (array_init == 0) {
#ifdef ALLOC_ARRAY
        clientArray = (Client *) calloc (MAX_CLIENTS, sizeof (Client));
#else
	memset (clientArray, 0, sizeof (clientArray));
#endif
	array_init++;
    }

#ifdef ALLOC_CLIENTS
    if (cnum >= CLIENT_START) {
        client = xr_H2P ((long) cnum);
    } else {
        client = &clientArray[cnum];
    }
#else
    client = &clientArray[cnum];
#endif

//fprintf (stderr, "getClient: cnum=%d  ptr=0x%x\n", cnum, client);
    return (client);
}

