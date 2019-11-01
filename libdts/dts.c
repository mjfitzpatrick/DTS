/**
 *  DTS.C -- DTS interface procedures,
 *
 *  @brief	DTS interface procedures,
 *
 *  @file  	dts.c
 *  @author  	Mike Fitzpatrick, NOAO
 *  @date	6/15/09
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <pthread.h>

#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>

#include "dts.h"
#include "dtsdb.h"




#define	_DTS_SRC_		1

int    queue_delay		= 0;
int    first_write 		= 1;

int    use_builtin_config	= 0;



/**
 *  DTSINIT -- Initialize the DTS daemon structure.
 *
 *  @brief  Initialize the DTS daemon structure.
 *  @fn     DTS *dtsInit (char *config, int port, char *host, char *root, 
 *		int initialize)
 *
 *  @param  config      DTS config file
 *  @param  port        DTS server port number
 *  @param  host        DTS server host name
 *  @param  root        DTS root directory
 *  @param  initialize  initialize root directory?
 *  @return             pointer to DTS structure
 */
DTS *
dtsInit (char *config, int port, char *host, char *sroot, int initialize)
{
    DTS *dts = (DTS *) NULL;
    char *root = (char *) NULL, *delay = (char *) NULL;
    char *cfg  = (char *) config;
    char *buf_init = (char *) NULL;
    int   init = initialize;


    /* Initialize the structure and values.
    */
    dts = calloc (1, sizeof (DTS));

    dts->serverPort	= port;
    dts->debug		= 0;		/* debug is set by the API	 */
    dts->mon_fd 	= DTSMON_NONE;
    strcpy (dts->serverHost, host);

    /*  Initialize the application string buffer ring. 
     */
    buf_init 		= dts_strbuf ("dts_init_strbuf");

    /* Create a string identifying the process.
    */
    sprintf (dts->whoAmI, "%s(%d)", (char *) getlogin(), (int) getpid () );


    sprintf (dts->serverUrl, "http://%s:%d/RPC2", host, port);
    if (config) {					/* user-supplied */
	if ( strcasecmp (config, "copyMode") == 0) { /* copy mode	 */
            strcpy (dts->configFile, cfg);
	    dts_loadConfigFile (dts, NULL, host, port, sroot, init);
	    dts_initCopyMode (dts, host, port);
	    return (dts);

	} else if (access (config, R_OK) != 0 && use_builtin_config == 0) {
            strcpy (dts->configFile, "string");		/* user string	 */
	    dts_loadConfigStr (dts, config, host, port, sroot, init);

	} else
            strcpy (dts->configFile, (use_builtin_config ? "string" : config));

    } else if ( (cfg = getenv (DTS_ENV_CONFIG)) ) {	/* environment   */
        strcpy (dts->configFile, cfg);

    } else {						/* defaults      */
	if (access ((cfg = dts_cfgPath ()), R_OK) == 0)
            strcpy (dts->configFile, cfg);
	if (access (cfg, R_OK) != 0) {
	    if (INIT_DEBUG)
		fprintf (stderr, "DBG: Can't access default '%s'\n", cfg);
	    cfg = NULL;
            strcpy (dts->configFile, "string");
	}
    }


    /* Look for a configuration file.
    */
    if (cfg && access (cfg, R_OK) == 0)
	strcpy (dts->configFile, cfg);
    else {
	if (access (DTS_CONFIG1, R_OK) == 0)
	    strcpy (dts->configFile, DTS_CONFIG1);
	else if (access (DTS_CONFIG2, R_OK) == 0)
	    strcpy (dts->configFile, DTS_CONFIG2);
	else if (access (DTS_CONFIG3, R_OK) == 0)
	    strcpy (dts->configFile, DTS_CONFIG3);
	else if (strncmp (dts->configFile, "string", 6) == 0) {
	    memset (dts->configFile, 0, SZ_FNAME);
	    if (use_builtin_config) {
                strcpy (dts->configFile, "string_def");	/* user string	 */
	        dts_loadConfigStr (dts, config, host, port, sroot, init);
	        if (INIT_DEBUG)
		    fprintf (stderr, "DBG: Using builtin config string\n");
	    }
	} else {
	    if (INIT_DEBUG)
		fprintf (stderr, "Error: No DTS config file found.\n");
	    return ((DTS *) NULL);
	}
    }

    /* Load the configuration file, matching the entry with the input
    ** host spec.  This allowsus to run multiple daemons on the same
    ** machine at different ports.
    */
    if (strncmp (dts->configFile, "string", 6) != 0) {
	if (INIT_DEBUG)
	    fprintf (stderr, "DBG: Loading config file '%s'\n",dts->configFile);
        if (dts_isDir (dts->configFile))
	    dts_loadConfigDir (dts, dts->configFile, host, port, sroot, init);
        else
	    dts_loadConfigFile (dts, dts->configFile, host, port, sroot, init);
    }


    /*  Now put us in the DTS root dir.
    */
    root = dts->serverRoot;
    if (root[0]) {
        if (access (root, R_OK|W_OK) == 0) {
            chdir (root);
	    if (geteuid() == 0)
                chroot (root);
        }
    }


    /*  Connect to the logging monitor or else open a local logfile.
#ifdef _DTS_SRC_
    dts_connectToMonitor (dts);
#endif
    */

    /*  Open the local transfer database.
     */
#ifdef USE_DTS_DB
    if (dts->dbFile[0])
	dts_dbOpen (dts->dbFile);
#endif

#ifdef LINUX
    on_exit (dtsOnExit, NULL); 		/* set an onexit handler  	*/
#endif

    /* Get an environment variable to see if we need to slow things down
     * for testing.
     */
    if ((delay = getenv ("DTS_DELAY")))
        queue_delay = (isdigit(delay[0]) ? atoi (delay) : 0);

    return (dts);
}


/**
 *  DTS_INITCOPYMODE -- Initialize the server for copy mode.
 */
void 
dts_initCopyMode (DTS *dts, char *host, int port)
{
    dtsClient *client = calloc (1, sizeof (dtsClient));

    client->clientPort    = port;
    client->clientContact = port - 1;
    client->clientLoPort  = -1;
    client->clientHiPort  = -1;
    strcpy (client->clientNet, "copyMode");
    strcpy (client->clientName, "copy");
    strcpy (client->clientHost, host);
    strcpy (client->clientDir, dts_localCwd());
    strcpy (client->clientCopy, dts_localCwd());
    strcpy (client->clientIP, dts_getLocalIP());
    sprintf (client->clientUrl,"http://%s:%d/RPC2", dts_getLocalIP(), port);

    dts->clients[dts->nclients++] = dts->self = client;
}


/**
 *  DTS_INITSERVERROOT -- Initialize the server root directory.
 */
void 
dts_initServerRoot (DTS *dts)
{
    int  i, ifd;
    char path[SZ_PATH];


    /* Create the spool directory.
     */
    if (access ("spool", R_OK|W_OK|X_OK) != 0)
        mkdir ("spool", DTS_DIR_MODE);

    /* Create the direct-copy directory.
     */
    if (access ("copy", R_OK|W_OK|X_OK) != 0)
        mkdir ("copy", DTS_DIR_MODE);

    /* For each of the queues we're running, create a working spool dir.
    */
    for (i=0; i < dts->nqueues; i++) {

	memset (path, 0, SZ_PATH);
	sprintf (path, "spool/%s", dts->queues[i]->name);

        if (access (path, R_OK|W_OK|X_OK) != 0) {
            mkdir (path, DTS_DIR_MODE);

	    sprintf (path, "spool/%s/current", dts->queues[i]->name);
	    dts_queueSetCurrent (path, 0);

	    memset (path, 0, SZ_PATH);
	    sprintf (path, "spool/%s/next", dts->queues[i]->name);
	    dts_queueSetCurrent (path, 0);
	}
    }

    /* Create a file that names the DTS, this makes it easy to verify 
     * we're talking to the correct DTS if needed.
     */
    memset (path, 0, SZ_PATH);
    sprintf (path, "DTS-%s", dts->whoAmI);
    if (access (path, R_OK) != 0) {
        if ((ifd = creat (path, DTS_DIR_MODE)) > 0)
	    close (ifd);
    }
}


/**
 *  DTSFREE -- Free the DTS daemon structure.
 *
 *  @brief  Free the DTS daemon structure.
 *  @fn     void dtsFree (DTS *dts)
 *
 *  @param  dts         DTS struct pointer
 *  @return             nothing
 */
void 
dtsFree (DTS *dts)
{
#ifdef USE_DTS_DB
    dts_dbClose ();			/* Close any open databases	*/
#endif
    free ((void *) dts);
}


/**
 *  DTS_CONNECTTOMONITOR -- Connect to the logging monitor and/open log file.
 *
 *  @brief  Connoct to the logging monitor and/open log file.
 *  @fn     void dts_connectToMonitor (DTS *dts)
 *
 *  @param  dts         DTS struct pointer
 *  @return             nothing
 */
static void dts_connectWorker (void *data);

void
dts_connectToMonitor (DTS *dts)
{
    int   rc;
    pthread_t  tid;                	/* thread attributes    */
    pthread_attr_t  attr;

    /* Initialize the service processing thread attribute.
    */
    pthread_attr_init (&attr);
    pthread_attr_setdetachstate (&attr, PTHREAD_CREATE_DETACHED);

    /* Handle processing in the background.
     */
#ifdef FORK_MONITOR
    if (fork () == 0)
        sleep (3);
    else
        return;
#else
    if ((rc = pthread_create (&tid, &attr, (void *) dts_connectWorker, 
	(void *) dts))) {
	    fprintf (stderr, "ERROR: cannot create monitor thread\n");
	    return;
    }
#endif

    return;
}

static void
dts_connectWorker (void *data)
{
    DTS *dts = (DTS *) data;


    memset (dts->mon_url, 0, SZ_FNAME);
    sleep (2);

    if (dts->monitor[0]) {
	if (index (dts->monitor, (int)':'))
            sprintf (dts->mon_url, "http://%s/RPC2", dts->monitor);
	else
            sprintf (dts->mon_url, "http://%s:%d/RPC2", 
		dts->monitor, DTSMON_PORT);
    } else
        sprintf (dts->mon_url, "http://%s:%d/RPC2", DTSMON_HOST, DTSMON_PORT);

    xr_setClient (DTSMON_CLIENT, dts->mon_url);
    dts->mon_fd = 1;

    dtsLog (dts, "^bConnecting to monitor from host '%s'^r", 
	dts_getLocalHost());

    /*  Quit the thread once we've made a connection.
    pthread_exit (NULL);
    */
}



/****************************************************************************
 *
 *  Error handling
 */


/**
 *  DTSERROREXIT -- Print an error message and exit.
 *
 *  @brief  Connect to the logging monitor and/open log file.
 *  @fn     void dtsErrorExit (char *msg)
 *
 *  @param  msg         Error message
 *  @return             nothing
 */
void
dtsErrorExit (char *msg)
{
#ifdef _DTS_SRC_
    extern DTS *dts;
    dtsLog (dts, "^s %s ^r", msg);
#endif

    perror (msg);
    exit (1);
}


/**
 *  DTSERRORWARN -- Print a warning message.
 *
 *  @brief  Connect to the logging monitor and/open log file.
 *  @fn     void dtsErrorWarn (char *msg)
 *
 *  @param  msg         Warning message
 *  @return             nothing
 */
void
dtsErrorWarn (char *msg)
{
#ifdef _DTS_SRC_
    extern DTS *dts;
    dtsLog (dts, "^b %s ^r", msg);
#endif

    perror (msg);
    exit (1);
}
