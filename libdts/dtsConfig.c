/**
 *  DTSCONFIG.C --  DTS Config File Interface 
 *
 *  @brief      DTS Config File Interface
 *
 *  @file       dtsConfig.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/15/09
 */
/*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <math.h>
#include <dirent.h>
#include <ctype.h>

#include "dtsPSock.h"
#include "dts.h"


/*
extern  DTS  *dts;
*/
extern  int   dts_monitor;


#define CFG_DEBUG (getenv("CFG_DBG")!=NULL||access("/tmp/CFG_DBG",F_OK)==0)

#define	DEBUG			CFG_DEBUG

#define CON_GLOBAL		0	/* global context		*/
#define CON_DTS			1	/* DTS daemon context		*/
#define CON_QUEUE		2	/* transfer queue context	*/
#define CON_XFER		3	/* transfer context		*/

#define	CFG_NREQUIRED		9	/* No. required keywords	*/


int	context = CON_GLOBAL;


/*  Private methods.
*/
static char *dts_cfgKey (char *line);
static char *dts_cfgVal (char *line);
static int   dts_cfgBool (char *line);
static int   dts_cfgInt (char *line);

extern char *dtsGets (char *s, int len, FILE *fd);



/**
 *  DTS_LOADCONFIGSTR -- Load the DTS configuration from a user string.
 *
 *  @brief  Load the DTS configuration from a user string.
 *  @fn     void dts_loadConfigDir (DTS *dts, char *cfg, char *dhost, 
 *		int dport, char *droot, int initialize)
 *
 *  @param  dts		DTS struct pointer
 *  @param  cfg		DTS config directory
 *  @param  dhost	DTS daemon host machine
 *  @param  dport	DTS daemon port number
 *  @param  droot	DTS daemon root directory
 *  @param  initialize	initialize DTS?
 *  @return		nothing
 */
void
dts_loadConfigStr (DTS *dts, char *cfg, char *dhost, int dport, char *droot, 
	int initialize)
{
    char   path[SZ_PATH];
    FILE  *fd = (FILE *) NULL;


    if (dts == (DTS *) NULL) {
	fprintf (stderr, "Error, dts_loadConfigStr() NULL dts pointer\n");
	return;
    }
    if (dts->configFile == NULL && cfg == NULL) {
	fprintf (stderr, "Error, dts_loadConfigStr() NULL config string\n");
	return;
    }

    memset (path, 0, SZ_PATH);
    sprintf (path, "/tmp/dtscfg%06d", (int) getpid());

    if ((fd = fopen (path, "w+")) != (FILE *) NULL) {

	fprintf (fd, "%s\n", cfg);
	fclose (fd);

        if (dts->verbose || INIT_DEBUG)
            dtsLog (dts, "Loading config from user string in '%s'....\n", path);
        dts_loadConfigFile (dts, path, dhost, dport, droot, initialize);

        unlink (path);
    }
}


/**
 *  DTS_LOADCONFIGDIR -- Load the DTS configuration files in a directory.
 *
 *  @brief  Load the DTS configuration file.
 *  @fn     void dts_loadConfigDir (DTS *dts, char *cfg, char *dhost, 
 *		int dport, char *droot, int initialize)
 *
 *  @param  dts		DTS struct pointer
 *  @param  cfg		DTS config directory
 *  @param  dhost	DTS daemon host machine
 *  @param  dport	DTS daemon port number
 *  @param  droot	DTS daemon root directory
 *  @param  initialize	initialize DTS?
 *  @return		nothing
 */
void
dts_loadConfigDir (DTS *dts, char *cfg, char *dhost, int dport, char *droot, 
	int initialize)
{
    DIR	   *dp;
    struct  dirent *entry;
    char    path[SZ_LINE];


    if ((dp = opendir (dts->configFile)) == (DIR *) NULL)
        return;
		
    /* Scan through the directory.
     */
    while ( (entry = readdir (dp)) != (struct dirent *) NULL ) {

	/*  Skip dot files, directories and README files.
	 */
	if (entry->d_name[0] == '.')		
	    continue;
	if (strcasecmp (entry->d_name, "README") == 0)
	    continue;
	if (dts_isDir (entry->d_name))
	    continue;

	memset (path, 0, SZ_LINE);
	sprintf (path, "%s/%s", cfg, entry->d_name);

	if (dts->verbose)
	    dtsLog (dts, "Loading config: %s", entry->d_name);
	dts_loadConfigFile(dts, path, dhost, dport, droot, initialize);
    }

    closedir (dp);			/* clean up	*/
}


/**
 *  DTS_LOADCONFIGFILE -- Load the DTS configuration file.
 *
 *  @brief  Load the DTS configuration file.
 *  @fn     void dts_loadConfigFile (DTS *dts, char *cfg, char *dhost, 
 *		int dport, char *droot, int initialize)
 *
 *  @param  dts		DTS struct pointer
 *  @param  cfg		DTS config file
 *  @param  dhost	DTS daemon host machine
 *  @param  dport	DTS daemon port number
 *  @param  droot	DTS daemon root directory
 *  @param  initialize	initialize DTS?
 *  @return		nothing
 */
void
dts_loadConfigFile (DTS *dts, char *cfg, char *dhost, int dport, char *droot, 
	int initialize)
{
    FILE  *fd = (FILE *) NULL;
    static char line[SZ_LINE], dhostIP[SZ_LINE], localhost[SZ_LINE];
    char   str[SZ_LINE], name[SZ_LINE], port[SZ_LINE], cport[SZ_LINE];
    char   hostIP[SZ_LINE], host[SZ_LINE], root[SZ_PATH], network[SZ_LINE];
    char   copyDir[SZ_PATH];
    char  *key, *val, *lp, *hip;
    int    i, lnum = 0, found = 0, valid = 0, nmatch = 0, in_dts = 0;
    int	   dlport = -1, dhport = -1, dcport = -1;

    dtsClient   *client	= (dtsClient *) NULL;
    dtsQueue	*dtsq	= (dtsQueue *) NULL;


    if (DEBUG) {
	fprintf (stderr, "loadConfig: host='%s' port=%d root='%s' init=%d\n",
	    dhost, dport, droot, initialize);
    }
	


    /*  Check for no config file found.  If we use the default, we don't
     *  initialize the cwd or the queues.
     */
    if (!cfg || !cfg[0]) {
	/*
        if (dts->verbose || dts->debug)
            fprintf (stderr, "Warning: No DTS config file found.\n");
	*/
            
	dts_loadDefaultConfig (dts, dhost, dport);
        return;

    } else if ((fd = fopen (cfg, "r")) == (FILE *) NULL) {
        if (dts->verbose || dts->debug)
            fprintf (stderr, "Error opening config file '%s'\n", cfg);
        return;
    }

    if (DEBUG)
	fprintf (stderr, "loadConfig: config file = '%s'\n", dts->configFile);
	

    /*  Read the file.
    */
    gethostname (localhost, SZ_LINE); 		/*  get local machine name */

    while (dtsGets (line, SZ_LINE, fd)) {
	lnum++;
	
	/*  Skip comments and empty lines.
	*/
	for (lp=line; *lp && isspace(*lp); lp++)
	    ;
	if (*lp == '#' || *lp == '\0')
	    continue;

	/*  New contexts or global variables begin at the first column
	*/
	key = dts_cfgKey (line);
	val = dts_cfgVal (line);

	if (strncmp (line, "dts", 3) == 0) {

	    if (context == CON_QUEUE && dtsq && 
		in_dts && dts_validateQueue (dts, dtsq) == OK)
		    dts->queues[dts->nqueues++] = dtsq;

	    if (found)
		/* Already have our entry, now read other clients. */
		found = 0;

	    /* First three lines must be the name, host and port, but we
	    ** allow them in any order.
	    */
	    while (!found) {

		memset (name,    0, SZ_LINE);
		memset (host,    0, SZ_LINE);
		memset (port,    0, SZ_LINE);
		memset (cport,   0, SZ_LINE);
		memset (root,    0, SZ_PATH);
		memset (copyDir, 0, SZ_PATH);

		/*  The first seven lines must be the name/host/port/root,
	 	 *  but we allow them in any order.
		 */
	       	strcpy (name, 	 "default");
	       	strcpy (host, 	 dts_getLocalIP());
	       	strcpy (network, "default");
	       	strcpy (root, 	 "/tmp");
	       	strcpy (copyDir, "/copy");
	       	strcpy (port, 	 DTS_PORT_STR);
	       	strcpy (cport,   DTS_CPORT_STR);
	       	dhport = -1;
	       	dlport = -1;

		for (i=CFG_NREQUIRED; i ; ) {
    		    dtsGets (line, SZ_LINE, fd);
		    key = dts_cfgKey (line);
		    val = dts_cfgVal (line);
		    lnum++;

		    /*  Break on a blank line and skip comments.
		     */
		    if (!key[0] || (key && key[0] == '\n'))
			break;
		    else if (key[0] == '#')
			continue;

		    if (strncasecmp (key, "name", 4) == 0) {
	        	strcpy (name, val);

		    } else if (strncasecmp (key, "host", 4) == 0) {
	        	strcpy (host, val);
	        	if ((hip = dts_resolveHost (val)) == NULL) {
			    fprintf (stderr, "Cannot resolve host '%s'\n", val);
			    exit (1);
			}
	        	strcpy (hostIP, hip);

		    } else if (strncasecmp (key, "port", 4) == 0) {
	        	strcpy (port, val);

		    } else if (strncasecmp (key, "contact", 4) == 0) {
	        	strcpy (cport, val);

		    } else if (strncasecmp (key, "loPort", 6) == 0) {
			dlport = atoi (val);

		    } else if (strncasecmp (key, "hiPort", 6) == 0) {
			dhport = atoi (val);

		    } else if (strncasecmp (key, "root", 4) == 0) {
	        	strcpy (root, val);

	    	    } else if (strncasecmp (key, "network", 4) == 0) {
	       		strcpy (network, val);

	    	    } else if (strncasecmp (key, "copyDir", 4) == 0) {
	       		strcpy (copyDir, val);

		    } else {
			/*  Break on the first optional keyword.
			 */
			if (dts->verbose > 1) {
			    fprintf (stderr, "Config Error line %d: ", lnum);
			    fprintf (stderr, "missing required keywords\n");
			}
			break;
		    }
		    i--;
		}

		/* If no specific port was specified (i.e. dport=0), allow
		** any port to match.
		*/
	        if (dport  == 0) dport   = atoi (port);
	        if (dcport == 0) dcport  = atoi (cport);
	        if (dlport  < 0) dlport  = DEF_XFER_PORT;
	        if (dhport  < 0) dhport  = DEF_XFER_PORT;

	        memset (dhostIP, 0, SZ_LINE);
	        if ((hip = dts_resolveHost (dhost)) == NULL) {
		    fprintf (stderr, "Cannot resolve host '%s'\n", dhost);
		    exit (1);
		}
	        strcpy (dhostIP, hip);
		if (DEBUG || dts->debug) {
		    fprintf (stderr, 
			"loadConfig: %s  hostIP=%s port=%s dport=%d\n", 
			host, hostIP, port, dport);
		    fprintf (stderr, "loadConfig: %s dhostIP=%s found=%d\n", 
			dhost, dhostIP, found);
		}

		/*
		if (atoi(port) == dport && strcasecmp (hostIP, dhostIP) == 0)
		if (atoi(port) == dport && 
		    	(strcasecmp (localhost, host) == 0 || 
			 strcasecmp (hostIP, dhostIP) == 0))
		if (atoi(port) == dport && 
		    (strncasecmp (localhost, host, nmatch) == 0 ||
		     strcasecmp (hostIP, dhostIP) == 0))

		*/
		nmatch = min(((int)strlen(host)), ((int)strlen(localhost)));

		if (atoi(port) == dport && 
		    strncasecmp (localhost, host, nmatch) == 0) {

//fprintf (stderr, "check: port/dport = %d/%d  local/host='%s'/'%s' dhost '%s' IP/dIP='%s'/'%s'\n", atoi(port), dport, localhost, host, dhost, hostIP, dhostIP);

		    /* The DTS configuration IS the one we run on this host
		    ** at the specified port.
		    */
	    	    context = CON_DTS;
		    in_dts = 1;

		    if (DEBUG || dts->debug) {
			fprintf (stderr, "MATCHED host=%s  localhost=%s\n",
			    host, localhost);
		    }

		    strcpy (dts->whoAmI, name);
		    strcpy (dts->serverName, name);

		    /* Allow an app-specific root to override the config.
		    */
		    if (droot && droot[0])
		        strcpy (dts->serverRoot, droot);
		    else
		        strcpy (dts->serverRoot, root);

		    strcpy (dts->serverHost, host);

/*
		    strcpy (dts->serverIP, hostIP);
		    sprintf (dts->serverUrl,"http://%s:%s/RPC2", 
			hostIP, port);
*/
		    strcpy (dts->serverIP, dhostIP);
		    sprintf (dts->serverUrl,"http://%s:%s/RPC2", 
			dts->serverIP, port);

//fprintf (stderr, "match:  serverUrl = '%s'\n", dts->serverUrl);
		    dts->serverPort = dport;
		    dts->contactPort = dport + 1;
		    dts->loPort = dlport;
		    dts->hiPort = dhport;
	            
		    found = 1;
		    valid = 1;


                    /* Save the host info as a client context.
                    */
                    client = calloc (1, sizeof (dtsClient));
                    client->clientPort = atoi (port);
		    if (cport[0])
                        client->clientContact = atoi (cport);
                    client->clientLoPort = dlport;
                    client->clientHiPort = dhport;
                    strcpy (client->clientNet, network);
                    strcpy (client->clientName, name);
                    strcpy (client->clientHost, host);
                    strcpy (client->clientDir, root);
                    strcpy (client->clientCopy, copyDir);
                    strcpy (client->clientIP, hostIP);
                    sprintf (client->clientUrl,"http://%s:%s/RPC2",hostIP,port);
                    for (i=0; i < dts->nclients; i++) {
                        if (strcmp (name, dts->clients[i]->clientName) == 0) {
                            dtsLog (dts,
                                "Warning: DTS name '%s' not unique (line %d)\n",
                                name, lnum++);
                        }
                    }
		    dts->self = client;
                    dts->clients[dts->nclients++] = client;


		    if (DEBUG || dts->debug) {
		        fprintf (stderr, 
			    "dtsServer: %-10s  host='%s' port=%s root='%s'\n",
			    name, host, port, root);
		        fprintf (stderr, "dtsServer: xfer ports = %d to %d\n",
			    dlport, dhport);
		    }
		        
		} else {
		    /* The DTS configuration is NOT the one we run on this
		    ** host.  Save the information for the client connection
		    */
		    client = calloc (1, sizeof (dtsClient));
		    client->clientPort = atoi (port);
		    if (cport[0])
                        client->clientContact = atoi (cport);
                    client->clientLoPort = dlport;
                    client->clientHiPort = dhport;
                    strcpy (client->clientNet, network);
		    strcpy (client->clientName, name);
		    strcpy (client->clientHost, host);
                    strcpy (client->clientDir, root);
                    strcpy (client->clientCopy, copyDir);
		    strcpy (client->clientIP, hostIP);
		    sprintf (client->clientUrl,"http://%s:%s/RPC2", 
			hostIP, port);

		    if (DEBUG || dts->debug) {
			fprintf (stderr, "NOT MATCHED host=%s  localhost=%s\n",
			    host, localhost);
		    }

	    	    context = CON_DTS;
		    in_dts = 0;
		    for (i=0; i < dts->nclients; i++) {
			if (strcmp (name, dts->clients[i]->clientName) == 0) {
			    fprintf (stderr,
				"Warning: DTS name '%s' not unique (line %d)\n",
				name, lnum++);
			}
		    }
		    dts->clients[dts->nclients++] = client;

		    if (DEBUG || dts->debug)
		        fprintf (stderr, 
			    "dtsClient  %-10s  host='%s' port=%s\n",
			    name, host, port);

		    /*  Skip ahead to the next block.
		    */
    		    while (dtsGets (str, SZ_LINE, fd) && lnum++) {
		        if (strncmp (str, "dts", 3) == 0) {
	    	    	    context = CON_DTS;
			    break;
		        }
    		    }

		    /* Check for end of file
		    */
		    if (str[0] == EOF || str[0] == '\0')
			break;
		}
	    }

	} else if (! isspace (line[0])) {
	    if (context == CON_GLOBAL) {
		if (strcasecmp (key, "debug") == 0)
		    dts->debug = dts_cfgBool (line);
		else if (strcasecmp (key, "verbose") == 0)
		    dts->verbose = dts_cfgBool (line);
		else if (strcasecmp (key, "monitor") == 0)
		    strcpy (dts->monitor, val);
		else if (strcasecmp (key, "semId") == 0)
		    dts_semInitId (atoi(val));
	        else if (strcasecmp (key, "hb_time") == 0)
	            dts->stat_period = atoi (val);
	        else if (strcasecmp (key, "password") == 0)
	            strcpy (dts->passwd, val);
	        else if (strcasecmp (key, "passwd") == 0)
	            strcpy (dts->passwd, val);
	        else if (strcasecmp (key, "ops_passwd") == 0)
	            strcpy (dts->ops_pass, val);
	    }
	    context = CON_GLOBAL;
	}


	/* Continue processing.
	*/
	key = dts_cfgKey (line);
	val = dts_cfgVal (line);

	if (context == CON_DTS) {

	    if (strcasecmp (key, "logfile") == 0) {
	        strcpy (dts->logFile, val);

	    } else if (strcasecmp (key, "dbfile") == 0) {
	        strcpy (dts->dbFile, val);

	    } else if (strcasecmp (key, "password") == 0) {
	        strcpy (dts->passwd, val);

	    } else if (strcasecmp (key, "passwd") == 0) {
	        strcpy (dts->passwd, val);

	    } else if (strcasecmp (key, "ops_passwd") == 0) {
	        strcpy (dts->ops_pass, val);

	    } else if (strncasecmp (key, "contact", 7) == 0) {
		strcpy (cport, val);
		dts->contactPort = atoi (val);

	    } else if (strcasecmp (key, "queue") == 0) {
	        context = CON_QUEUE;
	        dtsq = dts_newQueue (dts);
	    }

	} else if (strcasecmp(key, "queue") == 0) {

	    if (DEBUG || dts->debug)
	        fprintf (stderr, "dts queue\n");

	    if (context == CON_QUEUE && found) {
	        /*  Close out existing queue before opening new one.  First be
		 *  sure the queue we have is valid.
	         */
		if (!dtsq->deliveryDir) {
		    fprintf (stderr, 
			"Error: no deliveryDir specified for queue '%s'\n", 
			dtsq->name);
		    exit (1);
		}
	        if (dtsq && dts_validateQueue (dts, dtsq) == OK)
		    dts->queues[dts->nqueues++] = dtsq;
	    }
	    context = CON_QUEUE;

	    /*  Allocate a new queue structure.
	    */
	    dtsq = dts_newQueue (dts);

	} else if (context == CON_QUEUE) {

	    if (!val || !val[0])		/* no value specified	*/
		val = "";

	    if (strcasecmp (key, "name") == 0) {
	        if (dts->nqueues > 0) {
	            for (i=0; i < dts->nqueues; i++)
	                if (strcmp (val, dts->queues[i]->name) == 0) {
		    	    fprintf (stderr, 
				"Error: Duplicate queue name '%s'\n", val);
		    	    exit (1);
			}
	        }
		memset (dtsq->name, 0, SZ_FNAME);
	        strcpy (dtsq->name, val);
	    } else if (strcasecmp (key, "type") == 0) {
	        dtsq->type = dts_cfgQType (val);
	    } else if (strcasecmp (key, "node") == 0) {
	        dtsq->node = dts_cfgQNode (val);
	    } else if (strcasecmp (key, "src") == 0) {
	        strcpy (dtsq->src, val);
	    } else if (strcasecmp (key, "dest") == 0) {
	        strcpy (dtsq->dest, val);
	    } else if (strcasecmp (key, "purge") == 0) {
		dtsq->auto_purge = dts_cfgBool (val);
	    } else if (strcasecmp (key, "monitor") == 0) {
		strcpy (dts->monitor, val);

	    } else if (strcasecmp (key, "deliveryDir") == 0) {
		char emsg[SZ_LINE];

	        strcpy (dtsq->deliveryDir, val);
		if (strcmp (val, "spool") != 0) {
		    if (dts_testDeliveryDir (val, 1, emsg) == ERR) {
	    	        fprintf (stderr, "Error: %s\n", emsg);
		        exit (1);
		    }
		}
	    } else if (strcasecmp (key, "checksumPolicy") == 0) {
		dtsq->checksumPolicy = CS_NONE;
		if (strncasecmp (val, "chunk", 7) == 0)
		    dtsq->checksumPolicy = CS_CHUNK;
		else if (strncasecmp (val, "packet", 6) == 0)
		    dtsq->checksumPolicy = CS_PACKET;
		else if (strncasecmp (val, "stripe", 6) == 0)
		    dtsq->checksumPolicy = CS_STRIPE;

	    } else if (strcasecmp (key, "deliveryPolicy") == 0) {
		if (strncasecmp (val, "replace", 7) == 0)
		    dtsq->deliveryPolicy = QUEUE_REPLACE;
		else if (strncasecmp (val, "number", 6) == 0)
		    dtsq->deliveryPolicy = QUEUE_NUMBER;
		else if (strncasecmp (val, "original", 6) == 0)
		    dtsq->deliveryPolicy = QUEUE_ORIGINAL;
		else {
	    	    fprintf (stderr, 
			"Error: Invalid deliveryPolicy '%s' for queue '%s'\n",
			val, dtsq->name);
		    exit (1);
		}
	    } else if (strcasecmp (key, "deliverAs") == 0) {
	        strcpy (dtsq->deliverAs, val);
	    } else if (strcasecmp (key, "deliveryCmd") == 0) {
	        strcpy (dtsq->deliveryCmd, val);

	    } else if (strcasecmp (key, "transfer") == 0) {
	        strcpy (dtsq->transfer, val);
	    } else if (strcasecmp (key, "method") == 0) {
	        dtsq->method = dts_cfgQMethod (val);
	    } else if (strcasecmp (key, "mode") == 0) {
	        dtsq->mode = dts_cfgQMode (val);
	    } else if (strcasecmp (key, "nthreads") == 0) {
	        dtsq->nthreads = dts_cfgInt (line);
	    } else if (strcasecmp (key, "port") == 0) {
		if (!val[0] || strcasecmp (val, "auto") == 0)
	            dtsq->port = dts_getQPort (dts,dtsq->name, -1);
		else
	            dtsq->port = dts_getQPort (dts,dtsq->name,dts_cfgInt(line));
	    } else if (strcasecmp (key, "keepalive") == 0) {
	        dtsq->keepalive = dts_cfgInt (line);
	    } else if (strcasecmp (key, "udt_rate") == 0) {
	        dtsq->udt_rate = dts_cfgInt (line);
	    } else if (strcasecmp (key, "interval") == 0) {
	        dtsq->interval = dts_cfgInterval (val);
	    } else if (strcasecmp (key, "start_time") == 0) {
	        dtsq->stime = dts_cfgStartTime (val);
	    } else {
		fprintf (stderr, "Error: unknown keyword '%s' = '%s'\n",
		    key, (val[0] ? val : ""));
		continue;
	    }
	}
    }


    /*  If we get here then we found a config file, but no entry for
     *  the local host.  This is a fatal error.
    if (dport != DTSQ_PORT && 
	dport != DTSSH_PORT && 
	dport != DTSMON_PORT && 
	!valid) {
            fprintf (stderr, "Error: No local config entry for '%s' in '%s'\n", 
	        localhost, dts->configFile);
            exit (1);
    }
     */

    /* Save the last queue we found.
    */
    if (!dtsq->deliveryDir) {
        fprintf (stderr, "Error: no deliveryDir specified for queue '%s'\n", 
    	    dtsq->name);
        exit (1);
    }
    if (dtsq && in_dts && dts_validateQueue (dts, dtsq) == OK)
	dts->queues[dts->nqueues++] = dtsq;


    /* If no entry found, use some reasonable defaults.
    */
    if (!dts->serverRoot) {
	if (DEBUG || dts->debug || dts->verbose) 
	    fprintf (stderr, 
		"Warning: No entry in config file, using defaults\n");
	    
	strcpy (dts->whoAmI, "generic");
	strcpy (dts->serverName, "generic");
        if (dport == DTSQ_PORT || dport == DTSSH_PORT || dport == DTSMON_PORT)
	    strcpy (dts->serverRoot, "");
        else
	    strcpy (dts->serverRoot, "/tmp/dts");
	strcpy (dts->serverHost, "localhost");
	strcpy (dts->serverIP, "127.0.0.1");
//fprintf (stderr, "NO match:  serverUrl IP = '%s'\n", dts->serverIP);
	sprintf (dts->serverUrl,"http://127.0.0.1:%d/RPC2", dport);
	dts->serverPort = dport;
	dts->contactPort = dport + 1;
	dts->loPort = DEF_XFER_PORT;
	dts->hiPort = DEF_XFER_PORT + 1024;
    }

    /* Get the number of unique network names.
     */
    dts->nnetworks = dts_getNumNetworks (dts);


    /* Now see if the server root directory exists.  If so, initialize
    ** it (i.e. wipe it clean), otherwise simply create it.
    */
    if (!dts->serverRoot[0] || strcmp (dts->serverRoot, "./") == 0) {
	;

    } else if (access (dts->serverRoot, R_OK|W_OK|X_OK) == 0) {
	/* Make sure we're not in a DTS source directory....
	*/
	if (initialize) {
	    /* FIXME -- sloppy .... */
	    char  cmd[SZ_LINE], p1[SZ_PATH], p2[SZ_PATH];

	    memset (p1, 0, SZ_PATH); memset (p2, 0, SZ_PATH);
	    sprintf (p1, "%s/dts.c", dts->serverRoot);
	    sprintf (p2, "%s/dtsd.c", dts->serverRoot);

	    if ((access (p1,F_OK) == 0 || access (p2,F_OK) == 0)) {
		fprintf (stderr, 
		    "ERROR: Attempt to root init in DTS source dir!!!\n");
		exit (1);
	    }

	    memset (cmd, 0, SZ_LINE);
	    sprintf (cmd, "(chdir %s ; /bin/rm -rf *)", dts->serverRoot);
		/*  FIXME -- Comment out until end of development....
	        system (cmd);
		*/
	}

    } else {
	if (mkdir (dts->serverRoot, DTS_DIR_MODE) < 0) {
	    fprintf (stderr, "Error: cannot create root directory '%s'\n",
		dts->serverRoot);
	    exit (1);
	}
    }

    if (DEBUG || dts->debug) {
	fprintf (stderr, 
	    "loadConfig: name='%s' root='%s' host=%s port=%d\n",
	    dts->whoAmI, dts->serverRoot, dts->serverHost, dts->serverPort);
	dts_printConfig (dts);
	dts_printQPorts (dts);
    }
	

    /*  Clean up.  Allocated queue and client structs are freed when
    **  we shut down entirely.
    */
    if (fd)
        fclose (fd);
}


/**
 *  DTS_LOADDEFAULTCONFIG -- Duh
 */
int
dts_loadDefaultConfig (DTS *dts, char *host, int port)
{
    char localhost[SZ_LINE], name[SZ_LINE];
    char  *cwd, *lp, buf[SZ_PATH], ip[SZ_PATH];


    memset (ip, 0, SZ_PATH);
    memset (name, 0, SZ_LINE);
    memset (localhost, 0, SZ_LINE);

    cwd = getcwd (buf, (size_t) SZ_PATH);
    gethostname (localhost, SZ_LINE);           /*  get local machine name */
    strncpy (name, localhost, SZ_LINE);

    for (lp=name; *lp; lp++) {
	if (*lp == '.') {
	    *lp = '\0';
	    break;
	}
    }

    strcpy (ip, dts_resolveHost (localhost));
//fprintf (stderr, "loadDefaultConfig:  ip = '%s'\n", ip);


    /*   Set the default config parameters.
    */
    strcpy (dts->whoAmI, name);
    strcpy (dts->passwd, DEF_PASSWD);

    strcpy (dts->serverName, localhost);	    /* local dts node name */
    strcpy (dts->serverRoot, cwd);		    /* sandbox directory   */
    strcpy (dts->serverIP,   ip);		    /* local IP address    */
    dts->serverPort = port;
    dts->contactPort = 0;

    if (host && *host)   			    /* local host name     */
        strcpy (dts->serverHost, host);
    else
        strcpy (dts->serverHost, dts_getLocalHost());

    sprintf (dts->serverUrl, "http://%s:%d/RPC2",   /* server URL          */
	dts->serverIP, port);

    strcpy (dts->configFile, "");
    strcpy (dts->logFile, "/tmp/dts.log");
    strcpy (dts->dbFile, "/tmp/dts.db");


    return (OK);
}


/**
 *  DTS_NEWQUEUE -- Get a new DTS queue structure and fill in the defaults.
 */
dtsQueue *
dts_newQueue (DTS *dts)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;


    /*  Allocate a new queue structure and fill in the defaults.
    */
    dtsq = calloc (1, sizeof (dtsQueue));

    dtsq->dts             = dts;
    dtsq->type            = QUEUE_NORMAL;
    dtsq->node            = QUEUE_INGEST;
    dtsq->mode            = QUEUE_PUSH;
    dtsq->method          = TM_INLINE;
    dtsq->port            = -1;			/* force value to be found    */
    dtsq->nthreads        = 4;
    dtsq->keepalive       = 0;
    dtsq->deliveryPolicy  = QUEUE_REPLACE;

    /*  Initialize the queue semaphores.  These are actually created in the
     *  calling process when a queue in started.
     */
    dtsq->activeSem	  = 0;
    dtsq->countSem	  = 0;

    /*  Initialize the queue mutex.
     */
    pthread_mutex_init (&dtsq->mutex, NULL);

    sprintf (dtsq->name, "q%02d", dts->nqueues);   /* give a default name     */
    sprintf (dtsq->src,  "%s", dts_getLocalHost());

    sprintf (dtsq->infile,  "%s", "none");
    sprintf (dtsq->outfile, "%s", "none");

    /*  Undefined: 
     *	    dtsq->dest
     *	    dtsq->deliveryDir
     *	    dtsq->deliveryApp
     *	    dtsq->transfer
     */

    return (dtsq);
}


/**
 *  DTS_VALIDATEQUEUE -- Validate the queue structure.
 */
int
dts_validateQueue (DTS *dts, dtsQueue *dtsq)
{
    int status = OK;
    char  *name = (char *) NULL;


    /*  Sanity checks.
     */
    if (! dts) {
	fprintf (stderr, "Error: dts_validateQueue gets NULL 'dts'\n");
	exit (1);
    }
    if (! dtsq) {
	fprintf (stderr, "Error: dts_validateQueue gets NULL 'dtsq'\n");
	exit (1);
    }


    /*  Make sure we have a delivery directory.
     */
    if (! dtsq->deliveryDir[0] && dtsq->node == QUEUE_ENDPOINT ) {
	fprintf (stderr, "ERROR on queue %s: No delivery dir specified\n",
	    dtsq->name);
	status = ERR;
    }

    /*  Ensure we're running at least one thread. 
     */
    if (dtsq->nthreads < 1) {
	fprintf (stderr, "WARN on queue %s: Must have at least one thread\n",
	    dtsq->name);
	dtsq->nthreads = 1;
    }

    /*  Make sure we have a transfer port.
     */
    if (dtsq->port < 0)
	dtsq->port = dts_getQPort (dts, dtsq->name, -1);
	            
    /*  Require a name, it's an error if we have the placeholder given
     *  at the init.
     */
    name = dtsq->name;
    if (name[0] == 'q' && isdigit(name[1]) && isdigit(name[2])) {
	fprintf (stderr, "ERROR on queue %s: name is required\n", dtsq->name);
	status = ERR;
    }

    return (status);
}


/**
 *  DTS_GETQPORT -- Get the next available base port for a transfer queue.
 *  If the input port number is already in use we'll search for a new port
 *  to use, otherwise we return the input port (i.e. validate it).
 */
int
dts_getQPort (DTS *dts, char *qname, int tport)
{
    int   i, lo, hi, ret_val = -1;
    dtsQueue *dtsq = (dtsQueue *) NULL;


    lo = dts->loPort;
    ret_val = lo;
    if (tport < 0) {
        if (dts->nqueues == 0) {
    	    ret_val = lo;		/* first queue		*/

	} else {
            for (i=0; i < dts->nqueues && i < MAX_QUEUES; i++) {
	        dtsq = dts->queues[i];
	        hi = lo + dtsq->nthreads - 1;

	        if (!dtsq->name || strcmp (qname, dtsq->name) == 0) {
	            dtsq->port = lo;
		    ret_val = lo;
	        } else
		    lo = hi + 1;

	        if (hi > dts->hiPort) {
		    dtsErrLog (NULL, 
			"Warning: Reset DTS hiPort %d\n", dts->hiPort);
	            dts->hiPort = hi;
	        }
            }
	    ret_val = lo;
	}

    } else {
	ret_val = tport;
    }

    return (ret_val);
}



/**
 *  DTS_GETQPORT -- Get the next available base port for a transfer queue.
 *  If the input port number is already in use we'll search for a new port
 *  to use, otherwise we return the input port (i.e. validate it).
 */
void
dts_printQPorts (DTS *dts)
{
    int   i;
    dtsQueue *dtsq = (dtsQueue *) NULL;


    for (i=0; i < dts->nqueues && i < MAX_QUEUES; i++) {
	dtsq = dts->queues[i];

	fprintf (stderr, "queue = '%16.16s'  lo = %5d  hi = %5d\n",
	    dtsq->name, dtsq->port, (dtsq->port + dtsq->nthreads - 1));
    }
}



/******************************************************************************
**  Utility procedures.
******************************************************************************/

/**
 *  DTS_VALIDPASSWD -- Validate a passwd against the host.
 *
 *  @brief  Validate a passwd against the host.
 *  @fn     int dts_validPasswd (DTS *dts, char *passwd)
 *
 *  @param  dts		DTS struct pointer
 *  @param  passwd	passwd to be checked
 *  @return		nothing
 */
int
dts_validPasswd (DTS *dts, char *passwd)
{
    /* Not yet implemented as the most secure form of password ...
    */
    if (!passwd)
	return (ERR);
    if (strcmp (passwd, DEF_PASSWD) == 0)	/* compile-time passwd	*/
	return (OK);

    if (*passwd && dts->passwd[0])
	return ( (strcmp (passwd, dts->passwd) == 0) ? OK : ERR );
    else if (*passwd && dts->ops_pass[0])
	return ( (strcmp (passwd, dts->ops_pass) == 0) ? OK : ERR );
    else
        return (ERR);
}


/**
 *  DTS_NAMETOHOST -- Convert a DTS name to the host.
 *
 *  @brief  Convert a DTS name to the host.
 *  @fn     char *dts_nameToHost (DTS *dts, char *name)
 *
 *  @param  dts		DTS struct pointer
 *  @param  name	DTS name string
 *  @return		host name
 */
char *
dts_nameToHost (DTS *dts, char *name)
{
    register int i;


    if (dts == (DTS *) NULL ||  dts->serverName[0] == '\0' || name == NULL)
	return (name);

    if (strcmp (dts->serverName, name) == 0)
	return (dts->serverHost);

    else {
	for (i=0; i < dts->nclients; i++)
	    if (strcmp (dts->clients[i]->clientName, name) == 0)
		return (dts->clients[i]->clientHost);
    }

    /* Name wasn't one of the DTS names, assume it is a host name and
    ** give it back to the caller.
    */
    return ((char *) name);
}


/**
 *  DTS_NAMETOIP -- Convert a DTS name to the host IP.
 *
 *  @brief  Convert a DTS name to the host IP.
 *  @fn     char *dts_nameToIP (DTS *dts, char *name)
 *
 *  @param  dts		DTS struct pointer
 *  @param  name	DTS name string
 *  @return		IP string
 */
char *
dts_nameToIP (DTS *dts, char *name)
{
    register int i;
    char   *hp;


    if (dts == (DTS *) NULL ||  dts->serverName[0] == '\0' || name == NULL) {
	if (DEBUG || dts->debug || dts->verbose)
	    fprintf (stderr, "nametoIP:  NULL server name, using localhost\n");
	return ("127.0.0.1");
    }

    if (strcmp (dts->serverName, name) == 0)
	return (dts->serverIP);

    else {
	for (i=0; i < dts->nclients; i++)
	    if (strcmp (dts->clients[i]->clientName, name) == 0)
		return (dts->clients[i]->clientIP);
    }

    hp = dts_resolveHost (name);

    if (!hp && ((DEBUG || dts->debug || dts->verbose)))
        fprintf (stderr, "nametoIP:  NULL server name, using localhost\n");

    return ( hp ? hp : "127.0.0.1" );
}


/**
 *  DTS_CFGNAMETOHOST -- Convert a DTS name to the host name (2nd method).
 *
 *  @brief  Convert a DTS name to the host name (2nd method).
 *  @fn     char *dts_cfgNameToHost (DTS *dts, char *name)
 *
 *  @param  dts		DTS struct
 *  @param  name	DTS name string
 *  @return		host name
 */
char *
dts_cfgNameToHost (DTS *dts, char *name)
{
    register int i;

    for (i=0; i < dts->nclients; i++) {
	if (strcmp (dts->clients[i]->clientName, name) == 0)
	    return (dts->clients[i]->clientHost);
    }

    if (DEBUG || dts->debug || dts->verbose)
        fprintf (stderr, "cfgNameToHost:  NULL server name, using localhost\n");

    return ("127.0.0.1");
}


/**
 *  DTS_CFGNAMETOIP -- Convert a DTS name to the host IP (2nd method).
 *
 *  @brief  Convert a DTS name to the host IP (2nd method).
 *  @fn     char *dts_cfgNameToIP (DTS *dts, char *name)
 *
 *  @param  dts		DTS struct
 *  @param  name	DTS name string
 *  @return		IP name
 */
char *
dts_cfgNameToIP (DTS *dts, char *name)
{
    register int i;

    for (i=0; i < dts->nclients; i++) {
	if (strcmp (dts->clients[i]->clientName, name) == 0)
	    return (dts->clients[i]->clientIP);
    }

    if (DEBUG || dts->debug || dts->verbose)
        fprintf (stderr, "cfgNameToIP:  NULL server name, using localhost\n");

    return ("127.0.0.1");
}


/**
 *   Utility routine to get values for the local DTS host.
 */
char *
dts_getLocalRoot ()
{ 
/*
    static char localRoot[SZ_FNAME];

    memset (localRoot, 0, SZ_FNAME);
    strncpy (localRoot, dts->serverRoot, strlen (dts->serverRoot));

    return (localRoot);
*/
    extern DTS *dts;

    return (dts->serverRoot);
}


/**
 *  DTS_CFGQTYPE -- Get the type of queue.
 *
 *  @brief  Get the type of queue.
 *  @fn     int dts_cfgQType (char *s)
 *
 *  @param  s		string name of queue
 *  @return		queue type
 */
int
dts_cfgQType (char *s)
{
    if (strcasecmp (s,"normal")    == 0) return (QUEUE_NORMAL);
    if (strcasecmp (s,"scheduled") == 0) return (QUEUE_SCHEDULED);
    if (strcasecmp (s,"priority")  == 0) return (QUEUE_PRIORITY);

    return (-1);
}


/**
 *  DTS_CFGQNODE -- Get the node type of queue.
 *
 *  @brief  Get the node type of queue.
 *  @fn     int dts_cfgQNode (char *s)
 *
 *  @param  s		string name of queue
 *  @return		queue type
 */
int
dts_cfgQNode (char *s)
{
    if (strcasecmp (s,"ingest")   == 0) return (QUEUE_INGEST);
    if (strcasecmp (s,"transfer") == 0) return (QUEUE_TRANSFER);
    if (strcasecmp (s,"endpoint") == 0) return (QUEUE_ENDPOINT);

    return (-1);
}


/**
 *  DTS_CFGQMETHOD -- Get the queue transfer method.
 *
 *  @brief  Get the queue transfer method.
 *  @fn     int dts_cfgQMethod (char *s)
 *
 *  @param  s		string method name
 *  @return		method code
 */
int
dts_cfgQMethod (char *s)
{
    if (strcasecmp (s, "dts") == 0)     return (TM_PSOCK); /* default method */

    if (strcasecmp (s, "psock") == 0)   return (TM_PSOCK);
    if (strcasecmp (s, "user") == 0)    return (TM_USER);
    if (strcasecmp (s, "inline") == 0)  return (TM_INLINE);
    if (strcasecmp (s, "udt") == 0)     return (TM_UDT);
    if (strcasecmp (s, "scp") == 0)     return (TM_SCP);

    return (-1);
}


/**
 *  DTS_CFGQMODE -- Get the trander mode (push or pull).
 *
 *  @brief  Get the trander mode (push or pull).
 *  @fn     int dts_cfgQMode (char *s)
 *
 *  @param  s		string mode name
 *  @return		nothing
 */
int
dts_cfgQMode (char *s)
{
    return (strcasecmp(s,"push") == 0 ? QUEUE_PUSH : QUEUE_GIVE);
}


/**
 *  DTS_CFGQTYPESTR -- Convert mode to string name
 *
 *  @brief  Convert mode to string name
 *  @fn     char *dts_cfgQTypeStr (int type)
 *
 *  @param  type	queue type code
 *  @return		nothing
 */
char *
dts_cfgQTypeStr (int type)
{
    return (type == QUEUE_NORMAL    ? "normal" : 
	   (type == QUEUE_SCHEDULED ? "scheduled" : 
	   (type == QUEUE_PRIORITY  ?  "priority" : "unknown" )));
}


/**
 *  DTS_CFGQNODESTR -- Convert node to type string name
 *
 *  @brief  Convert dode to type string name
 *  @fn     char *dts_cfgQNodeStr (int node)
 *
 *  @param  node	queue node code
 *  @return		nothing
 */
char *
dts_cfgQNodeStr (int node)
{
    return (node == QUEUE_INGEST   ? "ingest" : 
	   (node == QUEUE_TRANSFER ? "transfer" : 
	   (node == QUEUE_ENDPOINT ? "endpoint" : "unknown" )));
}


/**
 *  DTS_CFGQMETHODSTR -- Convert method type to string name.
 *
 *  @brief  Convert method type to string name.
 *  @fn     char *dts_cfgQMethodStr (int method)
 *
 *  @param  method	method type code
 *  @return		nothing
 */
char *
dts_cfgQMethodStr (int method)
{
    switch (method) {
    case TM_USER:	return ("user");
    case TM_INLINE:	return ("inline");
    case TM_PSOCK:	return ("psock");
    case TM_SCP:	return ("scp");
    case TM_UDT:	return ("udt");
    default: 		return ("");
    }
}


/**
 *  DTS_CFGMODESTR -- Convert mode type to string name
 *
 *  @brief  Convert mode type to string name
 *  @fn     char *dts_cfgQModeStr (int mode)
 *
 *  @param  mode	mode type code
 *  @return		nothing
 */
char *
dts_cfgQModeStr (int mode)
{
    return (mode == QUEUE_PUSH ? "push" : "give");
}


/**
 *  DTS_CFGINTERVAL -- Get a timing interval (in sec) from the config value.
 *  Default unit is minutes, intervals may be specified in the form
 *
 *	<N>[h|m|s]	run every <N> (h)ours, (m)in, or (s)ec
 *
 *  @brief  Get a timing interval (in sec) from the config value.
 *  @fn     char *dts_cfgInterval (char *intstr)
 *
 *  @param  intstr	interval value (string type)
 *  @return		interval in seconds
 */
time_t
dts_cfgInterval (char *intstr)
{
    char *ip = intstr;
    int  units = 60;					/* default to minutes */

    for (ip=intstr; *ip && isdigit (*ip); ip++)	 	/* skip int value     */
	;

    if (*ip) {
	if ( *ip == 'h' || *ip == 'H' )
	    units = units * 60;
	else if ( *ip == 's' || *ip == 'S' )
	    units = units / 60;
	*ip = '\0';
    }

    return ( (time_t) (atoi(intstr) * units) );		/* time in seconds    */
}



/**
 *  DTS_CFGSTARTTIME -- Convert a start time to equivalent time_t seconds.
 *
 *  @brief  Get a timing interval (in sec) from the config value.
 *  @fn     char *dts_cfgStartTime (char *tstr)
 *
 *  @param  tstr	start time value (string type)
 *  @return		interval in seconds
 */
time_t
dts_cfgStartTime (char *tstr)
{
    return ( (time_t) 0 ); 	/* Not yet implemented */
}



/******************************************************************************
**  Private Methods
******************************************************************************/

/**
 *  DTS_CFGKEY -- Get the keyword from the config line.  Ignore leading
 *  whitespace, the context is derived in the caller.  We also ignore any
 *  inline comments and return only the first word on the line.
 */
static char *
dts_cfgKey (char *line)
{
    static char key[SZ_FNAME];
    char   *ip = line, *op = key;


    memset (key, 0, SZ_FNAME);
    for (ip=line; *ip && isspace (*ip); ip++)	/* skip leading whitespace */
	;

    while ( *ip && !isspace(*ip) )
	*op++ = *ip++;

    return (key);
}


/**
 *  DTS_CFGVAL -- Get the value from the config line.  Ignore leading
 *  whitespace, the context is derived in the caller.  We also ignore any
 *  inline comments and return only the first word on the line.
 */
static char *
dts_cfgVal (char *line)
{
    static char val[SZ_LINE];
    char   *ip = line, *op = val;

    memset (val, 0, SZ_LINE);

    for (ip=line; *ip && isspace(*ip); ip++) ;	/* skip leading w/s	  */

    while (*ip && !isspace(*ip++))  ;		/* skip keyword		  */

    if (! *ip || *ip == '\n')
	return (val);

    while (*ip && isspace(*ip++))   ;  		/* skip intermediate w/s  */ 

    if (*ip == '\n' || *ip == '#') 		/* no value given	  */
	return (NULL);

    for (ip--; *ip && *ip != '\n' && *ip != '#'; ) /* collect value	  */
	*op++ = *ip++;

    for (op--; *op == '#' || isspace (*op); op--)  /* trim trailing w/s   */ 
	*op = '\0';

    return (val);
}


/**
 *  DTS_CFGBOOL -- Get a 'boolean' value from a string.  Values may be 
 *  the integers 0/1, case-insensitive strings yes/no, or true/false.
 */
static int
dts_cfgBool (char *line)
{
    char  *s = dts_cfgVal (line);

    return ( (strchr("1yYtT", (int)*s) ? 1 : 0) );
}


/**
 *  DTS_CFGINT -- Get an integer value from a value field.
 */
static int
dts_cfgInt (char *line)
{
    return (atoi(dts_cfgVal(line)));
}


/**
 *  DTS_CFGPATH -- Construct a path to a user's ".dts_config" file.
 */
char *
dts_cfgPath ()
{
    static char path[SZ_PATH];
    char *home = NULL;

    memset (path, 0, SZ_PATH);
    if ((home = getenv ("HOME")))
        strcpy (path, home);
    strcat (path, "/.dts_config");

    return (path);
}


/******************************************************************************/


/**
 *  DTS_PRINTCONFIG -- Print Config file info.
 */
void
dts_printConfig (DTS *dts)
{
    printf ("\n");
    printf ("DTS Config:    dts = 0x%x\n\n", (unsigned int) dts);

    printf ("      whoAmI:  %s\n", dts->whoAmI);
    printf ("  serverName:  %s\n", dts->serverName);
    printf ("  serverRoot:  %s\n", dts->serverRoot);
    printf ("   serverUrl:  %s\n", dts->serverUrl);
    printf ("    serverIP:  %s\n", dts->serverIP);
    printf ("  serverHost:  %s\n", dts->serverHost);
    printf ("  serverPort:  %d\n", dts->serverPort);
    printf (" contactPort:  %d\n", dts->contactPort);
    printf ("\n");
    printf ("  configFile:  %s\n", dts->configFile);
    printf ("     logFile:  %s\n", dts->logFile);
    printf ("\n");
    printf ("    nclients:  %d\n", dts->nclients);
    printf ("     nqueues:  %d\n", dts->nqueues);
    printf ("    mon_port:  %d\n", dts->mon_port);
    printf ("       semId:  %d\n", dts->semId);
    printf ("     verbose:  %d\n", dts->verbose);
    printf ("       debug:  %d\n", dts->debug);
    printf ("       trace:  %d\n", dts->trace);
    printf ("\n");
}


/**
 *  DTS_FMTCONFIG -- Print Config file info as a formatted string.
 */
char *
dts_fmtConfig (DTS *dts)
{
    static char buf[2048];

    memset (buf, 0, 2048);
    sprintf (buf,
    "DTS Config:\n\n\
          whoAmI:  %s\n\
      serverName:  %s\n\
      serverRoot:  %s\n\
       serverUrl:  %s\n\
        serverIP:  %s\n\
      serverHost:  %s\n\
      serverPort:  %d\n\
     contactPort:  %d\n\
      configFile:  %s\n\
         logFile:  %s\n\
        nclients:  %d\n\
         nqueues:  %d\n\
        mon_port:  %d\n\
           semId:  %d\n\
         verbose:  %d\n\
           debug:  %d\n\
           trace:  %d\n\n",

		dts->whoAmI,
		dts->serverName,
		dts->serverRoot,
		dts->serverUrl,
		dts->serverIP,
		dts->serverHost,
		dts->serverPort,
		dts->contactPort,
		dts->configFile,
		dts->logFile,
		dts->nclients,
		dts->nqueues,
		dts->mon_port,
		dts->semId,
		dts->verbose,
		dts->debug,
		dts->trace);

    return ( buf );
}
