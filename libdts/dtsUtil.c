/**
 *  DTSUTIL.C -- Utility routines for the DTS.
 *
 *
 *	dtsDaemonize (DTS *dts)
 *	dts_sigHandler (int sig)
 *	dts_pause()
 *	dts_getLocalIP ()
 *	dts_getLocalHost ()
 *	dts_resolveHost (char *hostname)
 *	dts_fmtMode (char *lp, int flags)
 *	dts_patMatch (char *str, char *pattern)
 *	dts_isTemplate (char *s)
 *	dts_strbuf (char *s);
 *
 *	dtsGets (char *s, int len, FILE *fp)
 *	dtsError (char *msg)
 *	dts_debugLevel ()
 *	dts_printDTS (DTS *dts, FILE *fd)
 *	dts_printDTSClient (DTS *dts, FILE *fd)
 *	dts_printAllQueues (DTS *dts, FILE *fd)
 *	dts_printQueue (dtsQueue *dtsq, FILE *fd)
 *
 *	measure_start (void)
 *	measure_stop (long transferred)
 *
 *	base64_encode (uchar *source, size_t sourcelen, char *target,
 *		size_t targetlen)
 *	base64_decode (char *source, unsigned char *target, size_t targetlen)
 *
 *
 *  @brief	DTS Utility methods
 *
 *  @file  	dtsUtil.c
 *  @author  	Mike Fitzpatrick, NOAO
 *  @date	6/15/09
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <netdb.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <signal.h>
#include <fcntl.h>

#include "dts.h"


extern DTS *dts;


    
/****************************************************************************
**  Routines to make the task a daemon and handle child signals.
*/

/**
 *  DTSDAEMONIZE -- Daemonize the DTS.
 *
 *  @fn		dtsDaemonize (DTS *dts)
 *
 *  @param  dts		DTS struct pointer
 *  @return
 */
void
dtsDaemonize (DTS *dts)
{
    int    i, lfp;
    pid_t  pid;
    char   str[10];


    if (getppid() == 1)
	return;				/* we're already a daemon 	*/

    if ((pid = fork ()) < 0) {		/* fork error 			*/
	fprintf (stderr, "Error forking daemon process.\n");
	exit (1);

    } else if (pid > 0)
	exit (0);			/* parent exits 		*/


    /*  Child (daemon) continues ...
    */
    setsid ();				/* obtain a new process group 	*/
    for (i = getdtablesize(); i >= 0; --i)
	close (i);			/* close all descriptors	*/


    /*  If we're running as a daemon, connect the stdin/stdout/stderr to
    **  /dev/null so we don't produce any output.
    */
    i = open ("/dev/null", O_RDWR);
    dup (i);
    dup (i);


    /*  Set the umask for files we create and change to the working
    **  directory for operation.
    */
    umask (0002);			/* set new-file umask 		*/
    chdir (dts->workingDir);		/* change running directory 	*/


    /*  Create a lock-file so only one of us is running at a time.
    **  We've redirected standard i/o so can't output problem messages.
    */
    if ((lfp = open (DTS_LOCKFILE, O_RDWR | O_CREAT, 0640)) < 0)
	exit (1);			/* cannot open 			*/
    if (lockf (lfp, F_TLOCK, 0) < 0)
	exit (0);			/* cannot lock 			*/

    /* first instance continues */
    sprintf (str, "%d\n", getpid ());
    write (lfp, str, strlen (str));	/* record pid to lockfile 	*/


    /* Establish signal handlers.
    */
    /*signal (SIGCHLD, SIG_IGN); */	/* ignore child 		*/
    signal (SIGTSTP, SIG_IGN);		/* ignore tty signals 		*/
    signal (SIGTTOU, SIG_IGN);
    signal (SIGTTIN, SIG_IGN);
    signal (SIGHUP,  SIG_IGN);
    signal (SIGTERM, SIG_IGN);

#ifdef USE_SIGHANDLERS
    signal (SIGHUP,  dts_sigHandler);	/* catch hangup signal 		*/
    signal (SIGTERM, dts_sigHandler);	/* catch kill signal 		*/
#endif
    signal (SIGUSR1, dts_sigHandler);	/* clean shutdown request	*/
    signal (SIGUSR2, dts_sigHandler);	/* abort request		*/


    dts_connectToMonitor (dts);		/* connect to the status mon	*/

    /* Start the server.  Never returns.
    */
    dtsLog (dts, "DTS running as daemon\n");
    xr_startServer ();
}


/**
 *  DTS_SIGHANDLER -- Signal handler for the DTS daemon.
 *
 *  @fn		dts_sigHandler (int sig)
 *
 *  @param  sig		signal number
 */
void
dts_sigHandler (int sig)
{
    switch (sig) {
    case SIGHUP:
	dtsLog (dts, "HangUp signal received");
	break; 			/* hangup signal caught 		*/
    case SIGTERM:
	dtsLog (dts, "Terminate signal received");
	exit (0); 		/* terminate signal caught 		*/
    case SIGUSR1:
	dtsLog (dts, "Shutdown signal received");
	dts_setQueueShutdown (1);
	break; 			/* user1 (shutdown) signal caught 	*/
    case SIGUSR2:
	dtsLog (dts, "Abort signal received");
	dts_setQueueShutdown (2);
	exit (3);
	break; 			/* user2 (abort) signal caught 		*/
    }
}

void
dts_setQueueShutdown (int level)
{
    register int i = 0;
    int  count = 0;
    dtsQueue *dtsq;

    dts->shutdown = level;
    xr_setShutdownLevel (level);

    for (i=0; i < dts->nqueues; i++) {
	dtsq = dts->queues[i];
	if ((count = dts_semGetVal (dtsq->countSem))) {
	    dtsLog (dts, "Shutting down active queue [%d]'%s' \n", 
		i, dtsq->name);
	    dts_semSetVal (dtsq->activeSem, QUEUE_SHUTDOWN);
	} else {
	    dtsLog (dts, "Shutting down queue [%d]'%s' \n", i, dtsq->name);
	    pthread_kill (dtsq->qm_tid, SIGTERM);
	}
    }
}


/**
 *  DTS_PAUSE -- Dummy procedure to provide a debugger breakpoint.
 */
void
dts_pause()
{
    static int count = 0;
    count++;
}



/* Locates a segment, attaches our dtsq->qstat to the shared memory */

/**
 *
 */
void
dtsShm (DTS *dts)
{
    int   shmid, k=0;
    key_t key = 5678;
    dtsQueue   *dtsq = (dtsQueue *) NULL;	
    shmQueueStat *shmQstat;
  

    if (dts == NULL)
	return;

    if ((shmid = shmget (key, max(1,dts->nqueues) * sizeof(shmQueueStat),
	IPC_CREAT | 0666)) < 0) {
            perror("dtsShm: shmget");
            exit(1);
    }
    if ((shmQstat = (shmQueueStat *) shmat (shmid, NULL, 0)) == (void *) -1) {
	perror("dtsShm: shmat");
	exit(1);
    }


    for (k=0; k < dts->nqueues; k++) {
	dtsq = dts->queues[k];
	dtsq->qstat = (shmQstat+k);
    }	
}  



/****************************************************************************/

/**
 *  DTS_LOCALTIME - Generate a local time string.
 *
 *  @fn  char *dts_localTime()
 */
char *
dts_localTime ()
{
    time_t      t  = time (NULL);
    struct tm   tbuf, *tm;
    static char tstr[SZ_TSTR];


    tm = localtime_r (&t, &tbuf);

    memset (tstr, 0, SZ_TSTR);
    strftime (tstr, SZ_TSTR, "%FT%T", tm);

    return ( dts_strbuf (tstr) );
}


/**
 *  DTS_UTTIME - Generate a Greenwich Mean time string.
 *
 *  @fn  char *dts_UTTime()
 */
char *
dts_UTTime ()
{
    time_t      t  = time (NULL);
    struct tm  *tm, tbuf;
    char tstr[SZ_TSTR];

    tm = gmtime_r (&t, &tbuf);
    memset (tstr, 0, SZ_TSTR);
    strftime (tstr, SZ_TSTR, "%FT%T", tm);

    return ( dts_strbuf (tstr) );
}



/****************************************************************************/


/**
 *  DTS_GETLOCALIP -- Get the local IP address as a string.
 *
 *  @fn   char *dts_getLocalIP ()
 *
 *  @return 		character string of IP, e.g. "127.0.0.1"
 */

#define	USE_DNS_IP	

void 
dts_getPrimaryIP (char* buffer, size_t buflen)
{
    const char *kGoogleDnsIp = "8.8.8.8";  /* Google's public DNS server */
    unsigned short kDnsPort  = 53;
    struct sockaddr_in serv;
    int    sock, err;
    const  char *p;

    memset (buffer, 0, buflen);
    if ((sock = socket (AF_INET, SOCK_DGRAM, 0)) < 0) {
        strcpy (buffer, "127.0.0.1");   /* cannot get socket     */
        return;
    }

    memset (&serv, 0, sizeof (serv));
    serv.sin_family = AF_INET;
    serv.sin_addr.s_addr = inet_addr(kGoogleDnsIp);
    serv.sin_port = htons(kDnsPort);

    if ((err = connect (sock, (struct sockaddr*)&serv, sizeof(serv))) >= 0) {
        struct sockaddr_in name;
        socklen_t namelen = sizeof(name);

        if ((err=getsockname (sock, (struct sockaddr*)&name, &namelen)) < 0)
            strcpy (buffer, "127.0.0.1");       /* cannot connect socket  */
        else
            if ((p=inet_ntop (AF_INET, &name.sin_addr, buffer, buflen)) == NULL)
                strcpy (buffer, "127.0.0.1");   /* cannot get IP buffer name */
    } else
        strcpy (buffer, "127.0.0.1");   /* cannot get IP buffer name */

    close(sock);
}


char *
dts_getLocalIP ()
{
    struct in_addr  x_addr;                     /* internet address */
    struct hostent* host;
    char   local[128], namebuf[128];
    static char localIP[128];
    static int  initialized = 0;
    extern int  h_errno;


    if (!initialized) {
	memset (localIP, 0, 128);
	initialized++;
    }

    if (localIP[0]) 		/* see if we already have a localIP	*/
	return (localIP);
   

#ifdef USE_DNS_IP
    dts_getPrimaryIP (localIP, 128);
    return (localIP);
#endif

    memset (local,   0, 128);
    memset (namebuf, 0, 128);

    /* Get the local ip address as a character string.
    */
    gethostname (namebuf, 128);
    if ( (host = dts_getHostByName (dts_resolveHost (namebuf))) == NULL ) {
	fprintf (stderr, 
	    "getLocalIP:  dts_getHostByName(%s) returns NULL;  %s\n", 
	    namebuf, hstrerror (h_errno));
        strcpy (localIP, (char *) "127.0.0.1");
	return (localIP);
    }
    x_addr.s_addr = *((unsigned long *) host->h_addr_list[0]);

    strcpy (localIP, inet_ntoa (x_addr) );

    if (strncasecmp (localIP, "127.0.0.1", 6) == 0) {
        if ( (host = gethostbyaddr(localIP,strlen(localIP),AF_INET)) == NULL ) {
	    fprintf (stderr, "getLocalIP:  gethostbyaddr() returns '%s'\n", 
		hstrerror(h_errno));
            strcpy (localIP, (char *) "127.0.0.1");
	    return (localIP);
	}
	strcpy (localIP, host->h_name);
    }

//fprintf (stderr, "getLocalIP = '%s'\n", (localIP[0] ? localIP : "127.0.0.1") );
    return ( (localIP[0] ? localIP : "127.0.0.1") );
}


/**
 *  DTS_GETLOCALHOST -- Get the local host name as a string.
 *
 *  @fn   char *dts_getLocalHost ()
 *
 *  @return 		name of host
 */
char *
dts_getLocalHost ()
{
    static char namebuf[128];
    static int  init = 0;

    /* Get the local ip address as a character string.
    */
    if (! init++) {
        memset (namebuf, 0, 128);
        gethostname (namebuf, 128);
    }

    return ( namebuf );
}


/**
 *  DTS_GETHOSTBYNAME -- Get the host entry associated with a (cached) name.
 *
 *  @fn   char *dts_getHostByName (char *name)
 *
 *  @param  name 	host name
 *  @return 		host entry structure pointer
 */
typedef struct {
    char   name[SZ_LINE];
    char   ip[SZ_LINE];
    struct hostent *host;
}  hostTab, *hostTabP;

struct hostent_wrapper {
    struct hostent hentry;
    int    status;
};

static hostTab hostab[MAX_CLIENTS];

/*
#define USE_DNS_LOOKUP
*/


struct hostent *
dts_getHostByName (char *name)
{
    static int initialized = 0;
    struct in_addr x_addr;
    struct hostent *hp = (struct hostent *) NULL;
    hostTab  *h = (hostTabP) hostab;
    int    i, len;
#ifdef LINUX
    struct hostent hostbuf;
    int    hres, herr, tmplen = SZ_LINE;
    char   tmp[SZ_LINE];
#endif


/*  FIXME  */
#ifdef USE_DNS_LOOKUP
#ifdef LINUX
    hres = gethostbyname_r (name, &hostbuf, tmp, tmplen, &hp, &herr);
#else
    hp = gethostbyname (name);
#endif
    return (hp);
#endif

    if (!initialized) {
	memset (hostab, 0, sizeof (hostab));
	initialized++;
    }

    for (i=0; i < MAX_CLIENTS; i++, h++) {
	if (h && h->name[0]) {
	    len = min (strlen (name), strlen (h->name));
	    if (strncmp (name, h->name, len) == 0)
		return (h->host);
	} else 
	    break;				/* end of cache list */
    }

    /*  If we overflow the cache use a DNS lookup.
     */
    if (i >= MAX_CLIENTS) {
	dtsErrLog (NULL, "dts_getHostByName(): cache overflow on '%s'", name);
#ifdef LINUX
        hres = gethostbyname_r (name, &hostbuf, tmp, tmplen, &hp, &herr);
#else
        hp = gethostbyname (name);
#endif
        return (hp);
    }

    /*  Host not found, resolve and add it to the cache.
     */
#ifdef LINUX
    hres = gethostbyname_r (name, &hostbuf, tmp, tmplen, &hp, &herr);
#else
    hp = gethostbyname (name);
#endif
    if (hp == (struct hostent *) NULL) {
	fprintf (stderr, "dts_getHostByName: cannot resolve '%s'\n", name);
	exit (0);
    }

    strcpy (h->name, name);
    x_addr.s_addr = *((unsigned long *) hp->h_addr_list[0]);
    strcpy (h->ip,   inet_ntoa (x_addr));

    h->host = dts_dupHostent (hp); 
    return (h->host);
}


/**
 *  DTS_PRINTHOSTTAB -- Debug util to print host table.
 */
void
dts_printHostTable ()
{
    register int  i=0;
    hostTab  *h = (hostTabP) hostab;

    for (i=0; i < MAX_CLIENTS; i++, h++) {
	if (h && h->name[0])
	    fprintf (stderr, "hostab[%d]: %15.15s -> %15.15s\n", 
		i, h->name, h->ip);
	else
	    break;
    }
}


/**
 *   DTS_DUPHOSTENT -- Duplicate a hostent structure via a deep copy.
 */
struct hostent *
dts_dupHostent (struct hostent *hentry)
{
    struct hostent_wrapper *oldhw = NULL;
    struct hostent_wrapper *newhw = NULL;
    int i = 0;
    int aliascount=0;
    int addrcount=0;

	
    if (!hentry)
    	return NULL;

    oldhw = (struct hostent_wrapper *) hentry;
    newhw = (struct hostent_wrapper *) calloc(1,sizeof(struct hostent_wrapper));
    bzero(newhw, sizeof (struct hostent_wrapper));

    newhw->hentry.h_addrtype = hentry->h_addrtype;
    newhw->hentry.h_length = hentry->h_length;
    newhw->status = oldhw->status;

    if (hentry->h_name)
    	newhw->hentry.h_name = strdup(hentry->h_name);

    if (hentry->h_aliases) {
    	for (i=0; hentry->h_aliases[i] != 0; i++)
    	    aliascount++;
    	aliascount++;

	newhw->hentry.h_aliases = (char **)calloc (1,aliascount*sizeof (char*));
	bzero(newhw->hentry.h_aliases, aliascount * sizeof(char*));

	for (i=0; hentry->h_aliases[i] != 0; i++) {
	    if (hentry->h_aliases[i])
		newhw->hentry.h_aliases[i] = strdup (hentry->h_aliases[i]);
	}
    }
	
    if (hentry->h_addr_list) {
	for (i=0; hentry->h_addr_list[i] != 0; i++)
	    addrcount++;
	addrcount++;
		
	newhw->hentry.h_addr_list = 
	    (char **) calloc (1, addrcount * sizeof (char *));
	bzero (newhw->hentry.h_addr_list, addrcount * sizeof (char *));
		
	for (i=0; hentry->h_addr_list[i] != 0; i++) {
	    if (hentry->h_addr_list[i])
		newhw->hentry.h_addr_list[i] = strdup (hentry->h_addr_list[i]);
	}
    }

    return (struct hostent *) newhw;
}


/**
 *  DTS_GETALIASHOST -- Get the host associated with a node alias.
 *
 *  @fn   char *dts_getAliasHost (char *alias)
 *
 *  @param  alias 	client alias
 *  @return 		name of host
 */
char *
dts_getAliasHost (char *alias)
{
    register int i;

    /* Get the client host name.
    */
    for (i=0; i < dts->nclients; i++) {
        if (strcmp (alias, dts->clients[i]->clientName) == 0) {
            return ( dts->clients[i]->clientHost );
	}
    }

    return ( NULL );
}


/**
 *  DTS_GETALIASPORT -- Get the port associated with a node alias.
 *
 *  @fn   char *dts_getAliasPort (char *alias)
 *
 *  @param  alias 	client alias
 *  @return 		name of host
 */
int
dts_getAliasPort (char *alias)
{
    register int i;

    /* Get the client host name.
    */
    for (i=0; i < dts->nclients; i++) {
        if (strcmp (alias, dts->clients[i]->clientName) == 0) {
            return ( dts->clients[i]->clientPort );
	}
    }

    return ( 0 );
}


/**
 *  DTS_GETALIASDEST -- Get the destination associated with a node alias.
 *
 *  @fn   char *dts_getAliasDest (char *alias)
 *
 *  @param  alias 	client alias
 *  @return 		destination as 'node:port'
 */
char *
dts_getAliasDest (char *alias)
{
    register int i;
    char  dest[SZ_LINE];

    /* Get the client host name.
    */
    memset (dest, 0, SZ_LINE);
    for (i=0; i < dts->nclients; i++) {
        if (strcmp (alias, dts->clients[i]->clientName) == 0) {
	    sprintf (dest, "%s:%d", 
		dts->clients[i]->clientHost, dts->clients[i]->clientPort );
	    return ( strdup (dest) );
	}
    }

    return ( strdup (alias) );
}


/**
 *  DTS_GETNUMNETWORKS -- Get the number of unique networks in a configuration.
 *
 *  @fn   int dts_getNumNetworks (DTS *dts)
 *
 *  @return 		number of unique networks in a config structure
 */
int
dts_getNumNetworks (DTS *dts)
{
    char *n1, *n2;
    int i, j, num = 0;


    /*  Gather the network names, count the unique names.
     */
    for (i=0; i < dts->nclients; i++) {
        for (j=i+1; j < dts->nclients; j++) {
	    n1 = dts->clients[i]->clientNet;
	    n2 = dts->clients[j]->clientNet;
	    if (n1 && n2 && strcmp (n1, n2) == 0)
		break;
        }
	if (j == dts->nclients)
	    num++;
    }

    return (num);
}


/**
 *  DTS_RESOLVEHOST -- Resolve a host name to an IP string.
 *
 *  @fn   char *dts_resolveHost (char *hostname)
 *
 *  @param  hostname	FQDN hostname
 *  @return 		character string of IP, e.g. "127.0.0.1"
 */
char *
dts_resolveHost (char *hostname)
{
    struct in_addr  x_addr;                     /* internet address */
    struct hostent* host;
    char   *ip, rhost[SZ_PATH], *ret_val;
    char   *hname = NULL;
    char   *name  = hostname;
    char    local[SZ_PATH];
    int     port = 0;


    if (hostname == NULL)
	return (NULL);

    /* Strip any port specification.
    */
    memset (local, 0, SZ_PATH);
    memset (rhost, 0, SZ_PATH);
    if (hostname) {
        strcpy (rhost, hostname);
        if ( (ip = strchr (rhost, (int)':')) ) {
	    *ip = '\0';
 	    port = atoi (++ip);
	}
        name = rhost;
    } else {
	fprintf (stderr, "resolveHost:  hostname is NULL\n");
        return ("127.0.0.1");
    }

    if (name == NULL || strcasecmp (name, "localhost") == 0) {
	if (!name)
	    fprintf (stderr, "resolveHost:  'name' is localhost\n");
	hname = dts_getLocalIP ();

    } else {
	/* See if we can resolve a DTS name to the host machine.
	 */
	name = dts_nameToHost (dts, rhost);

        /* Get the local ip address as a character string.
        */
        if ( (host = dts_getHostByName (name)) != NULL ) {
            x_addr.s_addr = *((unsigned long *) host->h_addr_list[0]);
	    strcpy (local, inet_ntoa (x_addr));
	    if (strcmp (local, "127.0.0.0") == 0)	/* for OSX 	*/
	        strcpy (local, "127.0.0.1");
	    if (port)
		sprintf (local, "%s:%d", local, port);
	    hname = dts_strbuf (local);
	} else {
	    if (dts && dts->verbose)
		fprintf (stderr, "dts_resolveHost: unknown host '%s'\n", 
		    hostname);
	    return (name);
	}
    }

    if (hname) {
        ret_val = hname;
    } else if (dts) {
	ret_val = dts_cfgNameToIP (dts, name);
    } else  {
	ret_val = dts_getLocalIP ();
    }

    return ( ret_val );
}



/****************************************************************************/

/**
 *  LOGTIME - Generate a time string for the log.
 *
 *  @fn   str = dts_logtime (void)
 *
 *  @return 		a standard logfile timestring
 */
char *
dts_logtime ()
{
    time_t      t  = time (NULL);
    struct tm  *tm, tbuf;
    static char tstr[128];

    tm = gmtime_r (&t, &tbuf);

    memset (tstr, 0, 128);
    strftime (tstr, 128, "%m%d %T", tm);

    return (tstr);
}



/****************************************************************************/

/**
 *  Transfer and measurement utilities.
 */
struct timeval measure_start_tv;
struct timeval io_tv 	= {0, 0};


/**
 *  @brief  Timer utils.
 *
 *  @fn  measure_start (void)
 */

void 
dts_tstart (struct timeval *tv)
{
    gettimeofday (tv, NULL);
}

double
dts_tstop (struct timeval tv1)
{
    struct timeval tv2 = {0, 0}, tdiff = {0, 0};

    gettimeofday (&tv2, NULL);
    tdiff.tv_sec   = tv2.tv_sec  - tv1.tv_sec;
    tdiff.tv_usec  = tv2.tv_usec - tv1.tv_usec;
    if (tdiff.tv_usec >= 1000000) {
        tdiff.tv_usec -= 1000000;
	tdiff.tv_sec++;
    } else if (tdiff.tv_usec < 0) {
        tdiff.tv_usec += 1000000;
	tdiff.tv_sec--;
    }

    return ((double)tdiff.tv_sec + (double)tdiff.tv_usec / 1000000.0);
}


/* DTS_TIMEDIFF -- Return (t2 - t1) time in seconds.
 */
double
dts_timediff (struct timeval t1, struct timeval t2)
{
    double t = 0.0;
    struct timeval tv;

    memset (&tv, 0, sizeof (struct timeval));

    tv.tv_sec  = t2.tv_sec  - t1.tv_sec;
    tv.tv_usec = t2.tv_usec - t1.tv_usec;
    if (tv.tv_usec >= 1000000) {
        tv.tv_usec -= 1000000;   tv.tv_sec++;
    } else if (tv.tv_usec < 0) {
        tv.tv_usec += 1000000;   tv.tv_sec--;
    }

    t = ((double)tv.tv_sec + (double)tv.tv_usec / 1000000.0);
    return (t);
}



/**
 *  @brief  Start timer
 *
 *  @fn  measure_start (void)
 */
void 
measure_start (void)
{
    gettimeofday (&measure_start_tv, NULL);
}


/**
 *  @brief  Stop timer and print summary of transfer stats.
 *
 *  @fn  measure_stop (long transferred)
 *
 *  @param  transferred	    number of bytes transferred
 */
void 
measure_stop (long transferred)
{
    struct timeval tv;
    double tput, tputMB;


    gettimeofday (&tv, NULL);
    tv.tv_sec -= measure_start_tv.tv_sec - io_tv.tv_sec;
    tv.tv_usec -= measure_start_tv.tv_usec - io_tv.tv_usec;
    if (tv.tv_usec < 0) {
        tv.tv_usec += 1000000; tv.tv_sec -= 1;
    }

    tput = ((double)transferred * 8)
           / ((double)tv.tv_sec + (double)tv.tv_usec / 1000000.0);
    tput /= 1000000.0; /* megabits per sec */
    tputMB = (float)tput / 8.;

    fprintf (stderr, "DONE. Transferred: %ld bytes. Net time: %ld.%06ld\n",
            transferred, (long)tv.tv_sec, (long)tv.tv_usec);
    fprintf (stderr, "Throughput: %.3lf mb/s (%.3lf %s/s)\n", 
	(tputMB < 1.0 ? tput * 100 : tput), tputMB,
	(tputMB < 1.0 ? "KB" : "MB"));
    fprintf (stderr, "I/O time: %ld.%06ld\n", 
	(long)io_tv.tv_sec, (long)io_tv.tv_usec);
}


/**
 *  @brief  Return throughput in megabits/s
 *
 *  @fn  Mbs = transferMb (long fileSize, int sec, int usec)
 *
 *  @param  fileSize	number of bytes transferred
 *  @param  sec	    	transfer time seconds
 *  @param  usec    	transfer time micro-seconds
 */
double
transferMb (long fileSize, int sec, int usec)
{
    double  tput, tputMB;

    tput = ((double)fileSize * 8) / ((double)sec + (double)usec / 1000000.0);
    tput /= 1045876.0; 				/* megabits per sec */
    tputMB = (float)tput / 8.;

    return (tput);
}


/**
 *  @brief  Return throughput in megabytes/s
 *
 *  @fn  MBs = transferMB (long fileSize, int sec, int usec)
 *
 *  @param  fileSize	number of bytes transferred
 *  @param  sec	    	transfer time seconds
 *  @param  usec    	transfer time micro-seconds
 */
double
transferMB (long fileSize, int sec, int usec)
{
    double  tput, tputMB;

    tput = ((double)fileSize) / ((double)sec + (double)usec / 1000000.0);
    tput /= 1045876.0; 				/* megabits per sec */
    tputMB = (float)tput;

    return (tputMB);
}



/*  File listing utilities.
 */

static	int  m1[] = { 1, S_IREAD>>0, 'r', '-' };
static	int  m2[] = { 1, S_IWRITE>>0, 'w', '-' };
static	int  m3[] = \
	{ 3, S_ISUID|(S_IEXEC>>0), 's', S_IEXEC>>0, 'x', S_ISUID, 'S', '-' };
static	int  m4[] = { 1, S_IREAD>>3, 'r', '-' };
static	int  m5[] = { 1, S_IWRITE>>3, 'w', '-' };
static	int  m6[] = \
	{ 3, S_ISGID|(S_IEXEC>>3), 's', S_IEXEC>>3, 'x', S_ISGID, 'S', '-' };
static	int  m7[] = { 1, S_IREAD>>6, 'r', '-' };
static	int  m8[] = { 1, S_IWRITE>>6, 'w', '-' };
static	int  m9[] = \
	{ 3, S_ISVTX|(S_IEXEC>>6), 't', S_ISVTX, 'T', S_IEXEC>>6, 'x', '-' };

static	int *m[] = { m1, m2, m3, m4, m5, m6, m7, m8, m9};


/**
 *  DTS_FMTMODE -- Format the file mode permissions as a string.
 */
char *
dts_fmtMode (char *lp, int flags)
{
    int **mp;

	    
    *lp++ = (S_ISDIR(flags) ? 'd': '-');	/* set directory 	*/

    for (mp = &m[0]; mp < &m[sizeof (m)/sizeof (m[0])]; ) {
	register int *pairp = *mp++;
	register int n = *pairp++;

	while (n-- > 0) {
	    if ((flags&*pairp) == *pairp) {
		pairp++;
		break;
	    } else
		pairp += 2;
	}
	*lp++ = *pairp;
    }
    return (lp);
}


/**
 *  DTS_PATMATCH -- Match a string against a pattern template.
 */
int
dts_patMatch (char *str, char *pattern)
{
    register int done, match, status;
    register char c, *p;


    if (pattern == (char *) NULL)
        return (TRUE);
    if (strlen (pattern) == 0)
        return (TRUE);
    if (strcmp (pattern, "*") == 0)
        return (TRUE);
    if (strcmp (str, pattern) == 0 || strstr (pattern, str))
        return (TRUE);
    done = FALSE;

    while ((*pattern != '\0') && !done) {
	if (*str == '\0')
	    if ((*pattern != '{') && (*pattern != '*'))
	        break;

	switch (*pattern) {
	case '\\':
	    pattern++;
	    if (*pattern != '\0')
	        pattern++;
	    break;
	case '*':
	    pattern++;
	    status = FALSE;
	    while ((*str != '\0') && !status)
	    	status = dts_patMatch ((char *) str++, pattern);
	    if (status) {
	    	while (*str != '\0')
	    	    str++;
	    	while (*pattern != '\0')
	    	    pattern++;
	    }
	    break;
	case '[':
	    pattern++;
	    for ( ; ; ) {
	        if ((*pattern == '\0') || (*pattern == ']')) {
	    	    done = TRUE;
	    	    break;
	        }
	        if (*pattern == '\\') {
	    	    pattern++;
	    	    if (*pattern == '\0') {
	    	    	done = TRUE;
	    	    	break;
	    	    }
	        }
	        if (*(pattern + 1) == '-') {
	    	    c = (*pattern);
	    	    pattern += 2;
	    	    if (*pattern == ']') {
	    	    	done = TRUE;
	    	    	break;
	    	    }
	    	    if (*pattern == '\\') {
	    	    	pattern++;
	    	    	if (*pattern == '\0') {
	    	    	    done = TRUE;
	    	    	    break;
	    	    	}
	    	    }
	    	    if ((*str < c) || (*str > *pattern)) {
	    	    	pattern++;
	    	    	continue;
	    	    }
	        } else if (*pattern != *str) {
	    	    pattern++;
	    	    continue;
	        }
	        pattern++;
	        while ((*pattern != ']') && (*pattern != '\0')) {
	    	    if ((*pattern == '\\') && (*(pattern + 1) != '\0'))
	    	    	pattern++;
	    	    pattern++;
	        }
	        if (*pattern != '\0') {
	    	    pattern++;
	    	    str++;
	        }
	        break;
	    }
	    break;

	case '?':
	    pattern++;
	    str++;
	    break;

	case '{':
	    pattern++;
	    while ((*pattern != '}') && (*pattern != '\0')) {
	    	p = str;
	    	match = TRUE;
	    	while ((*p != '\0') && (*pattern != '\0') && 
	    	    (*pattern != ',') && (*pattern != '}') && match) {
	    	    if (*pattern == '\\')
	    	    	pattern++;
	    	    match = (*pattern == *p);
	    	    p++;
	    	    pattern++;
	    	}
	    	if (*pattern == '\0') {
	    	    match = FALSE;
	    	    done = TRUE;
	    	    break;
	    	} else if (match) {
	    	    str = p;
	    	    while ((*pattern != '}') && (*pattern != '\0')) {
	    	    	pattern++;
	    	    	if (*pattern == '\\') {
	    	    	    pattern++;
	    	    	    if (*pattern == '}')
			       pattern++;
	    	    	}
	    	    }
	    	} else {
	    	    while ((*pattern != '}') && (*pattern != ',') && 
	    	    	(*pattern != '\0')) {
	    	    	pattern++;
	    	    	if (*pattern == '\\') {
	    	    	    pattern++;
	    	    	    if ((*pattern == '}') || (*pattern == ','))
				pattern++;
	    	    	}
	    	    }
	    	}
	    	if (*pattern != '\0')
	    	    pattern++;
	    }
	    break;
	default:
	    if (*str != *pattern)
	    	done = TRUE;
	    else {
	    	str++;
	    	pattern++;
	    }
	}
    }

    while (*pattern == '*')
        pattern++;

    return ((*str == '\0') && (*pattern == '\0'));
}


/**
 *  DTS_ISTEMPLATE -- Is a string a pattern template?
 */
int
dts_isTemplate (char *s)
{
    return (strchr (s,(int)'*') ||
            strchr (s,(int)'?') ||
            strchr (s,(int)'[') ||
            strchr (s,(int)'{') );
}


/*****************************************************************************/

/**
 *  DTSGETS   A smart fgets() function
 *
 *  Read the line; unlike fgets(), we read the entire line but dump
 *  characters that go past the end of the buffer.    We accept
 *  CR, LF, or CR LF for the line endings to be "safe".
 *
 *  @fn   char *dtsGets (char *s, int len, FILE *fp)
 *
 *  @param  s		string buffer
 *  @param  len		length of buffer
 *  @param  fp		file descriptor
 *  @return 		pointer to next string on the stream
 */
char *
dtsGets (char *s, int len, FILE *fp)
{
    char  *ptr, *end;
    int	   ch;

    if (!s)
	return ((char *)NULL);

    ptr = s;
    end = s + len - 1;

    memset (ptr, 0, len);
    while ((ch = getc(fp)) != EOF) {
        if (ch == '\n')
            break;
        else if (ch == '\r') {
            /* See if a LF follows...
            */
            ch = getc(fp);
            if (ch != '\n')
                ungetc(ch, fp);
            break;

        } else if (ptr < end)
            *ptr++ = ch;
    }
    *ptr = '\0';

    if (ch == EOF && ptr == s)
        return (NULL);
    else {
	int len = strlen (s);

	/* Trim trailing whitespace.
	*/
	end = (s + len - 1); 
	while ( end > s && *end && isspace(*end) )
	    *end-- = '\0';

        return (s);
    }
}


/**
 *  DTS_STRBUF -- Return a string from a static buffer.
 */
#define NUM_STR_BUF		1024
#define SZ_BUF_STR		1024
#define STRBUF_MAX_WAIT		5


/* create a mutex for the string buffer */
pthread_mutex_t str_mut = PTHREAD_MUTEX_INITIALIZER;

char *
dts_strbuf (char *s)
{
    static int  index = 0;
#ifdef USE_STR_TRYLOCK
    static int  tot_leak = 0;
    int   max_wait = STRBUF_MAX_WAIT;
#endif
    static char strbuf[NUM_STR_BUF][SZ_BUF_STR];
    char *buf;
    int   len;


    len = strlen (s);
#ifdef USE_STR_TRYLOCK
    while (pthread_mutex_trylock (&str_mut)) {
	dtsLog (dts, "strbuf mutex lock waiting");
	dtsSleep (1);
	if (! max_wait--) {
	    buf = calloc (1, len + 1);
    	    strcpy (buf, s);
	    tot_leak += len;
	    dtsLog (dts, "strbuf leaked %d bytes (%d total) '%s'", 
		len, tot_leak, s);
	    return (buf);	/* FIXME - known memory leak	*/
	}
    }
#else
    pthread_mutex_unlock (&str_mut);
#endif

    index = (index + 1) % (NUM_STR_BUF - 1);
    memset (&strbuf[index], 0, SZ_BUF_STR);

    if (len > SZ_BUF_STR)
	s[SZ_BUF_STR-1] = '\0'; 		/* truncate the string 	*/
 
    buf = &strbuf[index][0];
    strncpy (buf, s, len);

    pthread_mutex_unlock (&str_mut);

    return (buf);
}



/*****************************************************************************/

/**
 * DTSERROR -- Print a fatal error message.
 */
#define _DTS_SRC_	1

void
dtsError (char *msg)
{
#ifdef _DTS_SRC_
    extern DTS *dts;
    // dts->nerrs++;
    dtsLog (dts, "dtsError: ^s %s ^r", msg);
#endif

#ifdef USE_PERROR
    perror (msg);
#else
    fprintf (stderr, "dtsError: %s\n", msg);
#endif
}


/**
 *  DTS_DEBUGLEVEL -- Return the current debug level to procedures without
 *  a DTS pointer.
 */
int
dts_debugLevel () 
{
    char *d = getenv ("DTS_DEBUG");

    return (d ? atoi (d) : (dts ? dts->debug : 0));
}



/******************************************************************************
**  Utility print routines.
******************************************************************************/

char *
dts_printDTS (DTS *dts, FILE *fd)
{
    char * buf = calloc (1, SZ_MAX_MSG);
    register int i;


    if (!dts) {
        doprnt (buf, "dts_printDTS:  NULL pointer\n");
        goto done_;
    }

    doprnt (buf, "DTS:  %s\n", dts->whoAmI);
    doprnt (buf, " serverUrl:  %s\n", dts->serverUrl);
    doprnt (buf, "   cfgFile:  %s\n", dts->configFile);
    doprnt (buf, "   workDir:  %s\n", dts->workingDir);
    doprnt (buf, "   logFile:  %s\n", dts->logFile);
    doprnt (buf, "  mon_port:  %d\n", dts->mon_port);
    doprnt (buf, "   verbose:  %d\n", dts->verbose);
    doprnt (buf, "     debug:  %d\n", dts->debug);

    for (i=0; i < dts->nclients; i++) {
        doprnt (buf, " Client[%d]:  %s:%d\n", i,
		dts->clients[i]->clientUrl, dts->clients[i]->clientPort);
        doprnt (buf, "      conn:  %d\n", dts->clients[i]->conn);
        doprnt (buf, "    active:  %d\n", dts->clients[i]->active);
    }
    doprnt (buf, "\n");

done_:
    if (fd) {
	fprintf (fd, "%s", buf);
        free ((void *) buf);
	return (NULL);
    } else
	return (buf);
}


char *
dts_printDTSClient (DTS *dts, FILE *fd)
{
    char *buf = calloc (1, SZ_MAX_MSG);


    if (!dts) {
        doprnt (buf, "dts_printDTSClient:  NULL pointer\n");
        goto done_;
    }

    doprnt (buf, "DTS:  %s\n", dts->whoAmI);
    doprnt (buf, " serverUrl:  %s\n", dts->serverUrl);
    doprnt (buf, "   cfgFile:  %s\n", dts->configFile);
    doprnt (buf, "   workDir:  %s\n", dts->workingDir);
    doprnt (buf, "   logFile:  %s\n", dts->logFile);
    doprnt (buf, "  mon_port:  %d\n", dts->mon_port);
    doprnt (buf, "   verbose:  %d\n", dts->verbose);
    doprnt (buf, "     debug:  %d\n", dts->debug);

done_:
    if (fd) {
	fprintf (fd, "%s", buf);
	free ((void *) buf);
	return (NULL);
    } else
	return (buf);
}


char *
dts_printAllQueues (DTS *dts, FILE *fd)
{
    char *buf = calloc (1, SZ_MAX_MSG);
    register int i;


    if (!dts) {
        doprnt (buf, "dts_printAllQueues:  NULL pointer\n");
        goto done_;
    }

    for (i=0; i < dts->nqueues; i++)
	if (fd)
	    dts_printQueue (dts->queues[i], fd);
	else
	    strcat (buf, dts_printQueue (dts->queues[i], fd));
    
    /* If we're printing to a file, it would have been done in the loop.
    */
done_:
    if (fd) {
	free ((void *) buf);
	return (NULL);
    } else
	return (buf);
}


char *
dts_printQueue (dtsQueue *dtsq, FILE *fd)
{
    char *buf = calloc (1, SZ_MAX_MSG);

    if (!dts) {
        doprnt (buf, "dts_printQueue:  NULL pointer\n");
        goto done_;
    }

    doprnt (buf, "Queue:        %s     ptr = 0x%lx\n", dtsq->name, (long)dtsq);
    doprnt (buf, "        src:  %s\n", dtsq->src);
    doprnt (buf, "       dest:  %s\n", dtsq->dest);
    doprnt (buf, "deliveryDir:  %s\n", dtsq->deliveryDir);
    doprnt (buf, "deliveryCmd:  %s\n", dtsq->deliveryCmd);
    doprnt (buf, "   transfer:  %s\n", dtsq->transfer);

    doprnt (buf, "   avg_rate:  %d\n", dtsq->stats.avg_rate);
    doprnt (buf, "   avg_time:  %d\n", dtsq->stats.avg_time);
    doprnt (buf, "   avg_size:  %d\n", dtsq->stats.avg_size);
    doprnt (buf, "   tot_xfer:  %d\n", dtsq->stats.tot_xfer);
    doprnt (buf, "     nfiles:  %d\n", dtsq->stats.nfiles);

    doprnt (buf, "     status:  %d\n", dtsq->status);
    doprnt (buf, "       type:  %s\n", dts_cfgQTypeStr(dtsq->type));
    doprnt (buf, "       mode:  %s\n", dts_cfgQModeStr(dtsq->mode));
    doprnt (buf, "     method:  %s\n", dts_cfgQMethodStr(dtsq->method));
    doprnt (buf, "   nthreads:  %d\n", dtsq->nthreads);
    doprnt (buf, "       port:  %d\n", dtsq->port);
    doprnt (buf, "\n");

done_:
    if (fd) {
	fprintf (fd, "%s", buf);
	free ((void *) buf);
	return (NULL);
    } else
	return (buf);
}



/*  Encode format an arguments into string buffer.
*/
void
doprnt (char *buf, char *format, ...)
{
    char line[SZ_LINE];
    va_list argp;
    extern void dts_encodeString();

    va_start (argp, format);
    memset (line, 0, SZ_LINE);
    dts_encodeString (line, format, &argp);
    strcat (buf, line);
    va_end (argp);
}


/*  Wrapper for system sleep().
 */
int
dtsSleep (unsigned int seconds)
{
    if (dts && dts->debug > 2)
	dtsLog (dts, "**** Sleep: %d\n", seconds);
    return ( sleep (seconds) );
}


/***************************************************************************/

/**
 *  Characters used for Base64 encoding
 */  
const char *b64_chars =
	"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static int  _base64_char_value (char base64char);
static void _base64_encode_triple (unsigned char triple[3], char result[4]);
static int  _base64_decode_triple (char quadruple[4], unsigned char *result);



/**
 *  encode an array of bytes using Base64 (RFC 3548)
 *
 *  @param source the source buffer
 *  @param sourcelen the length of the source buffer
 *  @param target the target buffer
 *  @param targetlen the length of the target buffer
 *  @return 1 on success, 0 otherwise
 */  
int base64_encode (unsigned char *source, size_t sourcelen, char *target,
		size_t targetlen)
{
    /* check if the result will fit in the target buffer.
    */
    if ((sourcelen+2)/3*4 > targetlen-1)
	 return 0;

    /* encode all full triples */
    while (sourcelen >= 3) {
	_base64_encode_triple (source, target);
	sourcelen -= 3;
	source += 3;
	target += 4;
    }

    /* encode the last one or two characters */
    if (sourcelen > 0) {
	unsigned char temp[3];
	memset (temp, 0, sizeof(temp));
	memcpy (temp, source, sourcelen);
	_base64_encode_triple (temp, target);
	target[3] = '=';
	if (sourcelen == 1)
	    target[2] = '=';

	target += 4;
    }

    /* terminate the string */
    target[0] = 0;

    return 1;
} 


/**
 * decode base64 encoded data
 *
 * @param source the encoded data (zero terminated)
 * @param target pointer to the target buffer
 * @param targetlen length of the target buffer
 * @return length of converted data on success, -1 otherwise
 */  
int base64_decode (char *source, unsigned char *target, size_t targetlen)
{
    char *src, *tmpptr, quadruple[4];
    unsigned char tmpresult[4];
    int i, tmplen = 3;
    size_t converted = 0;


    /* concatinate '===' to the source to handle unpadded base64 data.
    */
    src = (char *) calloc (1, strlen(source)+5);
    if (src == NULL)
	return -1;
    strcpy (src, source);
    strcat (src, "====");
    tmpptr = src;

    /* convert as long as we get a full result */
    while (tmplen == 3) {
	/* get 4 characters to convert.
	*/
	for (i=0; i<4; i++) {
	    /* skip invalid characters - we won't reach the end.
	    */
	    while (*tmpptr != '=' && _base64_char_value (*tmpptr)<0)
	 	tmpptr++;

     	    quadruple[i] = *(tmpptr++);
 	}

	/* convert the characters */
	tmplen = _base64_decode_triple (quadruple, tmpresult);

	/* check if the fit in the result buffer */
	if (targetlen < tmplen) {
	     free (src);
	     return -1;
 	}

 	/* put the partial result in the result buffer */
 	memcpy (target, tmpresult, tmplen);
 	target += tmplen;
 	targetlen -= tmplen;
 	converted += tmplen;
    }

    free ((char *) src);

    return converted;
}


/**
 *  encode three bytes using base64 (RFC 3548)
 *
 *  @param triple three bytes that should be encoded
 *  @param result buffer of four characters where the result is stored
 */  
static void
_base64_encode_triple (unsigned char triple[3], char result[4])
{
    int tripleValue, i;

    tripleValue = triple[0];
    tripleValue *= 256;
    tripleValue += triple[1];
    tripleValue *= 256;
    tripleValue += triple[2];

    for (i=0; i<4; i++) {
	result[3-i] = b64_chars[tripleValue % 64];
	tripleValue /= 64;
    }
} 


/**
 * determine the value of a base64 encoding character
 *
 * @param base64char the character of which the value is searched
 * @return the value in case of success (0-63), -1 on failure
 */  
static int
_base64_char_value (char base64char)
 {
    if (base64char >= 'A' && base64char <= 'Z')
	return base64char-'A';
    if (base64char >= 'a' && base64char <= 'z')
	return base64char-'a'+26;
    if (base64char >= '0' && base64char <= '9')
	return base64char-'0'+2*26;
    if (base64char == '+')
	return 2*26+10;
    if (base64char == '/')
	return 2*26+11;
    return -1;
} 


/**
 * decode a 4 char base64 encoded byte triple
 *
 * @param quadruple the 4 characters that should be decoded
 * @param result the decoded data
 * @return lenth of the result (1, 2 or 3), 0 on failure
 */  
static int
_base64_decode_triple (char quadruple[4], unsigned char *result)
 {
    int i, triple_value, bytes_to_decode = 3, only_equals_yet = 1;
    int char_value[4];

    for (i=0; i<4; i++)
	char_value[i] = _base64_char_value (quadruple[i]);

    /* check if the characters are valid */
    for (i=3; i>=0; i--) {
	if (char_value[i] < 0) {
	    if (only_equals_yet && quadruple[i]=='=') {

		/* we will ignore this character anyway, make it something
		 * that does not break our calculations.
		 */
		char_value[i]=0;
		bytes_to_decode--;
		continue;
	    }
	    return 0;
	}
	/* after we got a real character, no other '=' are allowed anymore */
	only_equals_yet = 0;
    }

    /* if we got "====" as input, bytes_to_decode is -1 */
    if (bytes_to_decode < 0)
	bytes_to_decode = 0;

    /* make one big value out of the partial values */
    triple_value = char_value[0];
    triple_value *= 64;
    triple_value += char_value[1];
    triple_value *= 64;
    triple_value += char_value[2];
    triple_value *= 64;
    triple_value += char_value[3];

    /* break the big value into bytes */
    for (i=bytes_to_decode; i<3; i++)
	triple_value /= 256;
    for (i=bytes_to_decode-1; i>=0; i--) {
	result[i] = triple_value%256;
	triple_value /= 256;
    }

    return bytes_to_decode;
} 
