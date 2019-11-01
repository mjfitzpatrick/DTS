/**
 *  DTS.H -- DTS data structures and macro definitions.
 *
 */

#include <xmlrpc-c/base.h>		/* XMLRPC-C interface		  */
#include <xmlrpc-c/client.h>
#include <xmlrpc-c/server.h>
#include <xmlrpc-c/server_abyss.h>

#include <sys/types.h>			/* for struct stat		  */
#include <sys/stat.h>
#ifdef Linux
#include <sys/vfs.h>
#else
#ifdef Darwin
#include <sys/param.h>                          /* for statfs()         */
#include <sys/mount.h>
#include <sys/statvfs.h>
#endif
#endif
#include <unistd.h>


#include "xrpc.h"			/* XRPC Wrapper			  */

#define DTS_PORT        3000		/* default server port		  */
#define DTS_HOST        "localhost"	/* default server host 		  */
#define DTS_SVC         "/RPC2"		/* xml-rpc service name		  */
#define DTS_NAME        "DTS"		/* http agent name		  */
#define DTS_VERSION     "v1.0"		/* http agent version		  */

#define DTSMON_PORT     2999		/* default monitor port		  */
#define DTSMON_HOST     "localhost"	/* default monitor host		  */
#define DTSMON_NAME     "DTSMonitor"	/* monitor task name		  */
#define DTSMON_VERSION  "v1.0"		/* monitor task version		  */

#define DTSSH_PORT      2998		/* DTS shell server port	  */
#define DTSSH_HOST      "localhost"	/* default monitor host		  */
#define DTSSH_NAME      "DTShell"	/* shell task name		  */
#define DTSSH_VERSION   "v1.0"		/* shell task version		  */

#define DTSCP_PORT      2997		/* DTS shell server port	  */
#define DTSCP_HOST      "localhost"	/* default monitor host		  */
#define DTSCP_NAME      "DTShell"	/* shell task name		  */
#define DTSCP_VERSION   "v1.0"		/* shell task version		  */

#define DTSQ_PORT       3100		/* DTS shell server port	  */
#define DTSQ_HOST       "localhost"	/* default monitor host		  */
#define DTSQ_NAME       "DTSQ"		/* task name		  	  */
#define DTSQ_VERSION    "v1.0"		/* task version		  	  */

#define	DTSMON_CLIENT	0
#define	DTSMON_NONE	-1

#define DTS_ENV_CONFIG	"DTS_CONFIG"	/* config file paths		  */
#define DTS_CONFIG1	"~/.dts"	/* config directory		  */
#define DTS_CONFIG2	"~/.dts_config" /* config file			  */
#define DTS_CONFIG3	"/usr/local/lib/dts.cfg"

#define	DTS_CWD		"./"		/* DTS working directory	  */
#define	DTS_LOCKFILE	"dtsd.lock"	/* DTS daemon lock file		  */
#define	DTS_LOGFILE	"dtsd.log"	/* DTS message log		  */

#define	DTS_PORT_STR	"3000"		/* DTS comm. port (as string)	  */
#define	DTS_CPORT_STR	"3001"		/* DTS contact port (as string)	  */
#define	DTS_LPORT_STR	"3005"		/* DTS lo port (as string)	  */
#define	DTS_HPORT_STR	"3099"		/* DTS hi port (as string)	  */

#define DTS_DIR_MODE		0775	/* default directory mode	  */
#define DTS_FILE_MODE		0664	/* default file mode	  	  */

#define _FILE_OFFSET_BITS 	64	/* for 64-bit file systems	  */ 

/*
#define	USE_FORK	    1
#define	USE_PTHREAD_JOIN    1
#define	USE_THREAD_SEMS     1
#define	USE_XFER_ASYNC	    1
#define	USE_DNS_LOOKUP	    1
#define	USE_DTS_DB	    1
*/

/* Important or sendfile/recvfile will overlap with DTSD semaphore.
*/
#define USE_SEM_IF 	    1 

#define	MAX_CLIENTS	    32		/* max no. connected clients	  */
#define	MAX_QUEUES	    16		/* max no. queues to manage	  */
#define	MAX_DIR_ENTRIES	    4096	/* max directory entries	  */
#define	MAX_EMSGS	    512		/* max error messages to save	  */


#define MIN_MULTI_FSIZE	    65536	/* smaller and we single-thread   */


#define	DTS_BSD_SUM32	    0		/* 32-bit checksum types	  */
#define	DTS_SYSV_SUM32	    1
#define	DTS_SUM32_TYPE	    DTS_SYSV_SUM32


/*  Compile-time passwd		  
 */
#define	DEF_PASSWD	    "xyzzy!"	

#define DEF_STAT_PERIOD	    60

/*  Compile-time UDT rate
 */
#define	DEF_UDT_RATE	    100		/* Megabits-per-second		  */


#define  min(a,b)   ((a)<(b)?(a):(b)) 	/* utility macros		  */
#define  max(a,b)   ((a)>(b)?(a):(b)) 
#define  abs(a)     ((a)<0?(-a):(a)) 


/* Debug and verbose flags.
 */
#define DTS_DBG    (getenv("DTS_DBG")!=NULL||access("/tmp/DTS_DBG",F_OK)==0)

#define PERF_DEBUG (getenv("PERF_DBG")!=NULL||access("/tmp/PERF_DBG",F_OK)==0)
#define SOCK_DEBUG (getenv("SOCK_DBG")!=NULL||access("/tmp/SOCK_DBG",F_OK)==0)
#define XFER_DEBUG (getenv("XFER_DBG")!=NULL||access("/tmp/XFER_DBG",F_OK)==0)
#define PTCP_DEBUG (getenv("PTCP_DBG")!=NULL||access("/tmp/PTCP_DBG",F_OK)==0)
#define IGST_DEBUG (getenv("IGST_DBG")!=NULL||access("/tmp/IGST_DBG",F_OK)==0)
#define DLVR_DEBUG (getenv("DLVR_DBG")!=NULL||access("/tmp/DLVR_DBG",F_OK)==0)
#define STAT_DEBUG (getenv("STAT_DBG")!=NULL||access("/tmp/STAT_DBG",F_OK)==0)
#define TIME_DEBUG (getenv("TIME_DBG")!=NULL||access("/tmp/TIME_DBG",F_OK)==0)
#define INIT_DEBUG (getenv("INIT_DBG")!=NULL||access("/tmp/INIT_DBG",F_OK)==0)

#define SEM_DEBUG  (getenv("SEM_DBG")!=NULL||access("/tmp/SEM_DBG",F_OK)==0)
#define CMD_DEBUG  (getenv("CMD_DBG")!=NULL||access("/tmp/CMD_DBG",F_OK)==0)

#define XFER_VERB  (getenv("XFER_VERB")!=NULL||access("/tmp/XFER_VERB",F_OK)==0)
#define PTCP_VERB  (getenv("PTCP_VERB")!=NULL||access("/tmp/PTCP_VERB",F_OK)==0)

#define	DTS_ASYNC  (getenv("DTS_ASYNC")!=NULL||access("/tmp/DTS_ASYNC",F_OK)==0)
#define DTS_NAGLE  (getenv("DTS_NAGLE")!=NULL||access("/tmp/DTS_NAGLE",F_OK)==0)
#define DTS_SUM_ALL \
	(getenv("DTS_SUM_ALL")!=NULL||access("/tmp/DTS_SUM_ALL",F_OK)==0)
#define DTS_SUM_DBG \
	(getenv("DTS_SUM_DBG")!=NULL||access("/tmp/DTS_SUM_DBG",F_OK)==0)



/**
 *  Transfer models.
 */
#define	TM_USER		    0		/* user-defined transport	  */
#define	TM_INLINE	    1		/* inline data transport	  */
#define	TM_PSOCK	    2		/* parallel socket transport	  */
#define	TM_SCP		    3		/* scp as transport		  */
#define	TM_UDT		    4		/* UDT as transport		  */


/**
 *  Transfer Context state.
 */
#define	XFER_PUSH	    0		/* push transfer model		  */
#define	XFER_PULL	    1		/* pull transfer model		  */

#define XFER_SOURCE         2           /* data source                    */
#define XFER_DEST           3           /* data sink                      */

#define	XFER_IDLE	    4		/* connection is idle		  */
#define	XFER_PENDING	    5		/* transfer is pending		  */
#define	XFER_IN_PROGRESS    6		/* transfer is in progress	  */
#define	XFER_COMPLETE	    7		/* transfer is complete		  */
#define	XFER_ABORTED	    8		/* transfer was cancelled	  */

#define XFER_TO		    XFER_PUSH	/* transfer direction		  */   
#define XFER_FROM	    XFER_PULL	/* transfer direction		  */   

#define PID_MAX             32768	/* max pid_t value [MACHDEP]	  */

#define SZ_MAX_MSG          32768
#define SZ_CONFIG	    4096	/* Sz of formatted config string  */

/**
 *  Useful values.
 */
#define MBYTE             1048576.0     /* 1024 * 1024 		  	  */
#define GBYTE           1073741824.0    /* 1024 * 1024 * 1024	  	  */
#define SELWIDTH            513		/* select() size		  */

#define SOCK_MAX_TRY        5		/* max socket retry		  */
#define SOCK_PAUSE_TIME     3		/* pause before socket retry	  */



/**
 *   Standard macro defines
 */
#ifndef	SZ_FNAME
#define	SZ_FNAME	    160
#endif
#ifndef	SZ_PATH
#define	SZ_PATH	    	    256
#endif
#ifndef	SZ_LINE
#define	SZ_LINE		    1024
#endif
#ifndef	SZ_CMD
#define	SZ_CMD		    8192
#endif
#ifndef	SZ_URL
#define	SZ_URL		    1024
#endif
#ifndef	SZ_BLOCK
#define	SZ_BLOCK	    65536
#endif
#ifndef	SZ_TSTR
#define	SZ_TSTR	    	    128
#endif
#ifndef OK				
#define OK                  0
#endif
#ifndef ERR
#define ERR                 1
#endif
#ifndef FALSE
#define FALSE               0
#endif
#ifndef TRUE
#define TRUE                1
#endif
#ifndef NO
#define NO                  0
#endif
#ifndef YES
#define YES                 1
#endif

/*  defined in $dts/include/udtc.h
typedef int 	UDTSOCKET;
*/


/**
 *  Transfer Context.  The context is used to describe the current state of
 *  a transfer request, and to hold status information while the transfer is
 *  in progress.  There is one context per connection, however since 
 */
typedef struct {
    int	   	 tmethod;		/* transport method (PSock, etc)  */
    int	   	 model;			/* push or pull model		  */
    int	   	 endpoint;		/* data source or sink		  */

    void   (*init)(void *data);		/* initialization function   	  */
    void   (*xfer)(void *data);		/* transfer function   		  */
    void   (*fini)(void *data);		/* endgame function   		  */
 
    void   (*preproc)(void *data);	/* pre-processor function   	  */
    void   (*postproc)(void *data);	/* post-processor function   	  */

    int	   	 port;			/* transfer socket (start port)	  */
    int	   	 nports;		/* no. of ports being used	  */
    int	   	 status;		/* status of transfer		  */

    long   	 fsize;			/* size of file being xfer'd 	  */
    long   	 transferred;		/* bytes xfer'd so far		  */
    float  	 tput_mb;		/* Mb/s throughput		  */
    float  	 tput_MB;		/* MB/s throughput		  */
    float  	 time;			/* transfer time in sec		  */

    int    	 valid;			/* is context valid?		  */

} xferContext, *xferContextP;


/**
 *  Transfer stats
 */
typedef struct {
    long   	fsize;			/* size of file being xfer'd 	  */

    float  	tput_mb;		/* effective Mb/s throughput	  */
    float  	tput_MB;		/* effective MB/s throughput	  */

    float  	time;			/* total transfer time in sec	  */
    int		sec;			/* total transfer time		  */
    int		usec;			/* total transfer time 		  */
    int		stat;			/* OK or ERR		  	  */
} xferStat, *xferStatP;



/**
 *  DTS client connection.
 */
typedef struct {
    int    clientPort;			/* default client port 		  */
    char   clientUrl[SZ_FNAME];		/* default client url 		  */
    char   clientHost[SZ_FNAME];	/* default client host 		  */
    char   clientDir[SZ_FNAME];		/* default client dir 		  */
    char   clientCopy[SZ_FNAME];	/* default copy dir 		  */
    char   clientIP[SZ_FNAME];		/* client IP address 		  */
    char   clientName[SZ_FNAME];	/* client name 		  	  */
    char   clientNet[SZ_FNAME];		/* client network	  	  */
    int    clientLoPort;		/* low xfer port 		  */
    int    clientHiPort;		/* high xfer port 		  */
    int    clientContact;		/* contact server port 		  */

    int    conn;			/* client connection		  */
    int    active;			/* connection active?		  */

} dtsClient, *dtsClientP;



/**
 *  DTS transfer queue.
 */

#define QUEUE_PAUSED	    0		/* queue is paused		  */
#define QUEUE_RUNNING	    1		/* queue is running		  */
#define QUEUE_ACTIVE	    2		/* queue is active		  */
#define QUEUE_WAITING	    3		/* queue is waiting for transfer  */
#define QUEUE_SHUTDOWN	    90		/* queue is shutting down. This number
					 * is high because sometimes it will
					 * block on decrement and cause it to
					 * be 89.  FIXME
					 */
#define QUEUE_INGEST	    5		/* ingest queue			  */
#define QUEUE_TRANSFER	    6		/* transfer queue		  */
#define QUEUE_ENDPOINT	    7		/* endpoint of queue		  */

#define QUEUE_NORMAL	    8		/* normal queue			  */
#define QUEUE_SCHEDULED	    9		/* schedule queue		  */
#define QUEUE_PRIORITY	    10		/* priority queue		  */

#define	QUEUE_REPLACE	    11		/* overwrite existing file	  */
#define	QUEUE_NUMBER	    12		/* renumber duplicate files	  */
#define	QUEUE_ORIGINAL	    13		/* use original name (unique?)	  */

#define	QUEUE_PUSH	    14		/* push queue transfer model	  */
#define	QUEUE_GIVE	    15		/* give queue transfer model	  */

#define QUEUE_RESPAWN       16		/* queue needs to be respawned */
#define QUEUE_KILLED	    17     	/* queue and it's thread has been
					 * terminated.
					 */
#define QUEUE_RESPAWNING    18



/**
 *  Checksum policies.
 */
#define CS_NONE         0               /* no checksum validation       */
#define CS_PACKET       1               /* packet checksum validation   */
#define CS_CHUNK        2               /* chunk checksum validation    */
#define CS_STRIPE       3               /* stripe checksum validation   */


/**
 *  Status information on the queue.
 */
typedef struct {
    char        infile[SZ_PATH];        /* current incoming file name     */
    char        outfile[SZ_PATH];       /* current outgoing file name     */
    char	curfil[SZ_FNAME];	/* currently processed filename   */

    float	avg_rate;		/* average Mbps transfer rate     */
    float	avg_size;		/* average file size (in MB)   	  */
    float	avg_time;		/* average transfer time (in sec) */
    float	tot_xfer;		/* No. of GB transferred on queue */
    float       tput_mb;    		/* throughput of last transfer    */
    int         nfiles;			/* number of files transferred 	  */
    int         pending;		/* number of files pending xfer	  */
} queueStat, *queueStatP;


/**
*  Shared memory queue struct.
*/
typedef struct{
    /*  Status summary for the queue.
     */
    int   	numflushes; 		/* number of flushes sent 	  */
    int   	canceledxfers;		/* number of canceled transfers,  */
					/* i.e for webmon poke 		  */
    int   	failedxfers;
    int   	nerrs;
    char  	msgs[40][SZ_LINE];	/* queue msg strings 	  	  */
    int   	lin_num; 		/* line number the msgs is on.	  */

    /*  Status Information for currently active transfer.
     */
    char        qname[SZ_PATH];         /* current queue name		  */
    char        infile[SZ_PATH];        /* current incoming file name     */
    char        outfile[SZ_PATH];       /* current outgoing file name     */

    size_t	xfer_size;		/* size (bytes) of xfer blob      */

    struct timeval   xfer_start;	/* start time of transfer	  */
    struct timeval   xfer_end;		/* end time of transfer	  	  */
    float	xfer_rate;		/* total transfer (Mbps)  	  */
    float	xfer_time;		/* total transfer (sec)  	  */
    int		xfer_stat;		/* transfer:  OK or ERR		  */

    struct timeval   net_start;		/* start time of net i/o	  */
    struct timeval   net_end;		/* end time of net i/o	  	  */
    float	net_rate;		/* network transfer (Mbps) 	  */
    float	net_time;		/* network transfer (sec) 	  */
    int		net_stat;		/* net xfer:  OK or ERR		  */

    struct timeval   disk_start;	/* start time of disk i/o	  */
    struct timeval   disk_end;		/* end time of disk i/o	  	  */
    float	disk_rate;		/* disk transfer (Mbps) 	  */
    float	disk_time;		/* disk transfer (sec) 	  	  */

    struct timeval   dlvr_start;	/* start time of disk i/o	  */
    struct timeval   dlvr_end;		/* end time of disk i/o	  	  */
    float	dlvr_time;		/* disk transfer (sec) 	  	  */
    int		dlvr_stat;		/* deliver:  OK or ERR		  */
} shmQueueStat;



/**
 *  Arbitrary keyword/value parameter.
 */
typedef struct {
    char name[SZ_PATH];
    char value[SZ_PATH];
} Param;

/**
 *  Structs for status information.
 */

typedef struct {
    char  	name[SZ_PATH];          /* queue name                     */
    char  	infile[SZ_PATH];        /* current incoming file name     */
    char  	outfile[SZ_PATH];       /* current outgoing file name     */
    char  	status[SZ_PATH];        /* queue status string            */
    char  	type[SZ_PATH];          /* queue type string              */
    char  	src[SZ_PATH];           /* queue's path src               */
    int   	current;                /* currently processed file       */
    int   	pending;                /* Number of pending files        */
    int   	qcount;                 /* Queue count semaphore value    */

    float  	rate;                   /* average mbs transfer rate      */
    float  	time;                   /* average transfer time (sec)    */
    float  	size;                   /* average transfer size (MB)     */
    float  	total;                  /* total transfer size (GB)	  */

    int   	nxfer;                  /* number of files transferred    */
    float  	Mbs;                    /* average Mbs transfer rate      */
    int 	canceledxfers;		/* num. of cancelled transfers	  */
    int 	numflushes;		/* num. of flushed transfers	  */
    
}  qStat, *qStatP;


typedef struct {
    char  	status[SZ_PATH];        /* node status string             */
    char  	root[SZ_PATH];          /* node's root path               */
    int   	dfree;                  /* free disk space (MB)           */
    int   	dused;                  /* used disk space (MB)           */

    int   	nerrors;                /* Number of error messgaes       */
    char       *emsgs[MAX_EMSGS];       /* error message strings          */

    int    	nqueues;		/* number of  managed queues	  */
    qStat  	queue[MAX_QUEUES];	/* Individual queue stats	  */
}  nodeStat, *nodeStatP;



/**
 *  Runtime structure for the DTS Queue Control file.
 */

#define	MAXPARAMS	64		/* max user-defined params 	  */

typedef void  (*SIGFUNC)();		/* signal handler type	  	  */

typedef struct {                        /* Control file info      	  */
    char   queueHost[SZ_PATH];          /* src host name          	  */
    char   queuePath[SZ_PATH];          /* local queue path       	  */
    char   queueName[SZ_PATH];          /* queue name             	  */
    char   deliveryName[SZ_PATH];       /* delivered name         	  */
    char   xferName[SZ_PATH];           /* file name to transfer       	  */
    char   filename[SZ_PATH];           /* file name              	  */
    char   srcPath[SZ_PATH];            /* full path at src       	  */
    char   igstPath[SZ_PATH];           /* full path at origin    	  */

    char           md5[SZ_PATH];        /* MD5 checksum           	  */
    int    	   isDir;               /* is this a directory?   	  */
    long   	   fsize;               /* file size              	  */
    mode_t 	   fmode;		/* file mode		  	  */
    unsigned int   sum32;               /* 32-bit checksum        	  */
    unsigned int   crc32;               /* 32-bit CRC value       	  */
    time_t 	   epoch;               /* epoch (sec) at origin  	  */

    Param  params[MAXPARAMS];           /* user-supplied params   	  */
    int    nparams;			/* number of params	  	  */
} Control;


/**
 *  Main queue descriptor.
 */
typedef struct {
    char        name[SZ_FNAME];		/* queue name			  */
    char	src[SZ_FNAME];		/* queue source name		  */
    char	dest[SZ_FNAME];		/* queue destination name	  */
    char	deliveryDir[SZ_PATH];	/* delivery directory		  */
    char	deliveryCmd[SZ_FNAME];	/* delivery command		  */
    char	deliverAs[SZ_FNAME];	/* delivery filename		  */
    char	transfer[SZ_FNAME];	/* transfer queues		  */

    int         type;			/* type of queue (sched, etc)     */
    int         node;			/* route node type (ingest, etc)  */
    int         mode;			/* transfer mode		  */
    int         method;			/* transfer method		  */
    int         nthreads;		/* no. transfer threads		  */
    int         port;			/* start transfer port		  */
    int         keepalive;		/* keep connections open?	  */
    int         deliveryPolicy;		/* existing file policy		  */
    int	 	auto_purge;		/* auto purge the spool dir	  */
    int         checksumPolicy;		/* checksum policy		  */
    
    int		activeSem;		/* queue is active semaphore	  */
    int		countSem;		/* queue count semaphore	  */
    pthread_t	qm_tid;			/* queue manager thread id	  */
    
    int         status;			/* queue status			  */
    int		udt_rate;		/* UDT transfer rate (Mbps)	  */
    time_t	interval;		/* interval for scheduled queues  */
    time_t	stime;			/* start time of interval	  */

    struct timeval init_time;		/* start time of init transfer    */
    struct timeval end_time;		/* start time of end transfer	  */

    char        infile[SZ_PATH];        /* current incoming file name     */
    char        outfile[SZ_PATH];       /* current outgoing file name     */

    xferContext context;		/* connection context		  */

    queueStat 	stats;			/* queue processing stats	  */
    Control 	ctrl;			/* outgoing control file          */
    
    int		nerrs;			/* number of errors on queue	  */
    
    /*
    int 	oldest;          	// keep track of oldest line	  //
    int  	msgline;        	// keep track of message line	  //
    char       *msgs[10];	 	// queue activity msg strings	  //
    */
    
    char       *emsgs[MAX_EMSGS];	/* error msg strings 	  	  */

    pthread_mutex_t mutex;		/* thread mutex lock 		  */

    void	*dts;			/* back pointer			  */
    shmQueueStat *qstat; 		/* shared memory pointer */
} dtsQueue, *dtsQueueP;


/**
 *  Runtime structure for the DTS daemon.
 */
typedef struct {
    char        whoAmI[SZ_LINE];	/* local name			  */
    char        passwd[SZ_LINE];	/* local password		  */
    char        ops_pass[SZ_LINE];	/* operations password		  */

    char        serverName[SZ_FNAME];	/* server name 		  	  */
    char        serverRoot[SZ_PATH];	/* sandbox root dir		  */
    int         serverPort;		/* default server port 		  */
    char        serverUrl[SZ_LINE];	/* default server url 		  */
    char        serverHost[SZ_FNAME];	/* default server host 		  */
    char        serverIP[SZ_FNAME];	/* server IP address 		  */
    int         contactPort;		/* contact server port 		  */

    int         loPort;			/* low transfer port 		  */
    int         hiPort;			/* high transfer port 		  */
    int	 	semId;			/* semaphore starting ID	  */

    char        configFile[SZ_FNAME];	/* DTS config file		  */
    char        workingDir[SZ_LINE];	/* default working directory	  */

    char        dbFile[SZ_FNAME];	/* database file name		  */
    char        logFile[SZ_FNAME];	/* log file name		  */
    FILE	*logfd;			/* log file descriptor		  */

    dtsClient   *clients[MAX_CLIENTS];	/* connected clients		  */
    dtsClient   *self;			/* server's client descriptor	  */
    int         nclients;		/* no. of connected clients	  */
    int         nnetworks;		/* no. of defined networks	  */

    dtsQueue    *queues[MAX_QUEUES];	/* managed queues		  */
    int         nqueues	;		/* no. of connected clients	  */

    int		nerrs;			/* number of errors	  	  */
    char       *emsgs[MAX_EMSGS];	/* error msg strings 	  	  */

    char        monitor[SZ_FNAME];	/* monitor address 		  */
    char        mon_url[SZ_FNAME];	/* monitor URL  		  */
    int		mon_fd;			/* monitor client descriptor	  */
    int		mon_port;		/* monitor connection port	  */
    int 	stat_period; 		/* stat timer wait period (sec)	  */
    pthread_t 	stat_tid; 		/* stat timer thread id 	  */

    int	 	daemonize;		/* run as daemon		  */
    int	 	verbose;		/* verbose flag			  */
    int	 	debug;			/* debug flag			  */
    int	 	copyMode;		/* copyMode flag		  */
    int	 	trace;			/* trace flag			  */
    int	 	shutdown;		/* shutdown flag		  */
    
    time_t 	start_time; 		/* start time for calculating the
					 * up time of the node
					 */
    char build_date[SZ_FNAME];		/* build date from <build.h> 	  */

} DTS, *DTSP;



/**
 *  Public method prototypes.
 */

/* dts.c
*/
DTS    *dtsInit (char *config, int port, char *host, char *root, int init);
void    dts_initServerRoot (DTS *dts);
void 	dts_initCopyMode (DTS *dts, char *host, int port);

void    dtsFree (DTS *dts);
void    dts_connectToMonitor (DTS *dts);

void    dtsErrorExit (char *msg);
void    dtsErrorWarn (char *msg);


/*  dtsASync.c 
*/
int 	dts_nullResponse(void *data);


/*  dtsChecksum.c
*/
char   *dts_fileMD5 (char *fname);
uint    dts_fileCRCChecksum (char *fname, unsigned int *crc);
uint    dts_fileChecksum (char *fname, int do_sysv);
uint    dts_fileCRC32 (char *fname);
int     dts_fileValidate (char *fname, uint sum32, uint crc, char *md5);

char   *dts_memMD5 (unsigned char *buf, size_t len);
uint    dts_memChecksum (unsigned char *buf, size_t len, int do_sysv);
uint    dts_memCRC32 (unsigned char *buf, size_t len);

void    checksum (unsigned char *data, int length, ushort *sum16, uint *sum32);
uint    addcheck32 (unsigned char *array, int length);


/*  dtsConfig.c 
*/
void 	dts_loadConfigStr (DTS *dts, char *cfg, char *host, int port, 
		char *root, int init);
void 	dts_loadConfigDir (DTS *dts, char *cfg, char *host, int port, 
		char *root, int init);
void 	dts_loadConfigFile (DTS *dts, char *cfg, char *host, int port, 
		char *root, int init);
int 	dts_loadDefaultConfig (DTS *dts, char *host, int port);
int     dts_getQPort (DTS *dts, char *qname, int tport);
void    dts_printQPorts (DTS *dts);
int     dts_validateQueue (DTS *dts, dtsQueue *dtsq);
dtsQueue *dts_newQueue (DTS *dts);

int     dts_validPasswd (DTS *dts, char *passwd);
char   *dts_nameToHost (DTS *dts, char *name);
char   *dts_nameToIP (DTS *dts, char *name);
char   *dts_cfgNameToHost (DTS *dts, char *name);
char   *dts_cfgNameToIP (DTS *dts, char *name);
char   *dts_getLocalRoot (void);
char   *dts_getLocalHost (void);
char   *dts_getLocalHostIP (void);
char   *dts_getAliasHost (char *alias);
char   *dts_getAliasDest (char *alias);
int     dts_getAliasPort (char *alias);
int     dts_getNumNetworks (DTS *dts);
int 	dts_cfgQType (char *s);
int 	dts_cfgQNode (char *s);
int 	dts_cfgQMethod (char *s);
int 	dts_cfgQMode (char *s);
char   *dts_cfgQTypeStr (int type);
char   *dts_cfgNodeeStr (int node);
char   *dts_cfgQMethodStr (int method);
char   *dts_cfgQModeStr (int mode);
char   *dts_cfgPath (void);
time_t  dts_cfgInterval (char *intstr);
time_t  dts_cfgStartTime (char *tstr);

void    dts_printConfig (DTS *dts);
char   *dts_fmtConfig (DTS *dts);


/*  dtsConsole.c 
*/
int 	dts_monAttach (void *data);
int 	dts_monDetach (void *data);
int 	dts_monConsole (void *data);


/*  dtsCommands.c 
*/
int 	dts_hostAccess (char *host, char *path, int mode);
char   *dts_hostCat (char *host, char *fname);
int     dts_hostChecksum (char *host, char *fname, unsigned int *sum32,
               		     unsigned int *crc, char **md5);
int 	dts_hostCopy (char *host, char *old, char *new);
int     dts_hostChmod (char *host, char *path , mode_t mode);
char   *dts_hostCwd (char *host);
char   *dts_hostCfg (char *host, int quiet);
int     dts_hostDel (char *host, char *path, char *passwd, int recursive);
char   *dts_hostDestDir (char *host, char *qname);
char   *dts_hostDir (char *host, char *path, int lsLong);
int     dts_hostIsDir (char *host, char *path);
int 	dts_hostDiskUsed (char *host, char *path);
int 	dts_hostDiskFree (char *host, char *path);
char   *dts_hostEcho (char *host, char *str);
int     dts_hostFGet (char *host, char *fname, char *local, int blk);
long    dts_hostFSize (char *host, char *path);
long    dts_hostFMode (char *host, char *path);
int     dts_hostFTime (char *host, char *path, char *mode);
int 	dts_hostMkdir (char *host, char *path);
int 	dts_hostPing (char *host);
int 	dts_hostPingSleep (char *host, int time);
char   *dts_hostPingStr (char *host);
int 	dts_hostPingArray (char *host);
int 	dts_hostPoke (char *host);
int 	dts_hostRemotePing (char *local, char *remote);
int 	dts_hostRename (char *host, char *old, char *new);
int 	dts_hostSetRoot (char *host, char *path);
int 	dts_hostSetDbg (char *host, char *flag);
int     dts_hostSvrPing (char *host, int time);
int 	dts_hostTouch (char *host, char *path);
char   *dts_hostDest (char *host, char *qname);
char   *dts_hostSrc (char *host, char *qname);
int     dts_hostPauseQueue (char *host, char *qname);
int     dts_hostPokeQueue (char *host, char *qname);
int     dts_hostFlushQueue (char *host, char *qname);
int     dts_hostStartQueue (char *host, char *qname);
int     dts_hostShutdownQueue (char *host, char *qname);
char   *dts_hostListQueue (char *host, char *qname);
char   *dts_hostPrintQueueCfg (char *host, char *qname);
int     dts_hostGetQueueStat (char *host, char *qname);
int     dts_hostSetQueueStat (char *host, char *qname, int val);
int     dts_hostSetQueueCount (char *host, char *qname, int count);
int     dts_hostGetQueueCount (char *host, char *qname);
int     dts_hostSetQueueDir (char *host, char *qname, char *dir);
char   *dts_hostGetQueueDir (char *host, char *qname);
int     dts_hostSetQueueCmd (char *host, char *qname, char *cmd);
char   *dts_hostGetQueueCmd (char *host, char *qname);
char   *dts_hostGetCopyDir (char *host);
int     dts_hostExecCmd (char *host, char *ewd, char *cmd);
/*
int     dts_hostAddToQueue (char *host, char *qname, char *fname);
int     dts_hostDelFromQueue (char *host, char *qname, char *fname);
*/


int 	dts_hostPush (char *host, int cmdPort, int argc, char *argv[]);
int 	dts_hostPull (char *host, int cmdPort, int argc, char *argv[]);

unsigned char *dts_hostRead(char *host, char *path, int off, int sz, int *nb);
int 	dts_hostWrite (char *host, char *path, int off, long sz, char *data);
int 	dts_hostPrealloc (char *host, char *path, long size);
int 	dts_hostStat (char *host, char *path, struct stat *st);
int 	dts_hostStatVal (char *host, char *path, char *val);

int 	dts_hostAbort (char *host, char *passwd);
int 	dts_hostContact (char *host);
int 	dts_hostShutdown (char *host, char *passwd);
int 	dts_hostSet (char *host, char *class, char *key, char *val);
char   *dts_hostGet (char *host, char *class, char *key);
int 	dts_hostUpStats (char *host, char *qname, xferStat *xfs);
int 	dts_hostNodeStat (char *host, char *qname, int errs, nodeStat *ns);
int 	dts_hostSubmitLogs (char *host, char *qname, char *log, char *recover);
char   *dts_hostGetQLog (char *host, char *qname, char *fname);
int 	dts_hostEraseQLog (char *host, char *qname, char *fname);

int     dts_isLocal (char *host);
void    dts_cmdInit (void);
int	dts_getClient (char *host);
int	dts_closeClient (int client);


/*  dtsDeliver.c 
*/
char   *dts_Deliver (dtsQueue *dtsq, Control *ctrl, char *fname, int *stat);
char   *dts_getDeliveryPath (dtsQueue *dtsq, Control *ctrl);
void    dts_loadDeliveryParams (Control *ctrl, char *pfname);
int     dts_testDeliveryDir (char *dpath, int create, char *msg);
int     dts_validateDelivery (Control *ctrl, char *dpath);
int     dts_addControlHistory (Control *ctrl, char *msg);

int     dts_sysExec (char *ewd, char *cmd);
int     dts_Await (int waitpid);
void    dts_Interrupt (int value);
void	dts_Enbint (SIGFUNC handler);


/*  dtsIngest.c 
*/
char   *dts_Ingest (dtsQueue *dtsq, Control *ctrl, char *fname, 
		char *cpath, int *stat);


/*  dtsLocal.c 
*/
int     dts_localAccess (char *path, int mode);
int     dts_localCopy (char *old, char *new);
char   *dts_localCwd ();
int     dts_localDelete (char *path, int recurse);
char   *dts_localDir (char *path, int lsLong);
int     dts_localChmod (char *path, mode_t mode);
int     dts_localMkdir (char *path);
int     dts_localMtime (char *path, long mtime);
int     dts_localRename (char *old, char *new);
int     dts_localTouch (char *path);
int     dts_localPrealloc (char *path, int size);


/*  dtsLog.c 
*/
void	dtsLog (DTS *dts, char *format, ...);
void	dtsLogMsg (DTS *dts, int key, char *format, ...);
void    dtsLogStat (DTS *dts, char *msg);
void	dtsErrLog (dtsQueue *dtsq, char *format, ...);
void	dtsTimeLog (char *format, struct timeval t1);
void    dts_encodeString (char *buf, char *format, va_list *argp);

void    dtsErrorExit (char *msg);
void    dtsErrorWarn (char *msg);

int     dts_cfgQType (char *s);
int     dts_cfgQMode (char *s);
int     dts_cfgQMethod (char *s);
char   *dts_cfgQTypeStr (int type);
char   *dts_cfgQMethodStr (int method);
char   *dts_cfgQModeStr (int mode);
char   *dts_cfgQNodeStr (int mode);
 

/*  dtsSem.c
*/
int 	dts_semInit (int id, int initVal);
int 	dts_semRemove (int id);
int 	dts_semSetVal (int id, int val);
int 	dts_semGetVal (int id);
int 	dts_semDecr (int id);
int 	dts_semIncr (int id);
void 	dts_semInitId (int id);
 

/*  dtsUtil.c
*/
void    dtsShm(DTS * mydts);//dtsQueue * mydtsq);
void    dtsDaemonize (DTS *dts);
void    dts_sigHandler (int sig);
void    dts_setQueueShutdown (int level);

char   *dts_localTime ();
char   *dts_UTTime ();

char 	*dts_getLocalIP ();
char 	*dts_getLocalHost ();
char    *dts_resolveHost (char *hostname);
void	 dts_pause (void);
char    *dts_strbuf (char *s);
struct hostent *dts_getHostByName (char *name);
struct hostent *dts_dupHostent (struct hostent *hentry);
void	 dts_printHostTable (void);

char    *dts_logtime (void);
void     dts_tstart (struct timeval *tv);
double   dts_tstop (struct timeval tv);
double   dts_timediff (struct timeval t1, struct timeval t2);
void     measure_start (void);
void     measure_stop (long transferred);
double   transferMb (long fileSize, int sec, int usec);
double   transferMB (long fileSize, int sec, int usec);

char    *dts_fmtMode (char *lp, int flags);
int   	 dts_patMatch (char *str, char *pattern);
int   	 dts_isTemplate (char *s);

char    *dtsGets (char *s, int len, FILE *fp);

void     dtsError (char *msg);
int	 dts_debugLevel (void);

char    *dts_printDTS (DTS *dts, FILE *fd);
char    *dts_printDTSClient (DTS *dts, FILE *fd);
char    *dts_printAllQueues (DTS *dts, FILE *fd);
char    *dts_printQueue (dtsQueue *dtsq, FILE *fd);
void     doprnt (char *buf, char *format, ...);
int 	 dtsSleep (unsigned int seconds);

int 	 base64_encode (unsigned char *source, size_t sourcelen, char *target,
                size_t targetlen);
int 	 base64_decode (char *source, unsigned char *target, size_t targetlen);


/*  dtsXfer.c 
*/
int 	dts_hostTo (char *host, int cmdPort, int method, int rate,
		int loPort, int hiPort, int threads, int mode, 
		int argc, char *argv[], xferStat *xfs);
int 	dts_hostFrom (char *host, int cmdPort, int method, int rate,
		int loPort, int hiPort, int threads, int mode,
		int argc, char *argv[], xferStat *xfs);
int     dts_xferDirTo (char *host, char *func, int method, int rate, 
		char *path, char *root, char *prefix, xferStat *xs);
int     dts_xferDirFrom (char *srcHost, char *dstHost, int dstLocal,
		char *func, int method, int rate, char *path, char *root, 
		char *prefix, xferStat *xs);
int     dts_xferFile (char *host, char *func, int method, int rate, char *path, 
		int fsize, mode_t fmode, xferStat *xs);

int 	dts_xferParseArgs (int argc, char *argv[], int *nthreads, int *port,
        	int *verbose, char **path_A, char **path_B);
int 	dts_xferParsePaths (char *host, char **path_A, char **path_B,
        	int *srcLocal, int *dstLocal, char **src_host, char **dest_host,
		char **msg_host, char **path);

/*  dtsPush.c
*/
int 	dts_xferPushFile (void *data);
int 	dts_xferReceiveFile (void *data);


/*  dtsPull.c
*/

int 	dts_xferPullFile (void *data);
int 	dts_xferSendFile (void *data);


/*  dtsFileUtil.c
*/
FILE   *dts_fopen (char *fname, char *mode);
int     dts_fclose (FILE *fp);
int     dts_fileOpen (char *fname, int flags);
int     dts_fileClose (int fd);
int     dts_fileSync (int fd);
int     dts_preAlloc (char *fname, long fsize);
long    dts_fileSize (int fd);
long    dts_fileMode (int fd);
long    dts_nameSize (char *fname);
long    dts_nameMode (char *fname);
int     dts_getBuffer (int fd, unsigned char **buf, long chunkSize, int tnum);
int     dts_fileRead (int fd, void *vptr, int nbytes);
int     dts_fileWrite (int fd, void *vptr, int nbytes);
int     dts_fileCopy (char *in, char *out);
int 	dts_isDir (char *path);
int 	dts_isFile (char *path);
int 	dts_isLink (char *path);
char   *dts_readLink (char *path);
int	dts_makePath (char *path, int isDir);
char   *dts_pathDir (char *path);
char   *dts_pathFname (char *path);
long 	dts_du (char *filename);
int	dts_unlink (char *dir, int recurse, char *template);
long	dts_dirSize (char *dir, char *template);
int     dts_dirCopy (char *in, char *out);
int     dts_statfs (char *dir, struct statfs *fs);


/*  dtsSandbox.c
*/
char   *dts_sandboxPath (char *in);


/*  dtsSockUtil.c
*/
int 	dts_openServerSocket (int port);
int 	dts_testServerSocket (int port);
int 	dts_getOpenPort (int port, int maxTries);
int 	dts_openClientSocket (char *host, int port, int retry);

int 	dts_openUDTServerSocket (int port, int rate);
int 	dts_openUDTClientSocket (char *host, int port, int retry);

int 	dts_sockRead (int fd, void *vptr, int nbytes);
int 	dts_sockWrite (int fd, void *vptr, int nbytes);
void	dts_setBlock (int sock);
void	dts_setNonBlock (int sock);
int 	dts_udtRead (int fd, void *vptr, long nbytes, int flags);
int 	dts_udtWrite (int fd, void *vptr, long nbytes, int flags);


/*  dtsTar.c
*/
int	dts_wtar(int nargc, char *nargv[]);
int	dts_rtar (int nargc, char *nargv[]);


/*  dtsQueue.c
*/
int   	dts_hostInitTransfer (char *host, char *qname, char *fname, char *msg);
int   	dts_hostEndTransfer (char *host, char *qname, char *qpath);
int   	dts_hostQueueAccept (char *host, char *qname, char *fname, char *msg);
int   	dts_hostQueueComplete (char *host, char *qname, char *qpath);
int   	dts_hostQueueRelease (char *host, char *qname);
int   	dts_hostQueueValid (char *host, char *qname);
int   	dts_hostSetQueueControl (char *host, char *qname, Control *ctrl);


/*  dtsQueueUtil.c
*/
dtsQueue  *dts_queueLookup (char *qname);
int  	   dts_queueGetCurrent (char *fname);
void       dts_queueSetCurrent (char *fname, int val);
int  	   dts_queueGetNext (char *fname);
void       dts_queueSetNext (char *fname, int val);
void  	   dts_queueGetInitTime (dtsQueue *dtsq, int *sec, int *usec);
void       dts_queueSetInitTime (dtsQueue *dtsq, struct timeval t);
void	   dts_queueCleanup (DTS *dts, dtsQueue *dtsq);
Control   *dts_loadControl (char *cpath, Control *ctrl);
int        dts_logControl (Control *ctrl, int stat, char *path);
int        dts_logXFerStats (Control *ctrl, xferStat *xfs, int stat, char *path);
void       dts_printControl (Control *ctrl, FILE *fd);
int        dts_saveControl (Control *ctrl, char *path);
char      *dts_fmtQueueCmd (dtsQueue *dtsq, Control *ctrl);
char      *dts_getQueuePath (DTS *dts, char *qname);
char      *dts_getQueueLog (DTS *dts, char *qname, char *logname);
char      *dts_getNextQueueDir (DTS *dts, char *qname);
char      *dts_verifyDTS (char *host, char *qname, char *fname);
int        dts_queueInitControl(char *host, char *qname, char *qpath, 
			char *opath, char *lfname, char *fname, char *dfname);
int        dts_queueProcess (dtsQueue *dtsq, char *lpath, char *rpath, 
			char *fname);
char      *dts_queueFromPath (char *qpath);
char      *dts_queueNameFmt (char *qname);
void       dts_queueDelete (DTS *dts, char *qpath);
void       dts_queueLock (dtsQueue *dtsq);
void       dts_queueUnlock (dtsQueue *dtsq);

int        dts_logXFerStats (Control *ctrl, xferStat *xfs, int stat,char *path);
int 	   dts_queueSetStats (dtsQueue *dtsq, xferStat *xfs);
queueStat *dts_queueGetStats (dtsQueue *dtsq);


/*  dtsQueueStat.c
 */
void	dts_qstatInit (char *qname, char *fname, size_t size);
void	dts_qstatSetFName (char *qname, char *fname);
void	dts_qstatSetSize (char *qname, size_t size);

void	dts_qstatStart (char *qname);
void	dts_qstatEnd (char *qname);
void	dts_qstatNetStart (char *qname);
void	dts_qstatNetEnd (char *qname);
void	dts_qstatDlvrStart (char *qname);
void	dts_qstatDlvrEnd (char *qname);
void	dts_qstatDiskStart (char *qname);
void	dts_qstatDiskEnd (char *qname);

void 	dts_qstatXferStat (char *qname, int stat);
void 	dts_qstatNetStat (char *qname, int stat);
void 	dts_qstatDlvrStat (char *qname, int stat);

void	dts_qstatSummary (DTS *dts, char *qname);
void	dts_qstatPrint (char *qname);
