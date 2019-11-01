/** 
 *  XRUTIL.C
 *
 *  Utility procedures.
 *
 *
 *  @brief      Utility procedures.
 *
 *  @file       xrUtil.c
 *  @author     Mike Fitzpatrick
 *  @date       6/10/09
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>

#include <xmlrpc-c/base.h>
#include <xmlrpc-c/client.h>
#include <xmlrpc-c/server.h>
#include <xmlrpc-c/server_abyss.h>

#include "xrpcP.h"


#define  DEBUG		0
#define  ABORT_DEBUG	1
#define  SHMSZ		27


extern	int  client_verbose;


/** ************************************************************************
 *  
 */

pthread_mutex_t stat_mutex = PTHREAD_MUTEX_INITIALIZER;

static int xr_stat_sem = 0;

void
xr_semSet (int statsem)
{
    xr_stat_sem = statsem;
}


void
xr_setStat (char stat)
{
    int lock;
    struct sembuf oper0;


    if (xr_stat_sem != 0) {
        if (stat == 1) {
            lock = pthread_mutex_lock (&stat_mutex);
	    oper0.sem_op = 1;
	    oper0.sem_flg = 0;
	    oper0.sem_num  = 0;
	    if (semop (xr_stat_sem, &oper0, 1) == -1) {
		if (DEBUG) fprintf (stderr, "error incrementing semaphore \n");
		if (ABORT_DEBUG)
		    abort ();
		else
		    exit (1);
	    }
            lock = pthread_mutex_unlock (&stat_mutex);

        } else if (stat == 2) {
            lock = pthread_mutex_lock (&stat_mutex);
            oper0.sem_op = 1;
    	    oper0.sem_flg = 0;
    	    oper0.sem_num  = 1;
    	    if (semop (xr_stat_sem, &oper0, 1) == -1) {
        	if (DEBUG) fprintf (stderr, "error incrementing semaphore \n");
		if (ABORT_DEBUG)
		    abort ();
		else
        	    exit (1);
    	    }
            lock = pthread_mutex_unlock (&stat_mutex);
        }
    }
                
    return;
}


void
xr_getStat (xr_dtsstat *passedstat)
{
    int lock;

    if (xr_stat_sem != 0) {
        lock = pthread_mutex_lock (&stat_mutex);
        passedstat->NumCallSync = semctl (xr_stat_sem, 0, GETVAL, 0);
        passedstat->NumMethod = semctl (xr_stat_sem, 1, GETVAL, 0);
        lock = pthread_mutex_unlock (&stat_mutex);
    }
    return;
}


/** ************************************************************************
 *  SIGTERM HANDLERS -- Setup the SIGTERM handler.
 */
xmlrpc_server_abyss_t *serverToTerminateP;

void
xr_setupSigtermHandler (xmlrpc_server_abyss_t *serverP)
{
    struct sigaction mysigaction;

    sigemptyset (&mysigaction.sa_mask);
    mysigaction.sa_flags = 0;
    mysigaction.sa_handler = xr_svrSigtermHandler;
    sigaction (SIGTERM, &mysigaction, NULL);
}

void
xr_svrSigtermHandler (int signalClass)
{
    xmlrpc_env env;

    xmlrpc_env_init (&env);
    xmlrpc_server_abyss_terminate (&env, serverToTerminateP);
    xr_dieIfFailed ("xmlrpc_server_abyss_terminate", env);

    xmlrpc_env_clean (&env);
}

void
xr_restoreSigtermHandler (void)
{
    struct sigaction mysigaction;

    sigemptyset (&mysigaction.sa_mask);
    mysigaction.sa_flags = 0;
    mysigaction.sa_handler = SIG_DFL;
    sigaction (SIGTERM, &mysigaction, NULL);
}



/** ************************************************************************
 *  SIGPIPE HANDLERS -- Setup the SIGPIPE handlers.
 */
void
xr_setupSigpipeHandlers (void)
{
    struct sigaction mysigaction;

    sigemptyset (&mysigaction.sa_mask);
    mysigaction.sa_flags = 0;
    mysigaction.sa_handler = SIG_IGN;
    sigaction (SIGPIPE, &mysigaction, NULL);
}



/** ************************************************************************
 *  Utility procedures.
 */

/**
 *  XR_GETPEERIPADDR -- Get the IP addr of the host on the server channel.
 */
char *
xr_getPeerIpAddr (TSession * const abyssSessionP)
{
    struct abyss_unix_chaninfo * channelInfoP;
    struct sockaddr_in * sockAddrInP;
    static char buf[32];
    static unsigned char *ipAddr;


    SessionGetChannelInfo (abyssSessionP, (void*)&channelInfoP);

    sockAddrInP = (struct sockaddr_in *) &channelInfoP->peerAddr;
    ipAddr      = (unsigned char *)&sockAddrInP->sin_addr.s_addr;

    memset (buf, 0, 32);
    sprintf (buf, "%u.%u.%u.%u", ipAddr[0], ipAddr[1], ipAddr[2], ipAddr[3]);

    return (buf);
}


void
xr_dieIfFailed (char *description, xmlrpc_env env)
{
    if (env.fault_occurred) {
        fprintf (stderr, "%s failed. %s\n", description, env.fault_string);
	if (ABORT_DEBUG)
	    abort ();
	else
            exit (1);
    }
}


void die_on_error (xmlrpc_env *env) 
{
    if (env->fault_occurred) {
        fprintf(stderr, 
	    "XML-RPC Fault: %s (%d)\n", env->fault_string, env->fault_code);
	if (ABORT_DEBUG)
	    abort ();
	else
	    exit (1);
    }
}


void warn_on_error (xmlrpc_env *env) 
{
    if (env->fault_occurred && client_verbose)
        fprintf(stderr, 
	    "XML-RPC Fault: %s (%d)\n", env->fault_string, env->fault_code);
}


void
xr_dbgPrintParams ( xmlrpc_server_abyss_parms s )
{
    fprintf (stderr, "Server Params:\n");
    fprintf (stderr, "\tport = %d\n", s.port_number);
    fprintf (stderr, "\tlogfile = %s\n", s.log_file_name);
    fprintf (stderr, "\tkeepalive_timeout = %d\n", s.keepalive_timeout);
    fprintf (stderr, "\tkeepalive_max_conn = %d\n", s.keepalive_max_conn);
    fprintf (stderr, "\ttimeout = %d\n", s.timeout);
}

