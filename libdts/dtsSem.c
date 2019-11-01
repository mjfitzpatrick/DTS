/**
 *  DTSSEM.C -- DTS utilities to manage semaphores easily.
 *
 *  @file       dtsSem.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/10/09
 *
 *  @brief  DTS utilities to manage semaphores easily.
 */


/*****************************************************************************/

#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <semaphore.h>
#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

#include "dts.h"


/* Make sure what we need is defined.
 */
#ifndef SEM_STAT
#define SEM_STAT        18
#define SEM_INFO        19
#endif

/* Some versions of libc only define IPC_INFO when __USE_GNU is defined. */
#ifndef IPC_INFO
#define IPC_INFO        3
#endif



/*  FIXME -- Currently blocks in a bad way
#define USE_MUTEX	1
*/
#define USE_SEMOP	1

#ifdef USE_MUTEX
pthread_mutex_t sem_mut = PTHREAD_MUTEX_INITIALIZER;
#endif

/* Sem ID.
 */
static int semId 	= 10250;



#if defined (__GNU_LIBRARY__) && !defined(_SEM_SEMUN_UNDEFINED)
/* union semun is defined by including <sys/sem.h> */
#else
#ifndef Darwin
/* we have to define it ourselves */
union semun {
    int val;
    struct semid_ds *buf;
    unsigned short int *array;
    struct seminfo *__buf;
};
#endif
#endif

extern DTS *dts;

#ifdef USE_STATIC_SEMOP
static struct sembuf sem_op;
#endif


#ifdef UNIT_TEST
int 
main (int argc, char *argv[])
{
    int   id, val = 0, stat = 0;


    if (argc > 1 && strcmp (argv[1], "-r") == 0) {
	/* Reset the semaphore. */
        if ((stat = dts_semIncr (atoi(argv[2]))) == OK)
	    fprintf (stderr, "sem reset incr=1   stat=%d\n", stat);
	return (OK);
    }



    if ((id = dts_semInit ((int)getuid(), 0)) < 0) {
	fprintf (stderr, "semInit failed\n");
	exit (1);
    }

    if (dts_semSetVal (id, 3) == OK)
	fprintf (stderr, "sem setval=3 OK\t");
    fprintf (stderr, "sem getval = %d\n", dts_semGetVal (id));

    if (dts_semIncr (id) == OK)
	fprintf (stderr, "sem incr OK\t");
    fprintf (stderr, "sem getval = %d\n", dts_semGetVal (id));

    if (dts_semDecr (id) == OK)
	fprintf (stderr, "sem decr OK\t");
    fprintf (stderr, "sem getval = %d\n", dts_semGetVal (id));


    while ((val = dts_semDecr (id)) >= 0) {
	fprintf (stderr, 
	    "sem loop decr OK, ret=%d  val = %d\n", val, dts_semGetVal(id));
  	dtsSleep (1);
    }

    if (dts_semRemove (id) == OK)
	fprintf (stderr, "sem removal OK\n");

    return (0);
}
#endif



/**
 *  Create and/or initialize the semaphore.
 */
int
dts_semInit (int id, int initVal)
{
    int    sem_id = -1;
#ifdef USE_SEM_IF
    int	   sid = id + 10250;
#else
    int	   sid = semId++;
#endif


    if (SEM_DEBUG)
	fprintf (stderr, "semInit: id = %d  initVal = %d\n", id, initVal);

    if (sid <= 0)
	return (-1);

    /*  Check to see if it already exists, if so initialize the value to zero.
     */
    if (semctl (sid, 0, GETVAL, 0) < 0) {
	if (errno == EINVAL) {
            /*  Create a semaphore set with the specified id and access
             *  only by the owner.
             */
            if ((sem_id = semget (sid, 1, IPC_CREAT | 0600)) < 0) {
		fprintf (stderr, "semInit: new sid = %d  failed '%s'\n", 
		    id, strerror(errno));
	        return (-1);
	    }

	    if (SEM_DEBUG)
		fprintf (stderr, "new semaphore: %d [%d]\n", sem_id, sid);
	} else
	    sem_id = sid;

        /*  Initialize the value. We allow the caller to pass in a negative
	 *  value to create the semaphore without initializing it.
	 */ 
        dts_semSetVal (sem_id, initVal);

        return (sem_id);

    } else {
        /*  Semaphore exists, just initialize the value. 
	 */ 
	if (SEM_DEBUG)
	    fprintf (stderr, "existing semaphore: %d\n", sid);
        dts_semSetVal (sid, initVal);
        return (sid);
    }
}


/**
 *  Destroy the semaphore.
 */
int
dts_semRemove (int id)
{
    return ( (semctl (id, 0, IPC_RMID, 0) < 0) ? -1 : 0 );
}


/**
 *  Set the value.
 */
int
dts_semSetVal (int id, int val)
{
    return ( (semctl (id, 0, SETVAL, val) < 0) ? -1 : 0 );
}


/**
 *  Get the value.
 */
int
dts_semGetVal (int id)
{
    return (semctl (id, 0, GETVAL, 0));
}


#ifdef USE_SEMOP

/**
 *  Decrement the semaphore value.
 */
int
dts_semDecr (int id)
{
    int  stat = 0;
    struct sembuf sem_op;


    sem_op.sem_num = 0;
    sem_op.sem_op  = -1;
    sem_op.sem_flg = SEM_UNDO;

    if (SEM_DEBUG)
	fprintf (stderr, "semop semDecr:  id=%d\n", id);

#ifdef USE_MUTEX
    if (SEM_DEBUG)
	fprintf (stderr, "semop semDecr:  mutex lock\n");
    pthread_mutex_lock (&sem_mut);
#endif

        
    if (dts_semGetVal (id) > 0) {
#ifdef UNIT_TEST
        if ((stat = semop (id, &sem_op, 1)) < 0)
#else
        if ((stat = semop (id, &sem_op, 1)) < 0 && dts->verbose > 2)
#endif
	    fprintf (stderr, "sem %d decr failed\n", id);
    }

#ifdef USE_MUTEX
    if (SEM_DEBUG)
	fprintf (stderr, "semop semDecr:  mutex unlock\n");
    pthread_mutex_unlock (&sem_mut);
#endif

    return (stat);
}


/**
 *  Increment the semaphore value.
 */
int
dts_semIncr (int id)
{
    int  stat;
    struct sembuf sem_op;

    sem_op.sem_num = 0;
    sem_op.sem_op  = 1;
    sem_op.sem_flg = SEM_UNDO;

    if (SEM_DEBUG)
	fprintf (stderr, "semop semIncr:  id=%d\n", id);

#ifdef USE_MUTEX
    if (SEM_DEBUG)
	fprintf (stderr, "semop semIncr:  mutex lock\n");
    pthread_mutex_lock (&sem_mut);
#endif

#ifdef UNIT_TEST
    if ((stat = semop (id, &sem_op, 1)) < 0)
#else
    if ((stat = semop (id, &sem_op, 1)) < 0 && dts->verbose > 2)
#endif
	fprintf (stderr, "sem %d incr failed\n", id);

#ifdef USE_MUTEX
    if (SEM_DEBUG)
	fprintf (stderr, "semop semIncr:  mutex unlock\n");
    pthread_mutex_unlock (&sem_mut);
#endif

    return (stat);
}

#else

/**
 *  Decrement the semaphore value.
 */
int
dts_semDecr (int id)
{
    int  val = 0;


    if (SEM_DEBUG)
	fprintf (stderr, "semDecr:  id=%d in=%d\n", id, dts_semGetVal(id));

#ifdef USE_MUTEX
    if (SEM_DEBUG)
	fprintf (stderr, "semDencr:  mutex lock\n");
    pthread_mutex_lock (&sem_mut);
#endif

    while (1) {
        val = dts_semGetVal (id) - 1;
        if (val >= 0) {
	    dts_semSetVal (id, val);
	    break;
	} else {
    	    if (SEM_DEBUG)
		fprintf (stderr, "semDecr(%d): waiting val=%d...\n", 
		    id, dts_semGetVal(id));
	    dtsSleep(1);
	}
    }

#ifdef USE_MUTEX
    if (SEM_DEBUG)
	fprintf (stderr, "semDencr:  mutex unlock\n");
    pthread_mutex_unlock (&sem_mut);
#endif

    return ((val < 0) ? -1 : 0);
}


/**
 *  Increment the semaphore value.
 */
int
dts_semIncr (int id)
{
    int  val = 0;


    if (SEM_DEBUG)
	fprintf (stderr, "semIncr:  id=%d in=%d\n", id, dts_semGetVal(id));

#ifdef USE_MUTEX
    if (SEM_DEBUG)
	fprintf (stderr, "semIncr:  mutex lock\n");
    pthread_mutex_lock (&sem_mut);
#endif

    val = dts_semGetVal (id) + 1;
    dts_semSetVal (id, val);

#ifdef USE_MUTEX
    if (SEM_DEBUG)
	fprintf (stderr, "semIncr:  mutex unlock\n");
    pthread_mutex_unlock (&sem_mut);
#endif

    return (OK);
}

#endif


/**
 *  Initialize the semaphore interface starting ID.
 */
void
dts_semInitId (int id)
{
    semId = id;
}
