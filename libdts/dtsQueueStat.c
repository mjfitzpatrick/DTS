/**
 *  DTSQUEUESTAT.C -- DTS queue statistics utilities.
 *
 *  @file  	dtsQueueStat.c
 *  @author  	Mike Fitzpatrick, NOAO
 *  @date  	6/10/09
 *
 *  @brief  DTS queue statistics utilities.  
 */


/*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <ctype.h>
#include <stdarg.h>

#include "dtsPSock.h"
#include "dts.h"


extern  DTS  *dts;
extern  int   dts_monitor;

#define	DEBUG		(dts&&dts->debug)

#define	QSTAT		dtsq->qstat




/**
 *  DTS_QSTATINIT -- Initialize the transfer stat struct.
 *
 *  @brief	Initialize the transfer stat struct.
 *  @fn		void dts_qstatInit (char *qname, char *fname, size_t size)
 *
 *  @param  fname	name of the file
 *  @param  qname	name of the queue
 *  @param  size	size of the file
 */
void
dts_qstatInit (char *qname, char *fname, size_t size)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
            if (fname)
	        strcpy (QSTAT->infile, fname);
            if (qname)
	        strcpy (QSTAT->qname, qname);
	    QSTAT->xfer_size = size;
	}
    }
}


/**
 *  DTS_QSTATSETFNAME -- Set the file name being transferred in a queue.
 *
 *  @brief	Set the file name being transferred in a queue.
 *  @fn		void dts_qstatSetFName (char *qname, char *fname)
 *
 *  @param  qname	queue name
 *  @param  fname	file name
 */
void
dts_qstatSetFName (char *qname, char *fname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    memset (QSTAT->infile, 0, SZ_PATH);
            if (fname)
	        strcpy (QSTAT->infile, fname);
        }
    }
}


/**
 *  DTS_QSTATSETSIZE -- Set the fize of file being transferred in a queue.
 *
 *  @brief	Set the fize of file being transferred in a queue.
 *  @fn		void dts_qstatSetSize (char *qname, size_t size)
 *
 *  @param  qname	queue name
 *  @param  size	size of the file
 */
void
dts_qstatSetSize (char *qname, size_t size)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    QSTAT->xfer_size = size;
        }
    }
}


/**
 *  DTS_QSTATSTART -- Set the start time of file being transferred in a queue.
 *
 *  @brief	Set the start time of file being transferred in a queue.
 *  @fn		void dts_qstatStart (char *qname)
 *
 *  @param  qname	name of the queue
 */
void
dts_qstatStart (char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    gettimeofday (&QSTAT->xfer_start, NULL);
	}
    }
}


/**
 *  DTS_QSTATEND -- Set the end time of file being transferred in a queue.
 *
 *  @brief	Set the end time of file being transferred in a queue.
 *  @fn		void dts_qstatEnd (char *qname)
 *
 *  @param  qname	name of the queue
 */
void
dts_qstatEnd (char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    double   tput;

	    gettimeofday (&QSTAT->xfer_end, NULL);
	    QSTAT->xfer_time = dts_timediff (QSTAT->xfer_start, QSTAT->xfer_end);

    	    tput = (QSTAT->xfer_size * 8) / QSTAT->xfer_time; 	/* Mbps */
	    QSTAT->xfer_rate = tput;
        }
    }
}


/**
 *  DTS_QSTATNETSTART -- Set the start time of network transfer time
 *
 *  @brief	Set the start time of network transfer time
 *  @fn		void dts_qstatNetStart (char *qname)
 *
 *  @param  qname	name of the queue
 */
void
dts_qstatNetStart (char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    gettimeofday (&QSTAT->net_start, NULL);
	}
    }
}


/**
 *  DTS_QSTATNETEND -- Set the end time of network transfer time
 *
 *  @brief	Set the end time of network transfer time
 *  @fn		void dts_qstatNetEnd (char *qname)
 *
 *  @param  qname	name of the queue
 */
void
dts_qstatNetEnd (char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    double   tput;

	    gettimeofday (&QSTAT->net_end, NULL);
	    QSTAT->net_time = dts_timediff (QSTAT->net_start, QSTAT->net_end);

    	    tput = (QSTAT->xfer_size * 8) / QSTAT->net_time; 	/* Mbps */
	    QSTAT->net_rate = tput;
	}
    }
}


/**
 *  DTS_QSTATDISKSTART -- Set the start time of network transfer time
 *
 *  @brief	Set the start time of network transfer time
 *  @fn		void dts_qstatDiskStart (char *qname)
 *
 *  @param  qname	name of the queue
 */
void
dts_qstatDiskStart (char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    gettimeofday (&QSTAT->disk_start, NULL);
	}
    }
}


/**
 *  DTS_QSTATDISKEND -- Set the end time of network transfer time
 *
 *  @brief	Set the end time of network transfer time
 *  @fn		void dts_qstatDiskEnd (char *qname)
 *
 *  @param  qname	name of the queue
 */
void
dts_qstatDiskEnd (char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    double   tput;

	    gettimeofday (&QSTAT->disk_end, NULL);
	    QSTAT->disk_time = dts_timediff (QSTAT->disk_start, QSTAT->disk_end);

    	    tput = (QSTAT->xfer_size * 8) / QSTAT->disk_time;     /* Mbps */
	    QSTAT->disk_rate = tput;
	}
    }
}


/**
 *  DTS_QSTATDISKSTART -- Set the start time of network transfer time
 *
 *  @brief	Set the start time of network transfer time
 *  @fn		void dts_qstatDlvrStart (char *qname)
 *
 *  @param  qname	name of the queue
 */
void
dts_qstatDlvrStart (char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    gettimeofday (&QSTAT->dlvr_start, NULL);
	}
    }
}


/**
 *  DTS_QSTATDISKEND -- Set the end time of network transfer time
 *
 *  @brief	Set the end time of network transfer time
 *  @fn		void dts_qstatDlvrEnd (char *qname)
 *
 *  @param  qname	name of the queue
 */
void
dts_qstatDlvrEnd (char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    gettimeofday (&QSTAT->dlvr_end, NULL);
	    QSTAT->dlvr_time = dts_timediff (QSTAT->dlvr_start, QSTAT->dlvr_end);
	}
    }
}


/**
 *  DTS_QSTATXFERSTAT -- Set the transfer status code.
 *
 *  @brief	Set the transfer status code.
 *  @fn		void dts_qstatXferStat (char *qname, int stat)
 *
 *  @param  qname	name of the queue
 *  @param  stat	OK or ERR
 */
void
dts_qstatXferStat (char *qname, int stat)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    QSTAT->xfer_stat = stat;
	}
    }
}


/**
 *  DTS_QSTATNETSTAT -- Set the network transfer status code.
 *
 *  @brief	Set the network transfer status code.
 *  @fn		void dts_qstatNetStat (char *qname, int stat)
 *
 *  @param  qname	name of the queue
 *  @param  stat	OK or ERR
 */
void
dts_qstatNetStat (char *qname, int stat)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    QSTAT->net_stat = stat;
	}
    }
}


/**
 *  DTS_QSTATDLVRSTAT -- Set the delivery status code.
 *
 *  @brief	Set the delivery status code.
 *  @fn		void dts_qstatDlvrStat (char *qname, int stat)
 *
 *  @param  qname	name of the queue
 *  @param  stat	OK or ERR
 */
void
dts_qstatDlvrStat (char *qname, int stat)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    QSTAT->dlvr_stat = stat;
	}
    }
}


/**
 *  DTS_QSTATSUMMARY -- Print the transfer summary to the log.
 *
 *  @brief	Print the transfer summary to the log.
 *  @fn		void dts_qstatSummary (DTS *dts, char *qname)
 *
 *  @param  dts		DTS struct pointer
 *  @param  qname	name of the queue
 */
void
dts_qstatSummary (DTS *dts, char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname && dts) {
        if ((dtsq = dts_queueLookup (qname))) {
	    char  msg[SZ_LINE];

	    if (dts->debug > 2)
		dts_qstatPrint (qname);
	    memset (msg, 0, SZ_LINE);
	    sprintf (msg, 
		"^b%-6.6s <  XSUM: %d %5.3f %6.2f  Net: %d %5.3f %6.2f  Disk: %5.3f %6.2f  Dlvr: %d %5.3f - %s^r", 
		dts_queueNameFmt (qname),
		QSTAT->xfer_stat, QSTAT->xfer_time, QSTAT->xfer_rate,
		QSTAT->net_stat,  QSTAT->net_time,  QSTAT->net_rate,
		                  QSTAT->disk_time, QSTAT->disk_rate,
		QSTAT->dlvr_stat, QSTAT->dlvr_time,
		QSTAT->infile);

            dtsLog (dts, msg);
	}
    }
}


/**
 *
 */
void
dts_qstatPrint (char *qname)
{
    dtsQueue *dtsq = (dtsQueue *) NULL;

    if (qname) {
        if ((dtsq = dts_queueLookup (qname))) {
	    fprintf (stderr, 
	      "Queue: '%s'   in: '%s'  out: '%s'\n",
	      QSTAT->qname, QSTAT->infile, QSTAT->outfile);

	    fprintf (stderr, "MISC:  size = %ld\n", (long) QSTAT->xfer_size);
	    fprintf (stderr, 
	      "XFER:\n    s: %d.%d e: %d.%d rate: %.3f time: %.3f stat: %d\n",
		(int)(QSTAT->xfer_start.tv_sec - QSTAT->xfer_start.tv_sec),
		(int)(QSTAT->xfer_start.tv_usec),
		(int)(QSTAT->xfer_end.tv_sec - QSTAT->xfer_start.tv_sec),
		(int)(QSTAT->xfer_end.tv_usec),
		QSTAT->xfer_rate, QSTAT->xfer_time, QSTAT->xfer_stat);
	    fprintf (stderr, 
	      " NET:\n    s: %d.%d e: %d.%d rate: %.3f time: %.3f stat: %d\n",
		(int)(QSTAT->net_start.tv_sec - QSTAT->xfer_start.tv_sec),
		(int)(QSTAT->net_start.tv_usec),
		(int)(QSTAT->net_end.tv_sec - QSTAT->xfer_start.tv_sec),
		(int)(QSTAT->net_end.tv_usec),
		QSTAT->net_rate, QSTAT->net_time, QSTAT->net_stat);
	    fprintf (stderr, 
	      "DISK:\n    s: %d.%d e: %d.%d rate: %.3f time: %.3f\n",
		(int)(QSTAT->disk_start.tv_sec - QSTAT->xfer_start.tv_sec),
		(int)(QSTAT->disk_start.tv_usec),
		(int)(QSTAT->disk_end.tv_sec - QSTAT->xfer_start.tv_sec),
		(int)(QSTAT->disk_end.tv_usec),
		QSTAT->disk_rate, QSTAT->disk_time);
	    fprintf (stderr, 
	      "DLVR:\n    s: %d.%d e: %d.%d time: %.3f stat: %d\n",
		(int)(QSTAT->dlvr_start.tv_sec - QSTAT->xfer_start.tv_sec),
		(int)(QSTAT->dlvr_start.tv_usec),
		(int)(QSTAT->dlvr_end.tv_sec - QSTAT->xfer_start.tv_sec),
		(int)(QSTAT->dlvr_end.tv_usec),
		QSTAT->dlvr_time, QSTAT->dlvr_stat);
        }
    }

}
