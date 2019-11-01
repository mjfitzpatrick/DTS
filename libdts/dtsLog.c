/**
 *  @file  	dtsLog.c
 *  @author  	Mike Fitzpatrick, NOAO
 *  @date  	6/10/09
 *
 *  @brief  DTS logging interface.  
 *
 *  We can log to a local file as well as to a remote monitoring application.
 *
 */
/*****************************************************************************/


#include <stdio.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <pthread.h>
#include <errno.h>

#include "dts.h"
#include "dtsdb.h"


#define	SZ_FMTSPEC	25		/* max size single format spec	*/
#define	SZ_ARGVAL	128
#define EOS		0

#define TY_INT		0		/* the only types we support	*/
#define TY_DOUBLE	1
#define TY_CHAR		2

/* 
#define	LOG_MUTEX	1
#define	USE_ASYNC	1
*/


/* Private methods.
*/
static char *dts_doarg (va_list **argp, int dtype);
static char *logtime (void);
static char *dts_stripCodes (char *str);

static char  argVal[SZ_ARGVAL];

#ifdef LOG_MUTEX
/*
static pthread_mutex_t log_mut = PTHREAD_MUTEX_INITIALIZER;
*/
pthread_mutex_t log_mut = PTHREAD_MUTEX_INITIALIZER;
#endif


/**
 *  DTS_LOG -- DTS message logger.
 *  
 *  @fn	     dtsLog (DTS *dts, char *format, ...)
 *
 *  @brief  	      DTS message logger.
 *  @param   dts      DTS struct pointer
 *  @param   format   message format string  
 *  @return  nothing
 */
void
dtsLog (DTS *dts, char *format, ...)
{
    int     len;
#ifdef USE_ASYNC
    int     async;
#endif
    char    *buf, *cbuf = NULL;
    va_list argp;


    va_start (argp, format);		/* encode as a single string	*/
    buf = calloc (1, (4 * SZ_LINE) );
    (void) dts_encodeString (buf, format, &argp);
    va_end (argp);

    len = strlen (buf);			/* ensure a newline		*/
    if (strcmp ("\n", &buf[len-1]))
	strcat (buf, "\n");
    
    if (dts->logfd)
        fprintf (dts->logfd, "%s [%8.8s] %s", logtime(), dts->whoAmI, buf);


    /*  Add a message to the local database.  Record id 1 is reserved for
     *  messages on the DTSD itself.
     */
#ifdef LOG_MUTEX
    /*  FIXME
    pthread_mutex_lock (&log_mut);
    */
    if (pthread_mutex_trylock (&log_mut) != 0) 
	dtsSleep (1);
#endif
#ifdef USE_DTS_DB
    (void) dts_dbAddMsg (1, buf);
#endif

    if (dts->mon_fd >= 0) {
#ifdef USE_ASYNC
	async = xr_newASync (DTSMON_CLIENT);
	xr_initParam (async);
	xr_setStringInParam (async, dts->whoAmI);
	xr_setStringInParam (async, buf);
	xr_callASync (async, "dtslog", dts_nullResponse);
	xr_freeASync (DTSMON_CLIENT);

#else
	if (dts->monitor && *(dts->monitor)) {
            int  client = dts_getClient (dts->monitor), res = 0;

	    xr_setStringInParam (client, dts->whoAmI);
	    xr_setStringInParam (client, buf);
            if (xr_callSync (client, "dtslog") == OK)
                xr_getIntFromResult (client, &res);
            dts_closeClient (client);
	}
#endif
    }

#ifdef LOG_MUTEX
    pthread_mutex_unlock (&log_mut);
#endif

    if (dts->debug)
        fprintf (stderr, "%s %s", logtime(), (cbuf = dts_stripCodes (buf)));

    free ((void *) buf);
    if (cbuf)
	free ((void *) cbuf);
}


/**
 *  DTS_LOGMSG -- DTS message logger.  Log to DTSMON and local database.
 *  
 *  @fn	     dtsLogMsg (DTS *dts, int key, char *format, ...)
 *
 *  @brief  	      Log DTS messages to DTSMON and local database
 *  @param   dts      DTS struct pointer
 *  @param   key      DB record key
 *  @param   format   message format string  
 *  @return  nothing
 */
void
dtsLogMsg (DTS *dts, int key, char *format, ...)
{
    int     len;
#ifdef USE_ASYNC
    int     async;
#endif
    char    *buf, *cbuf = NULL;
    va_list argp;


    va_start (argp, format);		/* encode as a single string	*/
    buf = calloc (1, (4 * SZ_LINE) );
    (void) dts_encodeString (buf, format, &argp);
    va_end (argp);

    len = strlen (buf);			/* ensure a newline		*/
    if (strcmp ("\n", &buf[len-1]))
	strcat (buf, "\n");
    
    if (dts->logfd)
        fprintf (dts->logfd, "%s [%8.8s] %s", logtime(), dts->whoAmI, buf);


    /*  Add a message to the local database.  Record id 1 is reserved for
     *  messages on the DTSD itself.
     */
#ifdef LOG_MUTEX
    /*  FIXME
    pthread_mutex_lock (&log_mut);
    */
    if (pthread_mutex_trylock (&log_mut) != 0)
	dtsSleep (1);
#endif
#ifdef USE_DTS_DB
    if (key)
	(void) dts_dbAddMsg (key, buf);
#endif

    if (dts->mon_fd >= 0) {
#ifdef USE_ASYNC
	async = xr_newASync (DTSMON_CLIENT);
	xr_initParam (async);
	xr_setStringInParam (async, dts->whoAmI);
	xr_setStringInParam (async, buf);
	xr_callASync (async, "dtslog", dts_nullResponse);

#else
	if (dts->monitor && *(dts->monitor)) {
            int  client = dts_getClient (dts->monitor), res = 0;

	    xr_setStringInParam (client, dts->whoAmI);
	    xr_setStringInParam (client, buf);
            if (xr_callSync (client, "dtslog") == OK)
                xr_getIntFromResult (client, &res);
            dts_closeClient (client);
	}
#endif
    }

#ifdef LOG_MUTEX
    pthread_mutex_unlock (&log_mut);
#endif

    if (dts->debug)
        fprintf (stderr, "%s %s", logtime(), (cbuf = dts_stripCodes (buf)));

    free ((void *) buf);
    if (cbuf)
	free ((void *) cbuf);
}


/**
 *  DTS_LOGSTAT -- DTS stat logger.
 *  
 *  @fn	     dtsLogStat (DTS *dts, char *msg)
 *
 *  @brief  	      DTS message logger.
 *  @param   dts      DTS struct pointer
 *  @param   msg   message string  
 *  @return  nothing
 */
void
dtsLogStat (DTS *dts, char *msg)
{
    int     len;
#ifdef USE_ASYNC
    int     async;
#endif


    len = strlen (msg);			/* ensure a newline		*/
    if (strcmp ("\n", &msg[len-1]))
	strcat (msg, "\n");
    
    if (dts->logfd)
        fprintf (dts->logfd, "%s [%8.8s] %s", logtime(), dts->whoAmI, msg);


#ifdef LOG_MUTEX
    /*  FIXME
    pthread_mutex_lock (&log_mut);
    */
    if (pthread_mutex_trylock (&log_mut) != 0) 
	dtsSleep (1);
#endif

    if (dts->mon_fd >= 0) {
#ifdef USE_ASYNC
	async = xr_newASync (DTSMON_CLIENT);
	xr_initParam (async);
	xr_setStringInParam (async, dts->whoAmI);
	xr_setStringInParam (async, msg);
	xr_callASync (async, "dtslog", dts_nullResponse);
	xr_freeASync (DTSMON_CLIENT);

#else
	if (dts->monitor && *(dts->monitor)) {
            int  client = dts_getClient (dts->monitor), res = 0;

	    xr_setStringInParam (client, dts->whoAmI);
	    xr_setStringInParam (client, msg);
            if (xr_callSync (client, "dtstat") == OK)
                xr_getIntFromResult (client, &res);
            dts_closeClient (client);
	}
#endif
    }

#ifdef LOG_MUTEX
    pthread_mutex_unlock (&log_mut);
#endif

    if (dts->debug) {
	char *cbuf = NULL;
        fprintf (stderr, "%s %s", logtime(), (cbuf = dts_stripCodes (msg)));
	free ((void *) cbuf);
    }
}


/**
 *  DTS_ERRLOG -- DTS message logger.
 *  
 *  @fn	     dtsErrLog (dtsQueue *dtsq, char *format, ...)
 *
 *  @brief  	      DTS message logger.
 *  @param   dtsq     queue pointer to save error msg (or NULL)
 *  @param   format   message format string  
 *  @return  nothing
 */

#define	SZ_ERRBUF	(4*SZ_LINE)

void
dtsErrLog (dtsQueue *dtsq, char *format, ...)
{
    char    buf[SZ_ERRBUF], err[SZ_ERRBUF];
    va_list argp;
    extern  DTS *dts;
    int nerr = dts->nerrs;


    memset (buf, 0, SZ_ERRBUF);
    memset (err, 0, SZ_ERRBUF);

    va_start (argp, format);		/* encode as a single string	*/
    (void) dts_encodeString (buf, format, &argp);
    va_end (argp);

    /*  Save the message to the queue struct.
     */
    /*  FIXME --Don't allocate since the fork breaks the message logging
     *          and we never actually use the space.
     */
    dts->emsgs[nerr] = calloc (1, max (SZ_LINE, strlen(buf) + 32));
    if (dtsq) {
        sprintf (dts->emsgs[nerr], "%s: %s", dtsq->name, buf);
        //dtsq->nerrs++;			// FIXME
        //dtsShm (dts);
        //dtsq->qstat->nerrs++;

    } else
        sprintf (dts->emsgs[nerr], "(null): %s", buf);
    dts->nerrs++;

    /*  If we fill the error message buf, throw out the oldest one and
     *  shift the list.
     */
    /*  FIXME -- Because of the fork this doesn't update the global struct.
    if (dts->nerrs == MAX_EMSGS) {
	register int i = 0;

	free (dts->emsgs[0]);
	for (i=0; i < MAX_EMSGS; i++)
	    dts->emsgs[i] = dts->emsgs[i+1];
    }
     */

    sprintf (err, "ERR: ^s %s ^r", buf);
    dtsLog (dts, err);
}


/**
 *  DTSTIMELOG -- DTS timing message logger.
 *  
 *  @fn	     dtsTimeLog (char *fmt, struct timeval t1)
 *
 *  @brief  	      DTS timing message logger.
 *  @param   format   message format string (must include single '%g' format)
 *  @param   t1       starting time struct
 *  @return  nothing
 */

void
dtsTimeLog (char *fmt, struct timeval t1)
{
    extern  DTS *dts;

    if (TIME_DEBUG)
        /* dtsLog (dts, fmt, dts_tstop (t1)); */
	fprintf (stderr, fmt, dts_tstop (t1));
}



/**
 *  DTS_ENCODESTRING -- Process the format to the output file, taking arguments
 *  from the list pointed to by argp as % format specs are encountered in 
 *  the input.
 *
 *  @fn	     dts_encodeString (char *buf, char *format, va_list *argp)
 *
 *  @param   buf     formatted output buffer
 *  @param   format  format string
 *  @param   argp    variable-length arguments
 *  @return
 */
void
dts_encodeString (char *buf, char *format, va_list *argp)
{
    register int ch;			/* next format char reference	*/
    char	formspec[SZ_FMTSPEC];	/* copy of single format spec	*/
    char	*fsp;			/* pointer into formspec	*/
    char  	cbuf[10];
    int	        done;			/* one when at end of a format	*/
    int	        nch = SZ_LINE;		/* one when at end of a format	*/


    while ((ch = *format++) && nch > 0) {
      if (ch == '%') {
    	fsp = formspec;
    	*fsp++ = ch;
    	done = 0;

    	while (!done) {
    	    ch = *fsp++ = *format++;

    	    switch (ch) {
    	    case EOS:
    		--format;
    		done++;
    		break;

    	    case 'l':
    		fsp--; 			/* arg size modifier; ignored for now */
    		break;

    	    case 'b':			/* nonstandard UNIX	*/
    	    case 'c':
    	    case 'd':
    	    case 'o':
    	    case 'x':
    	    case 'u':
    		*fsp = EOS;
    		strcat (buf, dts_doarg (&argp, TY_INT));
    		done++;
    		break;

    	    case 'E':			/* ANSI emulation	*/
    		*(fsp-1) = 'e';
    		goto rval;
    	    case 'G':			/* ANSI emulation	*/
    		*(fsp-1) = 'g';
    		goto rval;

    	    case 'e':
    	    case 'f':
    	    case 'g':
rval:
    		*fsp = EOS;
    		strcat (buf, dts_doarg (&argp, TY_DOUBLE));
    		done++;
    		break;

    	    case 's':
    		*fsp = EOS;
    		strcat (buf, dts_doarg (&argp, TY_CHAR));
    		done++;
    		break;
    	    }
    	}

      } else {
	  memset (cbuf, 0, 10);
	  sprintf (cbuf, "%c", ch);
    	  strcat (buf, cbuf);
      }

      nch = SZ_LINE - strlen (buf);	/* prevent overflow		*/
    }
}


/**
 *  DTS_DOARG -- Encode a single argument acording to the data type.
 *
 *  @fn     static char *dts_doarg (va_list **argp, int dtype)
 *
 *  @param  argp	argument list
 *  @param  dtype	data type
 *
 */
static char * 
dts_doarg (va_list **argp, int dtype)
{
    int		ival;
    double	dval;
    char	*cptr;


    memset (argVal, 0, SZ_ARGVAL);

    /* Pass the data value to be encoded, bump argument pointer by the
    ** size of the data object.  If there is no data value the case
    ** is a no-op.
    */
    switch (dtype) {
    case TY_INT:
        ival = va_arg ((**argp), int);
        sprintf (argVal, "%d", ival);
        break;
    case TY_DOUBLE:
        dval = va_arg ((**argp), double);
        sprintf (argVal, "%g", dval);
        break;
    case TY_CHAR:
        cptr = va_arg ((**argp), char *);
        sprintf (argVal, "%-6s", (cptr ? cptr : "INDEF"));
        break;
    }

    return (argVal);
}


/**
 *  LOGTIME - Generate a time string for the log.
 *
 *  @fn	static char *logtime()
 */
static char * 
logtime ()
{
    time_t      t  = time (NULL);
    struct tm  *tm, tbuf;
    char   tstr[128];

    tm = gmtime_r (&t, &tbuf);

    memset (tstr, 0, 128);
    strftime (tstr, 128, "%m%d %T", tm);

    return ( dts_strbuf (tstr) );
}


/**
 *  DTS_STRIPCODES -- Remove standout/bold codes from a message.
 */
static char *
dts_stripCodes (char *str)
{
    char buf[1024], *ip, *op;

    memset (buf, 0, 1024);
    for (ip=str, op=buf; *ip; ) {
	if (*ip == '^')
	    ip += 2;
	else 
	    *op++ = *ip++;
    }

    return ( strdup (buf) );
}
