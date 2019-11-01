/**
 *  DTSDELIVER.C -- DTS procedures to "deliver" a transfer object.
 *
 *  @file  	dtsDeliver.c
 *  @author  	Mike Fitzpatrick, NOAO
 *  @date  	6/10/09
 *
 *  @brief  DTS procedures to "deliver" a transfer object.
 */


/*****************************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <ctype.h>
#include <stdarg.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "dts.h"


extern  DTS  *dts;
extern  int   dts_monitor;

#define	DEBUG		(dts&&dts->debug)
#define	VDEBUG		(dts&&dts->debug>2)

#define	MAXTRIES	3


static int sig_int      = 0,
           sig_quit     = 0,
           sig_hup      = 0,
           sig_term     = 0;


/**
 *  DTS_DELIVER -- Deliver a file at a destination location.
 *
 *  @brief	Deliver a file at a destination location.
 *  @fn		char *dts_Deliver (dtsQueue *dtsq, Control *ctrl, 
 *			char *fpath, int *stat)
 *
 *  @param  dtsq	DTS queue structure
 *  @param  ctrl	Transfer control structure
 *  @param  fpath	path to the file in spool dir
 *  @param  stat	delivery application status
 *  @return
 */
char *
dts_Deliver (dtsQueue *dtsq, Control *ctrl, char *fpath, int *stat)
{
    /* static */ char *cmd, emsg[SZ_LINE], dpath[SZ_PATH], dfname[SZ_PATH];
    /* static */ char  pfname[SZ_PATH];
    int    status = OK, nfailed = 0;



    /*  Sanity checks.
     */
    if (! dtsq) {
        fprintf (stderr, "Error: dts_Deliver gets NULL 'dtsq'\n");
        exit (1);
    } else
	dts_qstatDlvrStart (dtsq->name);


    /*  If there is no delivery directory specified, just return.
     */
    if (!dtsq->deliveryDir[0]) {
        if (dts->verbose)
	    dtsLog (dts, "%6.6s <  DLVR: status=OK [No delivery directory]\n",
		dts_queueNameFmt(dtsq->name));
	return ((char *) NULL);
    }


    /*  First step:  Copy the file to the deilvery directory from the
     *  'fpath', which is the path to the file in the spool directory.
     */
    memset (emsg, 0, SZ_LINE);
    memset (dpath, 0, SZ_PATH);
    strcpy (dpath, dtsq->deliveryDir);
    strcpy (dfname, dts_getDeliveryPath (dtsq, ctrl));
    if (dtsq->deliverAs[0])
	dtsLog (dts, "%6.6s <  DLVR: deliverAs = '%s'\n",
	    dts_queueNameFmt(dtsq->name), ctrl->filename);

    if (VDEBUG || DLVR_DEBUG) {
	dtsLog (dts, " fpath = '%s'", fpath);
	dtsLog (dts, "dfname = '%s'", dfname);
	fprintf (stderr, "Deliver:  cp '%s' -> '%s'\n", fpath, dfname);
    }
	

    /*  FIXME -- Need to implement overwrite policy at this point.
     */
    while (nfailed < MAXTRIES && dts_fileCopy (fpath, dfname) == ERR) {
	nfailed++;
	unlink (dfname);
	if (dts->verbose || DLVR_DEBUG)
	    dtsLog (dts, "ERROR[%d]: Failed delivery copy to '%s'\n", 
		nfailed, dfname);
    }

    /*  Validate the delivery, i.e. make sure what we delivered is the
     *  same as what we already have.
     */
    if (nfailed >= MAXTRIES) {
	sprintf (emsg, "Delivery file copy failed");
	status = ERR;
	goto deliver_err_;
#ifdef DOUBLE_VALIDATE
    } else {
	/*  NOTE -- We only get called if we've passed validation as part of
	 *  the endTransfer method, so doing it here is redundant/expensive.
	 */
        if (dts_fileValidate (dfname,ctrl->sum32,ctrl->crc32,ctrl->md5) != OK) {
	    sprintf (emsg, "delivery checksum error '%s'", dfname);
	    status = ERR;
	    goto deliver_err_;
        }
#endif
    }

    /*  Second step:  Execute the delivery command on the delivered file.
     *
     *  The first step is to format the delivery command given the template
     *  argument we got from the config file, then execute the command.
     */
    *stat = OK;
    if (dtsq->deliveryCmd[0]) {
        cmd = dts_fmtQueueCmd (dtsq, ctrl);
	    
        if (VDEBUG || DLVR_DEBUG)
	    fprintf (stderr, "Deliver: cmd = '%s'\n", cmd);

	if (strcmp (dtsq->deliveryCmd, "dts.null") == 0) {
	    /*  Builtin special command to delete the delivered file/dir.
	     */
	    if (dts_localDelete (dfname, 1) < 0) {
	        dtsLog (dts, "ERROR[%d]: Failed to delete '%s'\n", 
		    nfailed, dfname);
	        status = 1;		/* set minor error status	*/
	    } else { 
        	if (dts->verbose || DLVR_DEBUG)
	            dtsLog (dts, "%6.6s <  DLVR: Auto-removed '%s'\n", 
			dts_queueNameFmt (dtsq->name), dfname);
	        status = 0;		/* set OK status		*/
	    }
	    *stat = status;

	} else if ((status = dts_sysExec (dtsq->deliveryDir, cmd)) != OK) {
	    /*  Otherwise, execute the specified command.
	     */

	    /*
	    *stat = status;
	    */
            dtsLog (dts, "deliver cmd error on '%s', status=%d", dfname,status);

            switch (status) {
            case -1:     break;         /* Command not found		*/
            case  0:     break;         /* OK                           */
            case  1:     break;         /* Minor error, keep going      */
            case  2:                    /* Fatal error, reject file     */
                goto deliver_err_;
            case  3:                    /* Fatal error, halt queue      */
                dts_semSetVal (dtsq->activeSem, QUEUE_PAUSED);
                goto deliver_err_;
            default:
                sprintf (emsg, "Unknown delivery cmd error, status=%d", status);
                goto deliver_err_;
            }

        }
        dtsLog (dts, "%6.6s <  DLVR cmd file=%s, status=%d", 
	    dts_queueNameFmt (dtsq->name), dfname, status);
    }


    /*  See if the deliveryApp left behind a parameter file we need to 
     *  append to the control file.  This can include a directive that
     *  tells us what the delivered filename should be or parameters that
     *  may be required by downstream delivery apps.
     */
    sprintf (pfname, "%s/%s.par", dtsq->deliveryDir, dtsq->name);
    if (access (pfname, R_OK) == 0) {
	if (DLVR_DEBUG)
            dtsLog (dts, "DLVR new pars file=%s", pfname);
	dts_loadDeliveryParams (ctrl, pfname);
	unlink (pfname);			/* remove the parfile	 */
    }


    /*  Last step is to append a history record of the delivery to the
     *  control file.
     */
    (void) dts_addControlHistory (ctrl, NULL);

    if (dts->verbose || DLVR_DEBUG)
	dtsLog (dts, "%6.6s <  DLVR: status=OK file=%s\n", 
	    dts_queueNameFmt (dtsq->name), dfname);

    dts_qstatDlvrStat (dtsq->name, OK);
    dts_qstatDlvrEnd (dtsq->name);

    return ((char *) NULL);


deliver_err_:
    (void) dts_addControlHistory (ctrl, emsg);

    if (dts->verbose)
	dtsLog (dts, "%6.6s <  DLVR: status=ERR file=%s\n", 
	    dts_queueNameFmt (dtsq->name), dfname);
	
    dts_qstatDlvrStat (dtsq->name, ERR);
    dts_qstatDlvrEnd (dtsq->name);

    return ( dts_strbuf (emsg) );
}


/**
 *  DTS_GETDELIVERYPATH -- Get the delivery path name.
 *
 *  @brief	Get the delivery path name.
 *  @fn		char *dts_getDeliveryPath (dtsQueue *dtsq, Control *ctrl)
 *
 *  @param  dtsq	queue structure
 *  @param  ctrl	control structure
 *  @return		nothing
 */
char *
dts_getDeliveryPath (dtsQueue *dtsq, Control *ctrl)
{
    char  dfname[SZ_PATH];

    memset (dfname, 0, SZ_PATH);

    /*  Sanity checks.
     */
    if (! dtsq) {
        fprintf (stderr, "Error: dts_getDeliveryPath gets NULL 'dtsq'\n");
        exit (1);
    }


    if (dtsq->deliverAs[0]) {
	if (strncasecmp (dtsq->deliverAs, "$F", 2) == 0) 
	    sprintf (dfname, "%s/%s", dtsq->deliveryDir,  ctrl->filename);
	else if (strncasecmp (dtsq->deliverAs, "$D", 2) == 0) 
	    sprintf (dfname, "%s/%s", dtsq->deliveryDir, ctrl->deliveryName);
	else
	    sprintf (dfname, "%s/%s", dtsq->deliveryDir, dtsq->deliverAs);

    } else {
	/*  If we don't specify a 'deliverAs' param, we default to use 
	 *  the name specified upstream, or else the original name.
	 */
	sprintf (dfname, "%s/%s", dtsq->deliveryDir, 
	    (ctrl->deliveryName[0] ? ctrl->deliveryName : ctrl->filename));
    }

    return ( dts_strbuf (dfname) );
}


/**
 *  DTS_LOADDELIVERYPARAMS -- Load the delivery app parameters.
 *
 *  @brief	Load the delivery app parameters.
 *  @fn		int dts_loadDeliveryParams (Control *ctrl, char *pfname)
 *
 *  @param  ctrl	control structure
 *  @param  pfname	path to the delivery parameters file
 *  @return		nothing
 */
void
dts_loadDeliveryParams (Control *ctrl, char *pfname)
{
    char    param[SZ_LINE], val[SZ_LINE], line[SZ_LINE], *ip, *op;
    FILE   *fd;


    if ((fd = fopen (pfname, "r")) != (FILE *) NULL) {
        while (dtsGets (line, SZ_LINE, fd)) {

            memset (val, 0, SZ_LINE);
            memset (param, 0, SZ_LINE);

            for (ip=line, op=param; !isspace(*ip); ip++)    /* param       */
		*op++ = *ip;
            while (isspace (*ip) || *ip == '=') 	    /* whitespace  */
		ip++;
            for (op=val; *ip && *ip != '\n'; ip++)     	    /* value       */
		*op++ = *ip;

            if (strncmp (param, "deliveryName", 12) == 0)
                strcpy (ctrl->deliveryName, val);
	    else {
                /* Arbitrary parameter.
                 */
                strcpy (ctrl->params[ctrl->nparams].name, param);
                strcpy (ctrl->params[ctrl->nparams].value, val);
                ctrl->nparams++;
	    }
	}
    }
    fclose (fd);
}


/**
 *  DTS_TESTDELIVERYDIR -- Test the delivery directory.
 *
 *  @brief	Test the delivery directory.
 *  @fn		int dts_testDeliveryDir (char *dpath, int create, char *msg)
 *
 *  @param  dpath	path to the delivery directory
 *  @param  create	create directory if it doesn't exist?
 *  @param  msg		return message string
 */
int
dts_testDeliveryDir (char *dpath, int create, char *msg)
{
    char   test[SZ_PATH];
    struct stat st;
    int    ifd = 0;


    /* Test for existence.
     */
    if (access (dpath, F_OK) < 0) {
	/*  Directory doesn't exist, see if we can create it.
	 */
	if (create) {
	    if (mkdir (dpath, DTS_DIR_MODE) < 0) {
		sprintf (msg, 
		    "deliveryDir: cannot create '%s': %s", 
		    dpath, strerror (errno));
		return (ERR);
	    }
	} else {
	    sprintf (msg, "deliveryDir: '%s' doesn't exist", dpath);
	    return (ERR);
	}
    } else {
	/*  Path exists, but make sure it is a directory.
	 */
	if (stat (dpath, &st) < 0) {
	    sprintf (msg, "deliveryDir: cannot stat '%s'", dpath);
	    return (ERR);
	}
	if (! S_ISDIR(st.st_mode) ) {
	    sprintf (msg, "deliveryDir: '%s' not a directory", dpath);
	    return (ERR);
	}
    }

    /* Test that we can write to it.
     */
    memset (test, 0, SZ_PATH);
    sprintf (test, "%s/.test", dpath);
    if ((ifd = creat (test, DTS_FILE_MODE)) < 0) {
        sprintf (msg, "deliveryDir: cannot write to directory '%s'", dpath);
        return (ERR);
    } else {
	close (ifd);
        unlink (test);
    }

    return (OK);
}


/**
 *  DTS_VALIDATEDELIVERY -- Verify the delivered file.
 *
 *  @brief	Verify the delivered file.
 *  @fn		int dts_validateDelivery (Control *ctrl, char *dpath)
 *
 *  @param  ctrl	transfer control strucure
 *  @param  dpath	path to the delivery directory
 */
int
dts_validateDelivery (Control *ctrl, char *dpath)
{
    return (dts_fileValidate (dpath, ctrl->sum32, ctrl->crc32, ctrl->md5));
}


/**
 *  DTS_ADDCONTROLHISTORY -- Add history record to control file.
 *
 *  @brief	Add history record to control file.
 *  @fn		int dts_addControlHistory (Control *ctrl, char *msg)
 *
 *  @param  ctrl	transfer control strucure
 *  @param  msg		NULL or an error message
 */
int
dts_addControlHistory (Control *ctrl, char *msg)
{
    FILE *fd = (FILE *) NULL;
    char  cpath[SZ_PATH];
    char *qp = dts_sandboxPath (ctrl->queuePath);

    memset (cpath, 0, SZ_PATH);
    sprintf (cpath, "%s/_control", qp);
    if (qp) free ((void *) qp);
    
    if ((fd = fopen (cpath, "a+")) == (FILE *) NULL)
	return (ERR);

    if (msg) {
    	fprintf (fd, "%-12s = ERR %s %s\n", ctrl->queueName, 
	    dts_localTime(), msg);
    } else
    	fprintf (fd, "%-12s = OK  %s\n", ctrl->queueName, dts_localTime());

    fflush (fd);
    fclose (fd);

    return (OK);
}


/**
 *  DTS_SYSEXEC -- Execute a general UNIX command passed as a string.  The 
 *  command may contain i/o redirection metacharacters.  The full path of 
 *  the command to be executed should be given.
 *
 *  @brief      Execute a general UNIX command passed as a string.
 *  @fn         int dts_sysExec (char *ewd, char *cmd)
 *
 *  @param  ewd       effective working directory
 *  @param  cmd       command string to be executed
 */
int
dts_sysExec (char *ewd, char *cmd)
{
    char  *ip, *argv[256], *inname = NULL, *outname = NULL;
    int    append = NO, pid = 0, argc = 0, stat = OK;


    if (dts->debug)
        dtsLog (dts, "sysExec Cmd: %s\n", cmd);


    /*  Initialize signal handling.
     */
    sig_int  = (int) signal (SIGINT,  SIG_IGN) & 01;
    sig_quit = (int) signal (SIGQUIT, SIG_IGN) & 01;
    sig_hup  = (int) signal (SIGHUP,  SIG_IGN) & 01;
    sig_term = (int) signal (SIGTERM, SIG_IGN) & 01;

    /*  Parse command string into argv array, inname, and outname.
     */
    for (ip=cmd; isspace (*ip); )
        ++ip;
    while (*ip) {
        if (*ip == '<')
    	    inname = ip + 1;
        else if (*ip == '>') {
    	    if (ip[1] == '>') {
    	        append = YES;
    	        outname = ip + 2;
    	    } else {
    	        append = NO;
    	        outname = ip + 1;
    	    }
        } else
    	    argv[argc++] = ip;
        while ( !isspace (*ip) && *ip != '\0' )
    	    ++ip;
        if (*ip) {
    	    *ip++ = '\0';
    	    while (isspace (*ip))
    	        ++ip;
        }
    }

    if (argc <= 0)				/* no command */
        return (-1);
    argv[argc] = 0;

    /*  Make sure the command exists.
     */
    if (access (argv[0], F_OK) < 0 || access (argv[0], X_OK) < 0) {
        if (dts->verbose) {
	    dtsLog (dts, "sysExec1: No such command '%s'", argv[0]);
	    /*  FIXME -- Causes an abort for some reason.
	    dtsErrLog (NULL, "sysExec2: No such command '%s'", argv[0]);
	    */
	}
	return (-1);
    }

    /* Execute the command. 
     */
    signal (SIGCHLD, SIG_DFL);		/* so our waitpid() works...  */
    if ((pid = fork()) == 0) {
        if (inname)
    	    freopen (inname, "r", stdin);
        if (outname)
    	    freopen (outname, (append ? "a" : "w"), stdout);
        dts_Enbint (SIG_DFL);

	chdir (ewd);
        execv (argv[0], argv);

	/* If we get this far the exec() failed.
	 */
        if (dts->verbose)
	    dtsErrLog (NULL, "Cannot execute %s", argv[0]);
    }

    stat = dts_Await (pid);

    return (stat);
}


/**
 *  DTS_AWAIT -- Wait for an asynchronous child process to terminate.
 *
 *  @brief      Wait for an asynchronous child process to terminate.
 *  @fn         int dts_Await (int pid)
 *
 *  @param  pid       process ID
 */
int 
dts_Await (int pid)
{
    int     w, status = 0;

    dts_Enbint (SIG_IGN);
    while ((w = waitpid ((pid_t) pid, &status, 0)) != pid) {
        if (w == -1) {
	    if (errno != ECHILD)
                dtsLog (dts, "sysExec[%d]: %s", errno, strerror(errno));
	    break;
        }
    }
    dts_Enbint ((SIGFUNC) dts_Interrupt);

    if (status & 0377) {
        if (status != SIGINT)
            dtsLog (dts, "Termination code %d", status);
        return (2);
    }

    return (WEXITSTATUS(status));
}


/**
 *  DTS_INTERRUPT -- Exception handler, called if an interrupt is received
 */
void
dts_Interrupt (int value)
{
    exit (value);
}


/**
 *  DTS_ENBINT -- Post an exception handler function to be executed if any
 *  sort of interrupt occurs.
 */
void
dts_Enbint (SIGFUNC handler)
{
    /*  Set the signal handlers.
     */
    if (sig_int  == 0) signal (SIGINT,  handler);
    if (sig_quit == 0) signal (SIGQUIT, handler);
    if (sig_hup  == 0) signal (SIGHUP,  handler);
    if (sig_term == 0) signal (SIGTERM, handler);
}
