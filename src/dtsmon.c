/** 
**  DTSMON -- Simple RPC client to log messages from DTS servers.
**
**  Usage:
**
**    dtsmon [-i] [-p <N>] [-version] [<log_file>]
**
**	-i		run interactively
**	-p		monitor port number
**
**	-version	print build version string
**
**	[log_file]	message log file
**
**
**  @file       dtsmon.c
**  @author     Mike Fitzpatrick, NOAO
**  @date       July 2010
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 

#include "dts.h"
#include "dtsClient.h"
#include "build.h"


FILE	*fd;
int	interactive	= 0;			/* options		*/
int  	client 		= DTSMON_NONE;
int	res		= OK;
int	verbose		= FALSE;
int	debug		= FALSE;
int	noop		= FALSE;
int 	webmon 		= FALSE;
    
char  	host[SZ_FNAME];
char	mon_url[SZ_FNAME];
char 	log_line[20][SZ_FNAME];
char 	error_log_line[20][SZ_FNAME];

static void  dtsmon_cmdLoop (void);
static void  dtsmon_attach (char *host, char *passwd);
static int   dtsmon_log (void *data);
static int   dtsmon_stat (void *data);
static char *dtsmon_time (void);
static void  dtsmon_printHelp (void);
static void  dtsmon_line (void);


/**
 *  Task entry point.
 */
int 
main (int argc, char **argv)
{
    register int i=0, j=0, len=0, port=DTSMON_PORT;


    /*  Our only possible argument is the name of the log file.  If no
    **  argument is given we echo the message to the standard output.
    */
    fd = stdout;
    for (i=1; i < argc; i++) {
        if (argv[i][0] == '-') {
            switch (argv[i][1]) {
            case 'i':   interactive++; 			break;
            case 'p':   port = atoi (argv[++i]); 	break;
            case 'v':
                if (strcmp (&(argv[i][2]), "ersion") == 0) {
                    printf ("Build Version: %s\n", build_date);
                    return (0);
                } else
                    verbose++;
                break;

            case '-':
                len = strlen (argv[i]);
                for (j=2; j < len; j++) {
                    switch ( argv[i][j] ) {
           
                    case 'd':  	debug++;        j=len; break;
                    case 'h':  
				break;
                    case 'n':  	noop++;         j=len; break;
                    case 'v':
                        if (strcmp (&(argv[i][j+1]), "ersion") == 0) {
                            printf ("Build Version: %s\n", build_date);
                            return (0);
                        } else
                            verbose++;                                      
                        j=len;
                        break;
                    default:
                        fprintf (stderr, "Invalid option '%s'\n", argv[i]);
                        exit (1);
                    }
                }
                break;

	    default:
		break;
	    }
	} else {

	    if ((fd = fopen (argv[1], "a")) == (FILE *) NULL) {
	        fprintf (stderr, "Cannot open log file '%s'\n", argv[1]);
	        exit (1);
	    }
	}
    }

    /* Sanity checks. 
    */
    if (fd != stdout)
	interactive = 0;


    /* Initialize.
    */
    memset (host, 0, SZ_FNAME);
    memset (mon_url, 0, SZ_FNAME);
    gethostname (host, SZ_FNAME);
    sprintf (mon_url, "http://%s:%d/RPC2", host, port);

    dtsmon_line ();
    fprintf (fd, "%s ", dtsmon_time() );
    fprintf (fd, "%s %s running on %s:%d, pid=%d\n",
	DTSMON_NAME, DTSMON_VERSION, host, port, (int)getpid());
    dtsmon_line ();


    /* Create the server and launch.  Note we never return from this
    ** and make no calls to other servers.
    */
    xr_createServer ("/RPC2", port, NULL);
    xr_addServerMethod ("dtslog", dtsmon_log, NULL);	/* message logging  */
    xr_addServerMethod ("dtstat", dtsmon_stat, NULL);	/* stat logging     */

    if (!interactive) {
    	while (xr_startServer () != 1)
            sleep (1);
	xr_killServer ();
    } else {
        xr_startServerThread ();
        dtsmon_cmdLoop ();
    }

    return (0);
}


/*  DTS_CMDLOOP -- Interactive command loop.  Commands control either the
**  DTS monitor itself, or DTS daemons that are reporting their info to this
**  monitor.
**
**	quit		        quit the command loop
**	break		        put a visible break in the output
**	clear		        clear the display (terminal output only)
**
**	attach <host>	        attach to a running DTS daemon
**	detach 	    	        detach from current targer host
**
**	list <dts|queue|all>    detach from current targer host
**	set <class> <opt> <val>	set a specific value
**	get <class> <opt>      	get a specific value
**
*/
static void
dtsmon_cmdLoop ()
{
    char        cmd[SZ_CMD],  method[SZ_CMD], host[SZ_FNAME], passwd[SZ_FNAME];
    char       *url = calloc (1, SZ_FNAME);


    memset (cmd, 0, SZ_CMD);
    memset (method, 0, SZ_CMD);
    memset (host, 0, SZ_FNAME);

    /* Try connecting to a local DTS.
    */
    strcpy (url, "http://localhost:3000/RPC2");
    if ((client = xr_initClient (url, "dtsmon", "v1.0")) >= 0)
        fprintf (fd, "%s [%s] %s\n", dtsmon_time(), "mon", url);


    while (fgets(cmd, SZ_CMD, stdin)) {

	xr_initParam (client);

        sscanf (cmd, "%s", method);

        if (strcmp (method, "quit") == 0) { 		/* QUIT   	*/
            break;

        } else if (strcmp (method, "help") == 0 || (cmd[0] == '?')) {
    	    dtsmon_printHelp ();

        } else if (strcmp (method, "break") == 0) {	/* BREAK	*/
    	    dtsmon_line ();

        } else if (strcmp (method, "clear") == 0) {
	    fprintf (fd, "[H[2J");			/* CLEAR  	*/
    	    dtsmon_line ();

	/****************************************************************/

        } else if (strcmp (method, "monitor") == 0) {	/* ATTACH	*/
            sscanf (cmd, "%s %s", method, host);
	    dtsmon_attach (host, NULL);

        } else if (strcmp (method, "console") == 0) {	/* CONSOLE	*/
	    memset (passwd, 0, SZ_FNAME);
            sscanf (cmd, "%s %s %s", method, host, passwd);
	    dtsmon_attach (host, passwd);

        } else if (strcmp (method, "detach") == 0) {	/* DETACH	*/
	    xr_callSync (client, "detach");
	    if (client >= 0) {
		xr_closeClient (client);
		client = DTSMON_NONE;
	    }


	/****************************************************************/

        } else if (strcmp (method, "list") == 0) {
            char  opt[SZ_CMD], res[SZ_MAX_MSG], *sres = res;

	    if (client >= 0) {
                sscanf (cmd, "%s %s", method, opt);

                xr_setStringInParam (client, opt);          /* option  */
                xr_callSync (client, "dtsList");

                xr_getStringFromResult (client, &sres);
	        if (sres)
		    fprintf (stderr, "%s\n", sres);
	        else
		    fprintf (stderr, "command failed\n");
		    
	    } else
		fprintf (stderr, "Error:  No client connected\n");


        } else if (strcmp (method, "set") == 0) {
            char  class[SZ_CMD], what[SZ_CMD], val[SZ_CMD];

	    if (client >= 0) {
                sscanf (cmd, "%s %s %s %s", method, class, what, val);

                xr_setStringInParam (client, "dtsmon");     /* who    */
                xr_setStringInParam (client, class);        /* class  */
                xr_setStringInParam (client, what);         /* what   */
                xr_setStringInParam (client, val);          /* value  */
                xr_callSync (client, "dtsSet");

                xr_getIntFromResult (client, &res);
	        if (res == OK)
		    fprintf (stderr, "%s.%s set to %s\n", class, what, val);
	        else
		    fprintf (stderr, "%s.%s set failed\n", class, what);
		    
	    } else
		fprintf (stderr, "Error:  No client connected\n");

        } else if (strcmp (method, "get") == 0) {
            char  class[SZ_CMD], what[SZ_CMD], val[SZ_CMD], *sres = val;

	    if (client >= 0) {
                sscanf (cmd, "%s %s %s", method, class, what);

                xr_setStringInParam (client, class);        /* class  */
                xr_setStringInParam (client, what);         /* what   */
                xr_callSync (client, "dtsGet");

                xr_getStringFromResult (client, &sres);
                xr_getIntFromResult (client, &res);
	        if (res == OK)
		    fprintf (stderr, "%s.%s  = %s\n", class, what, sres);
	        else
		    fprintf (stderr, "%s.%s get failed\n", class, what);
	    } else
		fprintf (stderr, "Error:  No client connected\n");

        } else if (strcmp (method, "get") == 0) {



	/****************************************************************/

        } else
	     fprintf (stderr, "Unknown command '%s'\n", method);

        bzero (cmd, SZ_CMD); bzero (method, SZ_CMD); bzero (host, SZ_FNAME);
    }

    if (url) free ((void *)url);
}


/* DTSMON_ATTACH -- Attach to a DTS either as a logger or a command console.
*/
static void
dtsmon_attach (char *host, char *passwd)
{
    char  *url = calloc (1, SZ_LINE);
	    
    sprintf (url, (strchr (host, (int)':') ? 
	"http://%s/RPC2" : "http://%s:3000/RPC2"), host);

    if (client >= 0)  xr_closeClient (client);
    client = xr_initClient (url, "dtsmon", "v1.0");

    xr_setStringInParam (client, mon_url);
    if (passwd) {
	xr_setStringInParam (client, passwd);
        xr_callSync (client, "console");

    } else 
	xr_callSync (client, "monitor");

    /* ignore result */

    free ((void *) url);
}


/*  DTSMON_LOG - Callback for the 'dtslog' command.  Arguments are the name
**  of the sender, and the message. 
*/
static int 
dtsmon_log (void *data)
{
    char *who = xr_getStringFromParam (data, 0);
    char *msg = xr_getStringFromParam (data, 1);
    int  len;


    len = strlen (msg);			/* stomp newline		  */
    if (strcmp ("\n", &msg[len-1]) == 0)
        msg[len-1] = '\0';

    /* process and character escapes in the message if we're writing
    ** to a terminal.
    */
    if (fd == stdout || fd == stderr) {
        char *buf = calloc (1, strlen(msg)*2);
        char *ip = msg, *op = buf;

	while (*ip) {
	    if (*ip == '^') {
		switch (*(ip+1)) {
		case 'b': *op++='\033'; *op++='['; *op++='1'; *op++='m'; break;
		case 's': *op++='\033'; *op++='['; *op++='7'; *op++='m'; break;
		case 'r': *op++='\033'; *op++='[';            *op++='m'; break;
		default:  fprintf (stderr, "s='%s'\n", ip);
		}
		ip += 2;
	    } else 
		*op++ = *ip++;
	}
        fprintf (fd, "%s [%8.8s] %s\n", dtsmon_time(), who, buf);
        free ( (void *) buf );

    } else
        fprintf (fd, "%s [%8.8s]%s\n", dtsmon_time(), who, msg);
    fflush (fd);


    xr_setIntInResult (data, OK); 	/* No useful result returned .... */

    free ((char *) who);
    free ((char *) msg);

    return (OK);
}


/*  DTSMON_STAT - Callback for the 'dtstat' command.  Arguments are the name
**  of the sender, and the message. 
*/
static int 
dtsmon_stat (void *data)
{
    char  mytmp[SZ_LINE];
    char  mytmp2[SZ_LINE];
    char  mystr[SZ_LINE];
    char  fromhost[SZ_LINE];
    char *who = xr_getStringFromParam (data, 0);
    char *msg = xr_getStringFromParam (data, 1);
    int  len;
    time_t now;


    len = strlen (msg);			/* stomp newline		  */
    if (strcmp ("\n", &msg[len-1]) == 0)
        msg[len-1] = '\0';

    time (&now);

    /* process and character escapes in the message if we're writing
     * to a terminal.
     *
     * For the moment we just print out the value, the 'msg' string will
     * be of the form keyw=valm we may want to break this out for logging
     * to Graphite.
     */

    fprintf (fd, "%s [%8.8s] STAT= %s\n", dtsmon_time(), who, msg);
    fflush (fd);
    strcpy(fromhost, who);

    /*Build the string to feed carbon/graphite */

    if (strstr(msg, "QRATEOUT") != NULL) { 
	//Example QRATE=des=50
    
	strcpy (mytmp, &msg[(strchr(msg, (int) '=')-msg)+1]);
 
	// Get our queuename in mytmp.
	if (strchr (mytmp, (int) '=') != NULL)
	    mytmp[ (strchr(mytmp, (int) '='))-mytmp ] = '\0'; 
    
	// Copy from last instance of = giving us average speed.
	strcpy (mytmp2, &msg[(strrchr(msg, (int) '=')-msg)+1]);

	// Qname average speed time
	sprintf (mystr, "%s_out_xfer_%s %s %d\n", fromhost, mytmp, mytmp2,
	    (int) now);

    } else if (strstr(msg, "QNUM") != NULL) { 
	// Example QNUM=des=50
	strcpy (mytmp,&msg[(strchr(msg, (int) '=')-msg)+1]);
 
	// Get our queuename in mytmp.
	if (strchr(mytmp, (int) '=') != NULL)
	    mytmp[ (strchr(mytmp, (int) '='))-mytmp ] = '\0';
    
	// Copy from last instance of = giving us average speed.
	strcpy (mytmp2, &msg[(strrchr(msg, (int) '=')-msg)+1]);

	//Qname average speed time
	sprintf (mystr, "%s_qnum_%s %s %d\n", fromhost, mytmp, mytmp2,
	    (int) now);

    } else if (strstr(msg, "usedspace") != NULL) { 
	// Copy after = of msg to mytmp.
	strcpy(mytmp,&msg[(strchr(msg, (int) '=')-msg)+1]);

	sprintf (mystr, "%s_usedspace %s %d\n",fromhost, mytmp, (int) now);
	
    } else if (strstr(msg, "dtsd_in") != NULL) { 
	// Copy after = of msg to mytmp.
	strcpy(mytmp,&msg[(strchr(msg, (int) '=')-msg)+1]);

 	sprintf (mystr, "%s_dtsd_in %s %d\n",fromhost, mytmp, (int) now);
	
    } else if (strstr(msg, "dtsd_out") != NULL) {
	// Copy after = of msg to mytmp.
	strcpy(mytmp,&msg[(strchr(msg, (int) '=')-msg)+1]);

	// Build our output string.
	sprintf (mystr, "%s_dtsd_out %s %d\n", fromhost, mytmp, (int) now); 

    } else {
	// Do nothing our string isn't here!
        xr_setIntInResult (data, OK); 	/* No useful result returned .... */
    
	free ((char *) who);
	free ((char *) msg);
	return (OK);
    }


    /*  Do our sock connection here to feed carbon which feeds graphite.
     */
    int sockfd, portno, n;
    struct sockaddr_in serv_addr;
    struct hostent *server;

    portno = 2003;
    sockfd = socket (AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) 
        exit(0);
    if ((server = gethostbyname ("localhost")) == NULL) {
        fprintf(fd,"ERROR, no such host\n");
        exit(0);
    }
    bzero ((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy ((char *)server->h_addr, 
         (char *)&serv_addr.sin_addr.s_addr,
         server->h_length);
    serv_addr.sin_port = htons(portno);
    if (connect (sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0) 
    	exit(0);
    n = write (sockfd, mystr, strlen (mystr));
    close(sockfd);

    /* end of carbon/ graphite feed */


    xr_setIntInResult (data, OK); 	/* No useful result returned .... */

    free ((char *) who);
    free ((char *) msg);

    return (OK);
}


/*  DTSMON_TIME - Generate a time string.
*/
static char * 
dtsmon_time ()
{
    time_t      t  = time (NULL);
    struct tm  *tm = gmtime (&t);
    static char tstr[128];

    memset (tstr, 0, 128);
    strftime (tstr, 128, "%m%d %T", tm);

    return (tstr);
}


/*  Utilities.
*/

static void
dtsmon_printHelp ()
{
    fprintf (stderr, "DTSMon Commands:\n");
    fprintf (stderr, "\n");
    fprintf (stderr, "  help or ?\t\t\tprint this message\n");
    fprintf (stderr, "  quit     \t\t\tquit the dts monitor\n");
    fprintf (stderr, "  break    \t\t\tprint a line break\n");
    fprintf (stderr, "  clear    \t\t\tclear the screen (terminal only)\n");
    fprintf (stderr, "  \n");
    fprintf (stderr, "  monitor <host>\t\tattach target to this monitor\n");
    fprintf (stderr, "  console <host> <passwd>\trequest console access\n");
    fprintf (stderr, "  detach        \t\tdetach from this target\n");
    fprintf (stderr, "  \n");
    fprintf (stderr, "  set <class> <op> <val>\tset a value\n");
    fprintf (stderr, "  get <class> <op>\tget a value\n");
    fprintf (stderr, "\t\n");
}


static void
dtsmon_line ()
{
    fprintf (fd, "========================================");
    fprintf (fd, "========================================\n");
    fflush (fd);
}
