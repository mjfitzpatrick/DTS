/**
 *  @file       dtsSockUtil.c
 *  @author     Mike Fitzpatrick, NOAO
 *  @date       6/10/09
 *
 *  @brief  DTS socket utilities.
 *
 *
 */


#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <ctype.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/select.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include <sys/errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#include "dts.h"
#include "dtsUDT.h"
#include "dtsPSock.h"
#include "udtc.h"


#define	DEBUG			0

pthread_mutex_t cs_mut = PTHREAD_MUTEX_INITIALIZER; /* client socket mutex */
pthread_mutex_t ss_mut = PTHREAD_MUTEX_INITIALIZER; /* server socket mutex */



/*  Private procedures.
 */
void 	dts_setNonBlock (int sock);
void 	dts_setBlock (int sock);



/** 
 *  dts_openServerSocket -- Open a socket to be used on the 'server' side.
 *
 *  @brief  Open a socket to be used on the 'server' side
 *  @fn     int dts_openServerSocket (int port)
 *
 *  @param  port	port number to open
 *  @return		socket descriptor
 *
 */
int
dts_openServerSocket (int port)
{
    struct  sockaddr_in servaddr; 	/* server address 		*/
    int     ps = 0;                    	/* parallel socket descriptor	*/
    int     sndsz = TCP_WINDOW_SZ, rcvsz = TCP_WINDOW_SZ;
    int32_t yes = 1, ntries = 5;


    pthread_mutex_lock (&ss_mut);

    //sndsz = rcvsz = TCP_WINDOW_MAX; 	/*  FIXME */
    sndsz = TCP_WINDOW_SZ;
    rcvsz = TCP_WINDOW_SZ;

    /* Create a socket.
    */
    if ((ps = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
	fprintf (stderr, "openServerSocket(%d): %s\n", errno, strerror(errno));
        dtsError ("serverSocket: socket()");
	return (-1);
    }

    setsockopt (ps, SOL_SOCKET, SO_REUSEADDR, (void *)&yes,   sizeof(yes));
#ifdef TCP_APPTUNE
    setsockopt (ps, SOL_SOCKET, SO_SNDBUF,    (void *)&sndsz, sizeof(int));
    setsockopt (ps, SOL_SOCKET, SO_RCVBUF,    (void *)&rcvsz, sizeof(int));
#endif

    if (DEBUG || SOCK_DEBUG)
	fprintf (stderr, "server socket %d on %s:%d\n", ps, 
	    dts_getLocalIP(), port );


    /* Set server address.
    */
    memset (&servaddr, 0, sizeof servaddr);
    servaddr.sin_family      = AF_INET;
    servaddr.sin_port        = htons(port);
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);

    /* Bind to the server socket and listen for a connection.
    */
    while (ntries--) {
        if (bind (ps, (struct sockaddr*)&servaddr, sizeof servaddr) < 0) {
            dtsErrLog (NULL, "serverSock: bind(%d:%s)", port, strerror (errno));
            /*dtsError ("serverSocket: bind(%d)", port); */
	    if (!ntries)
	        return (-1);
        } else {
            if ((listen (ps, SOMAXCONN)) < 0) {
                dtsErrLog (NULL, 
		    "serverSock: listen(%d:%s)", port, strerror(errno));
                /*dtsError ("serverSocket: listen(%d)", port); */
	        if (!ntries)
	            return (-1);
            } else { 
	        break;
            }
	}
	dtsSleep (SOCK_PAUSE_TIME);
    }

    pthread_mutex_unlock (&ss_mut);

    return (ps);
}


/** 
 *  dts_testServerSocket -- Test whether a server socket is in use.
 *
 *  @brief  Test whether a server socket is in use.
 *  @fn     int dts_testServerSocket (int port)
 *
 *  @param  port	port number to open
 *  @return		status (0=free, 1=in-use)
 *
 */
int
dts_testServerSocket (int port)
{
    struct sockaddr_in servaddr; 	/* server address 		*/
    int    ps, status = 0;             	/* parallel socket descriptor	*/
    int32_t yes = 1;


    /*  Create a socket.
     */
    if ((ps = socket (AF_INET, SOCK_STREAM, 0)) < 0)
        dtsError ("testServer: socket()");

    /*  Re-use socket address.
     */
    setsockopt (ps, SOL_SOCKET, SO_REUSEADDR, (void *)&yes,   sizeof(yes));

    /*  Set server address.
     */
    memset (&servaddr, 0, sizeof servaddr);
    servaddr.sin_family      = AF_INET;
    servaddr.sin_port        = htons(port);
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);

    /*  Bind to the server socket and listen for a connection.
     */
    if (bind (ps, (struct sockaddr*)&servaddr, sizeof servaddr) != 0)
	status = 1;
    else
	close (ps);

    return (status);
}


/** 
 *  dts_getOpenPort -- Get an open server port.  The input port is a high
 *  value, if not immediately open we decrement the port up to the max
 *  number of attempts.
 *
 *  @brief  Get an open server port.
 *  @fn     int dts_getOpenPort (int port, int maxTries)
 *
 *  @param  port	initial port number to open (high value)
 *  @param  maxTries	max ports to test
 *  @return		open port number
 *
 */
int
dts_getOpenPort (int port, int maxTries)
{
    int  i, oport = port;

    for (i=0; i < maxTries; i++, oport--) {
	if (dts_testServerSocket (oport) == 0)
	    return (oport);
	dtsSleep (SOCK_PAUSE_TIME);
    }

    fprintf (stderr, "Cannot get open port = %d, maxTries = %d\n", 
	port, maxTries);

    return (ERR);
}


/** 
 *  dts_openClientSocket -- Open a socket to be used on the 'client' side.
 *
 *  @brief  Open a socket to be used on the 'client' side
 *  @fn     int dts_openClientSocket (char *host, int port, int retry)
 *
 *  @param  host	host name
 *  @param  port	port number to open
 *  @param  retry	attempt to reconnect?
 *  @return		socket descriptor
 *
 */
int
dts_openClientSocket (char *host, int port, int retry)
{
    char    *ip, lhost[SZ_PATH];
    struct  sockaddr_in servaddr; 	/* server address 		*/
    int     ps = 0;                    	/* parallel socket descriptor	*/
    int     sndsz = TCP_WINDOW_SZ, rcvsz = TCP_WINDOW_SZ;
    //socklen_t yes = 1;
    socklen_t ctry = 0;

    pthread_mutex_t cs_mut = PTHREAD_MUTEX_INITIALIZER;


    pthread_mutex_lock (&cs_mut);

    //sndsz = rcvsz = TCP_WINDOW_MAX; 	/*  FIXME */
    sndsz = TCP_WINDOW_SZ;
    rcvsz = TCP_WINDOW_SZ;

    /* Remove any DTS server port information from the host specification.
    */
    memset (lhost, 0, SZ_PATH);
    strcpy (lhost, host);
    if ( (ip = strchr (lhost, (int) ':')) )
	*ip = '\0';

    /* Create a socket.
    */
    if ((ps = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
	fprintf (stderr, "openClientSocket(%d): %s\n", errno, strerror(errno));
        dtsError ("clientSocket: socket() fails, '%s'");
	return (-1);
    }

    //setsockopt (ps, SOL_SOCKET, SO_REUSEADDR, (void *)&yes,   sizeof(yes));
#ifdef TCP_APPTUNE
    setsockopt (ps, SOL_SOCKET, SO_SNDBUF,    (void *)&sndsz, sizeof(int));
    setsockopt (ps, SOL_SOCKET, SO_RCVBUF,    (void *)&rcvsz, sizeof(int));
#endif


    /* Set server address.
    */
    memset (&servaddr, 0, sizeof (struct sockaddr_in));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_port        = htons(port);

    /* Connect to server.
    */
    for (ctry = (retry ? 0 : SOCK_MAX_TRY); ctry <= SOCK_MAX_TRY; ctry++) {

        if ( !inet_aton(lhost, &servaddr.sin_addr) ) {
	    struct hostent *he = dts_getHostByName ( lhost );

            if (!he) {
                dtsError ("Cannot resolve address.\n");
                exit (2);
            }
            if (he->h_addrtype != AF_INET || 
	        he->h_length != sizeof (servaddr.sin_addr)) {
                    fprintf (stderr, "Cannot handle addr type %d, length %d.\n",
                        he->h_addrtype, he->h_length);
                    exit(2);
            }
            memcpy (&servaddr.sin_addr, he->h_addr_list[0], 
	        sizeof (servaddr.sin_addr) );
        }

        if (connect (ps, (struct sockaddr *)&servaddr, sizeof servaddr) < 0) {
	    /*
	    char msg[256];

	    memset (msg, 0, 256);
	    sprintf (msg, "client connect() failed to %s:%d, try %d", 
		lhost, port, ctry);
            dtsError (msg);

	    memset (msg, 0, 256);
	    sprintf (msg, "client connect() failed: '%s'", strerror (errno));
            dtsError (msg);
	    */

	    if (!retry || ctry == SOCK_MAX_TRY) {
	        fprintf (stderr, 
		    "client connect() failed to %s:%d\n", lhost, port);
	        close (ps);
		ps = -1;
		break;
	    } else
	        dtsSleep (SOCK_PAUSE_TIME);
        } else
	    break;
    }

    if (DEBUG || SOCK_DEBUG)
	fprintf (stderr, "client socket %d on %s -->  %s:%d\n", ps, 
	    dts_getLocalIP(), lhost, port );
	
	
    pthread_mutex_unlock (&cs_mut);

    return (ps);
}


/*****************************************************************************
 *****	UDT Sockets
 ****************************************************************************/

/** 
 *  dts_openUDTServerSocket -- Open a socket to be used on the 'server' side.
 *
 *  @brief  Open a socket to be used on the 'server' side
 *  @fn     int dts_openUDTServerSocket (int port)
 *
 *  @param  port	port number to open
 *  @return		socket descriptor
 *
 */
int	/* UDTSOCKET */
dts_openUDTServerSocket (int port, int rate)
{
    struct    sockaddr_in servaddr; 	/* server address 		*/
    UDTSOCKET ps = 0;                 	/* parallel socket descriptor	*/
    int32_t   ntries = 5;


    pthread_mutex_lock (&ss_mut);
    udt_startup ();

    /* Create a socket.
    */
    if ((ps = udt_socket (AF_INET, SOCK_STREAM, 0)) < 0) {
	fprintf (stderr, "openUDTServerSocket(%d): %s\n", 
	    errno, strerror(errno));
        dtsError ("serverSocket: socket()");
	return (-1);
    }
    if (rate)
        udt_setsockopt_cc (ps);        	/* Set congestion control 	*/

    if (DEBUG || SOCK_DEBUG)
	fprintf (stderr, "UDT server socket %d on %s:%d rate: %d\n", ps, 
	    dts_getLocalIP(), port, rate );


    /* Set server address.
    */
    memset (&servaddr, 0, sizeof servaddr);
    servaddr.sin_family      = AF_INET;
    servaddr.sin_port        = htons(port);
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);

    /* Bind to the server socket and listen for a connection.
    */
    while (ntries--) {
        if (udt_bind (ps, (struct sockaddr*)&servaddr, sizeof servaddr) < 0) {
            dtsErrLog (NULL, "serverSock: udt_bind(%d:%s)", 
		port, strerror (errno));
	    if (!ntries)
	        return (-1);
        } else {
            if ((udt_listen (ps, SOMAXCONN)) < 0) {
                dtsErrLog (NULL, 
		    "serverSock: listen(%d:%s)", port, strerror(errno));
	        if (!ntries)
	            return (-1);
            } else { 
	        break;
            }
	}
	dtsSleep (2);
    }

    pthread_mutex_unlock (&ss_mut);

    return (ps);
}


/** 
 *  dts_openUDTClientSocket -- Open a socket to be used on the 'client' side.
 *
 *  @brief  Open a socket to be used on the 'client' side
 *  @fn     int dts_openUDTClientSocket (char *host, int port, int retry)
 *
 *  @param  host	host name
 *  @param  port	port number to open
 *  @param  retry	attempt to reconnect?
 *  @return		socket descriptor
 *
 */
int
dts_openUDTClientSocket (char *host, int port, int retry)
{
    char    *ip, lhost[SZ_PATH];
    struct  sockaddr_in servaddr; 	/* server address 		*/
    UDTSOCKET ps = 0;                  	/* parallel socket descriptor	*/
    socklen_t ctry = 0;

    pthread_mutex_t cs_mut = PTHREAD_MUTEX_INITIALIZER;


    pthread_mutex_lock (&cs_mut);
    udt_startup();


    /* Remove any DTS server port information from the host specification.
    */
    memset (lhost, 0, SZ_PATH);
    strcpy (lhost, host);
    if ( (ip = strchr (lhost, (int) ':')) )
	*ip = '\0';

    /* Create a socket.
    */
    if ((ps = udt_socket (AF_INET, SOCK_STREAM, 0)) < 0) {
	fprintf (stderr, "openUDTClientSocket(%d): %s\n", 
	    errno, strerror(errno));
        dtsError ("clientSocket: socket() fails, '%s'");
	return (-1);
    }


    /* Set server address.
    */
    memset (&servaddr, 0, sizeof (struct sockaddr_in));
    servaddr.sin_family      = AF_INET;
    servaddr.sin_port        = htons(port);

    /* Connect to server.
    */
    for (ctry = (retry ? 0 : SOCK_MAX_TRY); ctry <= SOCK_MAX_TRY; ctry++) {

        if ( !inet_aton(lhost, &servaddr.sin_addr) ) {
	    struct hostent *he = dts_getHostByName ( lhost );

            if (!he) {
                dtsError ("Cannot resolve address.\n");
                exit (2);
            }
            if (he->h_addrtype != AF_INET || 
	        he->h_length != sizeof (servaddr.sin_addr)) {
                    fprintf (stderr, "Cannot handle addr type %d, length %d.\n",
                        he->h_addrtype, he->h_length);
                    exit(2);
            }
            memcpy (&servaddr.sin_addr, he->h_addr_list[0], 
	        sizeof (servaddr.sin_addr) );
        }

        if (udt_connect(ps, (struct sockaddr *)&servaddr,sizeof servaddr) < 0) {
	    if (!retry || ctry == SOCK_MAX_TRY) {
	        fprintf (stderr, 
		    "client UDT connect() failed to %s:%d, try %d, retry %d\n", 
		    lhost, port, ctry, retry);
	        udt_close (ps);
		ps = -1;
		break;
	    } else
	        dtsSleep (SOCK_PAUSE_TIME);
        } else
	    break;
    }

    if (DEBUG || SOCK_DEBUG)
	fprintf (stderr, "client socket %d on %s -->  %s:%d\n", ps, 
	    dts_getLocalIP(), lhost, port );
	
    pthread_mutex_unlock (&cs_mut);

    return (ps);
}




/*****************************************************************************
 *****	Utility Routines
 ****************************************************************************/

/**
 *  DTS_SOCKREAD -- Read exactly "n" bytes from a socket descriptor. 
 *
 *  @brief  Recv exactly "n" bytes from a socket descriptor. 
 *  @fn     int dts_sockRead (int fd, void *vptr, int nbytes)
 *
 *  @param  fd          file descriptor
 *  @param  vptr        data buffer to be written
 *  @param  nbytes      number of bytes to write
 *  @return             number of bytes written
 */
int
dts_sockRead (int fd, void *vptr, int nbytes)
{
    char    *ptr = vptr;
    int     nread = 0, nb = 0;
    long    nleft = nbytes;
    fd_set  allset, fds;
    extern  int psock_checksum_policy;
    unsigned int sum, nl = sizeof (int);
    char *s = (char *) &sum;
	

    /*  Set non-blocking mode on the descriptor.
     */
    dts_setNonBlock (fd);

    FD_ZERO (&allset);
    FD_SET (fd, &allset);
    memcpy (&fds, &allset, sizeof(allset));

    if (psock_checksum_policy == CS_PACKET) {
        if (select (SELWIDTH, &fds, NULL, NULL, NULL)) {
	  for (nl = sizeof (int); nl > 0; ) {
            if ( (nb = recv (fd, s, nl, 0)) < 0 )
                if (errno == EINTR || errno == EAGAIN)
                    nb = 0;             /* and call recv() again */
	    nl -= nb;
	    s  += nb;
          }
	}
    }

    while (nleft > 0) {
      memcpy (&fds, &allset, sizeof(allset));
      if (select (SELWIDTH, &fds, NULL, NULL, NULL)) {
        if ( (nb = recv (fd, ptr, nleft, 0)) < 0) {
            if (errno == EINTR || errno == EAGAIN)
                nb = 0;             /* and call recv() again */
            else {
		dtsErrLog (NULL, "dts_sockRead: %s\n", strerror (errno));
                return (-1);
	    }
        } else if (nb == 0)
            break;                  /* EOF */

        nleft -= nb;
        ptr   += nb;
        nread += nb;
      }
    }

    if (psock_checksum_policy == CS_PACKET) {
	unsigned int rsum = dts_memCRC32 ((unsigned char *)vptr,(size_t)nbytes);
	int resend = (rsum != sum);
	char *s = (char *) &resend;

	for (nl = sizeof(int); nl > 0; ) {
            if ( (nb = send (fd, s, nl, 0)) < 0 )
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                    nb = 0;             /* and call send() again */
	    nl -= nb;
	    s += nb;
	}
    }

    if (nleft != 0)
	dtsErrLog (NULL, "dts_sockRead: Error  nleft = %d\n", nleft);

    return (nread);                 /* return no. of bytes read */
}


/**
 *  DTS_SOCKWRITE -- Write exactly "n" bytes to a socket descriptor. 
 *
 *  @brief  Send exactly "n" bytes to a socket descriptor. 
 *  @fn     int dts_sockWrite (int fd, void *vptr, int nbytes)
 *
 *  @param  fd          file descriptor
 *  @param  vptr        data buffer to be written
 *  @param  nbytes      number of bytes to write
 *  @return             number of bytes written
 */
int
dts_sockWrite (int fd, void *vptr, int nbytes)
{
    fd_set   allset, fds;
    char    *ptr = vptr;
    int      nwritten = 0,  nb = 0, nl = sizeof (int);
    long     nleft = nbytes;
    unsigned int  resend;

    extern  int psock_checksum_policy;


    /*  Set non-blocking mode on the descriptor.
     */
    dts_setNonBlock (fd);

resend_packet:
    FD_ZERO (&allset);
    FD_SET (fd, &allset);
    memcpy (&fds, &allset, sizeof(allset));

    if (psock_checksum_policy == CS_PACKET) {
        unsigned int sum = dts_memCRC32 ((unsigned char *)vptr, (size_t)nbytes);
	char *s = (char *) &sum;
	
	for (nl = sizeof (int); nl > 0; ) {
          if (select (SELWIDTH, NULL, &fds, NULL, NULL)) {
            if ( (nb = send (fd, s, nl, 0)) < 0 )
                if (errno == EINTR || errno == EAGAIN)
                    nb = 0;             /* and call send() again */
	    nl -= nb;
	    s += nb;
          }
	}
    }

    while (nleft > 0) {
      memcpy (&fds, &allset, sizeof(allset));
      if (select (SELWIDTH, NULL, &fds, NULL, NULL)) {
        if ( (nb = send (fd, ptr, nleft, 0)) < 0 ) {
            if (errno == EINTR || errno == EAGAIN)
                nb = 0;             /* and call send() again */
            else {
		dtsErrLog (NULL, "dts_sockWrite: %s\n", strerror (errno));
                return (-1);
	    }

        } else if (nb > 0) {
            nleft    -= nb;
            ptr      += nb;
            nwritten += nb;
        }
      }
    }

    if (psock_checksum_policy == CS_PACKET) {
	char *s = (char *) &resend;

	for (nl = sizeof (int); nl > 0; ) {
          memcpy (&fds, &allset, sizeof(allset));
          if (select (SELWIDTH, &fds, NULL, NULL, NULL)) {
            if ( (nb = recv (fd, s, nl, 0)) < 0 )
                if (errno == EINTR || errno == EAGAIN)
                    nb = 0;             /* and call recv() again */
	    nl -= nb;
	    s  += nb;
          }
	}

	if (resend) {
	    dtsErrLog (NULL, "sockWrite: resending packet ....\n");
	    goto resend_packet;
	}
    }

    if (nleft != 0)
	dtsErrLog (NULL, "dts_sockWrite: Error  nleft = %d\n", nleft);

    return (nwritten);
}


/** 
 *  DTS_SETNONBLOCK -- Set a non-blocking mode on the descriptor.
 */
void
dts_setNonBlock (int sock)
{
    int flags;


    /* Set socket to non-blocking.
    */
    if ((flags = fcntl (sock, F_GETFL, 0)) < 0) {
        /* Handle error */
	return;
    }
    if (fcntl (sock, F_SETFL, flags | O_NONBLOCK) < 0) {
        /* Handle error */
	return;
    }
}


/** 
 *  DTS_SETBLOCK -- Set a blocking mode on the descriptor.
 */
void
dts_setBlock (int sock)
{
    int flags;


    /* Set socket to non-blocking.
    */
    if ((flags = fcntl (sock, F_GETFL, 0)) < 0) {
        /* Handle error */
	return;
    }
    if (fcntl (sock, F_SETFL, flags & (~O_NONBLOCK)) < 0) {
        /* Handle error */
	return;
    }
}


/**
 *  DTS_UDTREAD -- Read exactly "n" bytes from a UDT socket descriptor. 
 *
 *  @brief  Recv exactly "n" bytes from a UDT socket descriptor. 
 *  @fn     int dts_udtRead (int fd, void *vptr, long nbytes, int flags)
 *
 *  @param  fd          file descriptor
 *  @param  vptr        data buffer to be written
 *  @param  nbytes      number of bytes to write
 *  @param  flags       option flags
 *  @return             number of bytes written
 */
int
dts_udtRead (int fd, void *vptr, long nbytes, int flags)
{
    char    *ptr = vptr;
    int     nread = 0, nb = 0;
    long    nleft = nbytes;
    extern  int udt_checksum_policy;
    unsigned int sum, nl = sizeof (int);
    char *s = (char *) &sum;
	

    /*  Set non-blocking mode on the descriptor.
    dts_setNonBlock (fd);
     */

    if (udt_checksum_policy == CS_PACKET) {
	for (nl = sizeof (int); nl > 0; ) {
            if ( (nb = udt_recv (fd, s, nl, flags)) < 0 )
                nb = 0;             /* and call recv() again */
	    nl -= nb;
	    s  += nb;
        }
    }

    while (nleft > 0) {
        if ( (nb = udt_recv (fd, ptr, nleft, flags)) < 0) {
            nb = 0;             /* and call recv() again */
        } else if (nb == 0)
            break;                  /* EOF */

        nleft -= nb;
        ptr   += nb;
        nread += nb;
    }

    if (udt_checksum_policy == CS_PACKET) {
	unsigned int rsum = dts_memCRC32 ((unsigned char *)vptr,(size_t)nbytes);
	int resend = (rsum != sum);
	char *s = (char *) &resend;

	for (nl = sizeof(int); nl > 0; ) {
            if ( (nb = udt_send (fd, s, nl, 0)) < 0 )
                nb = 0;             /* and call send() again */
	    nl -= nb;
	    s += nb;
	}
    }

    if (nleft != 0)
	dtsErrLog (NULL, "dts_udtRead: Error  nleft = %d\n", nleft);

    return (nread);                 /* return no. of bytes read */
}


/**
 *  DTS_UDTWRITE -- Write exactly "n" bytes to a UDT socket descriptor. 
 *
 *  @brief  Send exactly "n" bytes to a UDT socket descriptor. 
 *  @fn     int dts_udtWrite (int fd, void *vptr, long nbytes, int flags)
 *
 *  @param  fd          file descriptor
 *  @param  vptr        data buffer to be written
 *  @param  nbytes      number of bytes to write
 *  @param  flags       option flags
 *  @return             number of bytes written
 */
int
dts_udtWrite (int fd, void *vptr, long nbytes, int flags)
{
    char    *ptr = vptr;
    int      nwritten = 0,  nb = 0, nl = sizeof (int);
    long     nleft = nbytes;
    unsigned int  resend;

    extern  int udt_checksum_policy;


    /*  Set non-blocking mode on the descriptor.
     */
    dts_setNonBlock (fd);

resend_packet:
    if (udt_checksum_policy == CS_PACKET) {
        unsigned int sum = dts_memCRC32 ((unsigned char *)vptr, (size_t)nbytes);
	char *s = (char *) &sum;
	
	for (nl = sizeof (int); nl > 0; ) {
            if ( (nb = udt_send (fd, s, nl, flags)) < 0 )
                nb = 0;             /* and call send() again */
	    nl -= nb;
	    s += nb;
	}
    }

    while (nleft > 0) {
        if ( (nb = udt_send (fd, ptr, nleft, flags)) < 0 ) {
            nb = 0;             /* and call udt_send() again */

        } else if (nb > 0) {
            nleft    -= nb;
            ptr      += nb;
            nwritten += nb;
        }
    }

    if (udt_checksum_policy == CS_PACKET) {
	char *s = (char *) &resend;

	for (nl = sizeof (int); nl > 0; ) {
            if ( (nb = udt_recv (fd, s, nl, 0)) < 0 )
                nb = 0;             /* and call udt_recv() again */
	    nl -= nb;
	    s  += nb;
	}

	if (resend) {
	    dtsErrLog (NULL, "udtWrite: resending packet ....\n");
	    goto resend_packet;
	}
    }

    if (nleft != 0)
	dtsErrLog (NULL, "dts_udtWrite: Error  nleft = %d\n", nleft);

    return (nwritten);
}
