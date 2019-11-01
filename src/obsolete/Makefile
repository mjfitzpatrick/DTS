#///////////////////////////////////////////////////////////////////////////////
#//
#//  DTS -- Makefile for the Data Transfer System daemon and support code.
#//
#///////////////////////////////////////////////////////////////////////////////

# primary dependencies

NAME 	  	= dts
VERSION   	= 0.1
PLATFORM  	= $(shell uname -s)
HERE 	  	:= $(shell /bin/pwd)


# secondary dependencies

LIBBASE 	= lib$(NAME)
STATICLIB 	= $(HERE)/$(LIBBASE).a
SHAREDLIB 	= $(HERE)/$(LIBBASE).so.$(VERSION)


# stuff that's precious to keep

.PRECIOUS:	$(STATICLIB) $(SHAREDLIB)
.KEEP_STATE:


# includes, flags and libraries
CC 		= gcc
CINCS  		= -I$(HERE) -I../include
CFLAGS 		= -g -D$(PLATFORM) -m32 -Wall $(CINCS)
LFLAGS		= -L. -L../lib

DBG_LIB		= #-lefence
LIBS		= -ldts -lpthread -lm -lxrpc $(DBG_LIB) #-lcurl

CLIBS		= -lpthread -lm
DTS_LIBS 	= -ldts
XRPC_LIBS 	= -lxrpc
PSOCK_LIBS 	= -lpsock-mt			# not currently used

DEP_LIBS	= ../lib/libdts.a ../lib/libxrpc.a


# list of source and include files
DTS_SRCS 	= dtsd.c
DTS_OBJS 	= dtsd.o

DTSQ_SRCS 	= dtsq.c
DTSQ_OBJS 	= dtsq.o

DTSMON_SRCS 	= dtsmon.c
DTSMON_OBJS 	= dtsmon.o

DTSCP_SRCS 	= dtscp.c
DTSCP_OBJS 	= dtscp.o

DTSCMD_SRCS 	= dtscmd.c
DTSCMD_OBJS 	= dtscmd.o

DTSH_SRCS 	= dtsh.c
DTSH_OBJS 	= dtsh.o

XFER_SRCS 	= xfer.c
XFER_OBJS 	= xfer.o

TARGETS		= dtsd dtsq dtsmon dtscmd dtsh dtscp xfer



# targets

all:
	make apps

apps:
	make dtsd
	make dtsq
	make dtsmon
	#make dtscp
	make dtscmd
	make dtsh

clean:
	/bin/rm -rf *.[aeo] $(TARGETS)

everything:
	make clean
	make all
	make install

install: all 
	mv $(TARGETS) ../bin/



# Unit test programs to be built.

dtsd: $(DTS_OBJS) $(DEP_LIBS)
	$(CC) $(CFLAGS) -o dtsd $(DTS_OBJS) $(LFLAGS) $(LIBS)

dtsq: $(DTSQ_OBJS) $(DEP_LIBS)
	$(CC) $(CFLAGS) -o dtsq $(DTSQ_OBJS) $(LFLAGS) $(LIBS)

dtsmon: $(DTSMON_OBJS) $(DEP_LIBS)
	$(CC) $(CFLAGS) -o dtsmon $(DTSMON_OBJS) $(LFLAGS) $(LIBS)

#dtscp: $(DTSCP_OBJS) $(DEP_LIBS)
#	$(CC) $(CFLAGS) -o dtscp $(DTSCP_OBJS) $(LFLAGS) $(LIBS)

dtscmd: $(DTSCMD_OBJS) $(DEP_LIBS)
	$(CC) $(CFLAGS) -o dtscmd $(DTSCMD_OBJS) $(LFLAGS) $(LIBS)

dtsh: $(DTSH_OBJS) $(DEP_LIBS)
	$(CC) $(CFLAGS) -o dtsh $(DTSH_OBJS) $(LFLAGS) $(LIBS)

#xfer: $(XFER_OBJS) $(DEP_LIBS)
#	$(CC) $(CFLAGS) -o xfer $(XFER_OBJS) $(LFLAGS) $(LIBS)





#=======================
# leave this stuff alone
#=======================

%.o: %.c
	$(CC) -Wall $(CINCS) $(CFLAGS) -c $< -o $@
