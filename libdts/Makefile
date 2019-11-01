#///////////////////////////////////////////////////////////////////////////////
#//
#//  DTS -- Makefile for the Data Transfer System daemon and support code.
#//
#///////////////////////////////////////////////////////////////////////////////

# primary dependencies

NAME 	  	= dts
VERSION   	= 1.0
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
CFLAGS 		= -g -O3 -D$(PLATFORM) -Wall $(CINCS) -D_FILE_OFFSET_BITS=64
OFLAGS 		= -O3 -D$(PLATFORM) -Wall $(CINCS)
LFLAGS		= -L. -L../lib

DBG_LIB		= -lefence
CLIBS		= -lpthread -lm
XRPC_LIBS 	= -lxrpc
UDT_LIBS 	= -ludt
PSOCK_LIBS 	= -lpsock-mt			# not currently used
LIBS		= -ldts -lpthread -lm -lxrpc -ludt $(DBG_LIB)


# list of source and include files
DTS_SRCS 	= dts.c dtsClient.c dtsMethods.c dtsASync.c dtsPSock.c \
		  dtsConfig.c dtsConsole.c dtsLog.c dtsUtil.c \
		  dtsPush.c dtsPull.c dtsFileUtil.c dtsSockUtil.c \
		  dtsXfer.c dtsCommands.c dtsSandbox.c dtsLocal.c \
		  dtsChecksum.c dtsTar.c dtsQueue.c dtsQueueUtil.c \
		  dtsQueueStat.c dtsDeliver.c dtsIngest.c dtsSem.c dtsUDT.c
DTS_OBJS 	= dts.o dtsClient.o dtsMethods.o dtsASync.o dtsPSock.o \
		  dtsConfig.o dtsConsole.o dtsLog.o dtsUtil.o \
		  dtsPush.o dtsPull.o dtsFileUtil.o dtsSockUtil.o \
		  dtsXfer.o dtsCommands.o dtsSandbox.o dtsLocal.o \
		  dtsChecksum.o dtsTar.o dtsQueue.o dtsQueueUtil.o \
		  dtsQueueStat.o dtsDeliver.o dtsIngest.o dtsSem.o dtsUDT.o
DTS_INCS 	= dts.h dtsPSock.h dtsMethods.h dtsTar.h dtsUDT.h

TARGETS		= libdts



# targets

all:
	@echo "static char *build_date = \""`date`"\";"  > ../build.h
	make libs

all_static: Static $(STATICLIB)

all_shared: Shared $(SHAREDLIB)


libs:
	make install

clean:
	/bin/rm -rf *.[aeo]

libdts: $(DTS_OBJS)
	ar r libdts.a $(DTS_OBJS)

everything:
	make clean
	make all
	make install

install: libdts
	make libdts
	cp libdts.a ../lib/
	cp dts.h dtsPSock.h dtsMethods.h ../include/



# Unit test programs to be built.

dts: $(DTS_OBJS)
	$(CC) $(CFLAGS) -o dts $(DTS_OBJS) $(LFLAGS) $(LIBS)



#=======================
# leave this stuff alone
#=======================

dtsChecksum.o: dtsChecksum.c
	$(CC) -Wall $(CINCS) $(OFLAGS) -g -c dtsChecksum.c

%.o: %.c $(DTS_INCS)
	$(CC) -Wall $(CINCS) $(CFLAGS) -c $< -o $@


$(STATICLIB): $(SRCS:%.c=Static/%.o)
	/usr/bin/ar rv $@ $?
Static/%.o: %.c $(INCS)
	/usr/bin/gcc $(CINCS) $(CFLAGS) -c $< -o $@
Static:
	/bin/mkdir $@
	chmod 777 $@

$(SHAREDLIB): $(SRCS:%.c=Shared/%.o)
	/usr/bin/ld -shared -o $@ $? -lc -ldl
Shared/%.o: %.c $(INCS)
	/usr/bin/gcc $(CINCS) $(CFLAGS) -fpic -shared -c $< -o $@
Shared:
	/bin/mkdir $@
	chmod 777 $@

