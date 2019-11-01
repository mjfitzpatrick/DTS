#///////////////////////////////////////////////////////////////////////////////
#//
#//  DTS -- Makefile for the Data Transfer System daemon and support code.
#//
#///////////////////////////////////////////////////////////////////////////////

# primary dependencies

NAME 	  	= dts
VERSION   	= 0.1
PLATFORM  	= $(shell uname -s)
PLARCH  	= $(shell uname -m)
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
C++ 		= g++
CINCS  		= -I$(HERE) -I../ -I../include #-I../libxrpc/include
CFLAGS 		= -g -D$(PLATFORM) -Wall $(CINCS) -D_FILE_OFFSET_BITS=64 $(CARCH)
LFLAGS		= -L. -L../lib #-static

ifeq ($(PLATFORM),Darwin)
    ifeq  ($(PLARCH),x86_64)
        CARCH   = -m64 -mmacosx-version-min=10.8
    else
        CARCH   = -m32 -mmacosx-version-min=10.8
    endif
    LIBS	= -ldts -ldb -lxrpc $(DBG_LIB) -lm -lpthread -ldl -lcurl
else
    CARCH       = 
    LIBS	= -ldts -ldb -lxrpc $(DBG_LIB) -lrt -lm -lpthread -ldl
endif



DBG_LIB		=  #/usr/lib/libefence.a
#LIBS		= -static -ldts -ldb -lxrpc $(DBG_LIB) -lpthread -lrt -lm -ldl
#LIBS		= -ldts -ldb -lxrpc $(DBG_LIB) -lpthread -lrt -lm -ldl

CLIBS		= -lpthread -lm
DTS_LIBS 	= -ldts -ldb
XRPC_LIBS 	= -lxrpc
PSOCK_LIBS 	= -lpsock-mt			# not currently used

DEP_LIBS	= ../lib/libdts.a ../lib/libxrpc.a
DEP_INCS	= ../build.h ../include/dts.h ../include/dtsMethods.h


# list of source and include files
DTS_SRCS 	= dtsd.c
DTS_OBJS 	= dtsd.o

DTSQ_SRCS 	= dtsq.c
DTSQ_OBJS 	= dtsq.o

DTSMON_SRCS 	= dtsmon.c
DTSMON_OBJS 	= dtsmon.o

DTSH_SRCS 	= dtsh.c
DTSH_OBJS 	= dtsh.o

DTSTAT_SRCS 	= dtstat.c
DTSTAT_OBJS 	= dtstat.o

DTLOG_SRCS 	= dtslog.c
DTLOG_OBJS 	= dtslog.o

DTSCP_SRCS 	= dtscp.c
DTSCP_OBJS 	= dtscp.o

DTS_SVR_SRCS 	= dts_svr.c
DTS_SVR_OBJS 	= dts_svr.o

TARGETS		= dtsd dtsq dtscp dtsmon dtsh dtstat dtslog 


# targets

all:
	make apps

apps:
	@echo "static char *build_date = \""`date`"\";"  > ../build.h
	make dtsd
	make dtsq
	make dtsmon
	make dtsh
	make dtstat
	make dtscp
	make dtslog
	/bin/rm -f core core.* */core*
	#/usr/bin/scp .alias dts*  denali:dts/src/
	cp $(TARGETS) ../bin/

clean:
	/bin/rm -rf *.[aeo] $(TARGETS)

everything:
	make clean
	make all
	make install


install: all 
	mv $(TARGETS) ../bin/


# Default config files to be builtin to each binary.
des_defaults: cfg/des.dts_config cfg/des.dtsq_config
	sed -f gui2c.sed cfg/des.dts_config   > def_dtsConfig.h
	sed -f gui2c.sed cfg/des.dtsq_config  > def_dtsqConfig.h

odi_defaults: cfg/odi.dts_config cfg/odi.dtsq_config
	sed -f gui2c.sed cfg/odi.dts_config   > def_dtsConfig.h
	sed -f gui2c.sed cfg/odi.dtsq_config  > def_dtsqConfig.h


# Main package programs to be built.

dtsd: $(DTS_OBJS) $(DEP_LIBS) $(DEP_INCS)
	$(C++) $(CFLAGS) -o dtsd $(DTS_OBJS) $(LFLAGS) $(LIBS) -ludt

dtsq: $(DTSQ_OBJS) $(DEP_LIBS) $(DEP_INCS)
	$(C++) $(CFLAGS) -o dtsq $(DTSQ_OBJS) $(LFLAGS) $(LIBS) -ludt

dtsmon: $(DTSMON_OBJS) $(DEP_LIBS) $(DEP_INCS)
	$(CC) $(CFLAGS) -o dtsmon $(DTSMON_OBJS) $(LFLAGS) $(LIBS)

dtsh: $(DTSH_OBJS) $(DEP_LIBS) $(DEP_INCS)
	$(C++) $(CFLAGS) -o dtsh $(DTSH_OBJS) $(LFLAGS) $(LIBS) -ludt

dtstat: $(DTSTAT_OBJS) $(DEP_LIBS) $(DEP_INCS)
	$(C++) $(CFLAGS) -o dtstat $(DTSTAT_OBJS) $(LFLAGS) $(LIBS) -ludt

dtscp: $(DTSCP_OBJS) $(DEP_LIBS) $(DEP_INCS)
	$(C++) $(CFLAGS) -o dtscp $(DTSCP_OBJS) $(LFLAGS) $(LIBS) -ludt

dtslog: $(DTLOG_OBJS) $(DEP_LIBS) $(DEP_INCS)
	$(C++) $(CFLAGS) -o dtslog $(DTLOG_OBJS) $(LFLAGS) $(LIBS) -ludt


dts_svr: $(DTS_SVR_OBJS) $(DEP_LIBS) $(DEP_INCS)
	$(CC) $(CFLAGS) -o dts_svr $(DTS_SVR_OBJS) $(LFLAGS) $(LIBS)


# Generic test program

xtest: xtest.c $(DEP_LIBS) $(DEP_INCS)
	$(CC) $(CFLAGS) -o xtest xtest.c $(LFLAGS) $(LIBS)

zz: zz.o $(DEP_LIBS) $(DEP_INCS)
	$(CC) -w $(CFLAGS) -o zz zz.o $(LFLAGS) $(LIBS)

zzs: zzs.o $(DEP_LIBS) $(DEP_INCS)
	$(CC) -w $(CFLAGS) -o zzs zzs.o $(LFLAGS) $(LIBS)



#=======================
# leave this stuff alone
#=======================

%.o: %.c $(DEP_INCS)
	$(CC) -Wall $(CINCS) $(CFLAGS) -c $< -o $@
