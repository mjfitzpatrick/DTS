#///////////////////////////////////////////////////////////////////////////////
#//
#//
#///////////////////////////////////////////////////////////////////////////////

# primary dependencies

NAME     	= xrpc
VERSION  	= 1.0
HERE    	:= $(shell /bin/pwd)
PLATFORM        := $(shell uname -s)
PLMACH          := $(shell uname -m)


# secondary dependencies

LIBBASE     = lib$(NAME)
STATICLIB   = $(HERE)/$(LIBBASE).a
SHAREDLIB   = $(HERE)/$(LIBBASE).so.$(VERSION)


# stuff that's precious to keep

.PRECIOUS:
.KEEP_STATE:


CC              = gcc
CINCS           = -I$(INCDIR)  -I.
ifeq ($(PLATFORM),Darwin)
    ifeq  ($(PLMACH),x86_64)
        CARCH   = -m64 -mmacosx-version-min=10.7
    else
        CARCH   = -m32 -mmacosx-version-min=10.7
    endif
else
    CARCH       = 
endif


# includes, flags and libraries
CINCS   = -I./ -I../include -Ixmlrpc-c/lib/abyss/src
CFLAGS  = $(CARCH) -g -Wall $(CINCS) -D$(PLATFORM)


# list of source and include files

SRCS  = xrClient.c xrServer.c xrMethod.c xrUtil.c xrStruct.c xrArray.c \
	xrHandles.c
OBJS  = xrClient.o xrServer.o xrMethod.o xrUtil.o xrStruct.o xrArray.o \
	xrHandles.o
INCS  = xrpc.h xrpcP.h


LFLAGS 	= -L. -L./lib 
LIBS 	=


# targets

all:  xrpc

clean:
	(./mkclean)
	/bin/rm -rf Shared Static UnitTests/* *.o *.a *.e

install: xrpc
	(cp libxrpc.a ../lib)
	cp -rp $(INCS) ./include/* ../include

World:
everything:
	make clean
	make xrpc
	make base
	make install

include:

#
base:
	(./mklibs)


xrpc: $(SRCS:%.c=%.o)
	cp $(OBJS) lib/build/
	/usr/bin/ar rv libxrpc.a $?
	(cp libxrpc.a ../lib)
	cp -rp $(INCS) ./include/* ../include

%.o: %.c $(INCS)
	/usr/bin/gcc -Wall $(CINCS) $(CFLAGS) -c $< -o $@


#=======================
# leave this stuff alone
#=======================

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
