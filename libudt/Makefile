#///////////////////////////////////////////////////////////////////////////////
#//
#//
#///////////////////////////////////////////////////////////////////////////////

# primary dependencies

NAME            = udt
VERSION         = 1.0
HERE            := $(shell /bin/pwd)
PLATFORM        := $(shell uname -s)
PLMACH          := $(shell uname -m)


CINCS           = -I$(INCDIR)  -I.
C++ 		= g++ -g


CC              = gcc
CINCS           = -I$(INCDIR)  -I.
ifeq  ($(PLMACH),x86_64)
    CARCH   = -DAMD64 -m64 -mmacosx-version-min=10.8
else
    CARCH   = -DIA32 -m32
endif
    CARCH   = -DAMD64 -m64 -mmacosx-version-min=10.8

#ifndef os
#   os = LINUX
#endif
#
#ifndef arch
#   arch = IA32
#endif

CCFLAGS = -fPIC -Wall -Wextra -D$(PLATFORM) -finline-functions -O3 -fno-strict-aliasing $(CARCH) #-msse3

OBJS = udtc.o md5.o common.o window.o list.o buffer.o packet.o channel.o queue.o ccc.o cache.o core.o epoll.o api.o
DIR = $(shell pwd)



all: libudt.a

%.o: %.cpp %.h udt.h udtc.h
	$(C++) $(CCFLAGS) $< -c

libudt.so: $(OBJS)
ifneq ($(PLATFORM), Darwin)
	$(C++) -shared -o $@ $^
else
	$(C++) -dynamiclib -o libudt.dylib -lstdc++ -lpthread -lm $^
endif

libudt.a: $(OBJS)
	#ar -rcs $@ $^
	ar -rc $@ $^
	cp udt.h udtc.h ../include/
	cp libudt.a ../lib

install:
	cp udt.h udtc.h ../include
	cp libudt.a ../lib

clean:
	rm -f *.o *.so *.dylib *.a udt

