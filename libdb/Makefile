#///////////////////////////////////////////////////////////////////////////////
#//
#//
#///////////////////////////////////////////////////////////////////////////////

# primary dependencies

NAME     = db
VERSION  = 1.0
HERE    := $(shell /bin/pwd)


# secondary dependencies

LIBBASE     = lib$(NAME)
STATICLIB   = $(HERE)/$(LIBBASE).a
SHAREDLIB   = $(HERE)/$(LIBBASE).so.$(VERSION)


# stuff that's precious to keep

.PRECIOUS:
.KEEP_STATE:


# includes, flags and libraries
CINCS   = -I./ -I../include
#CFLAGS  = -m32 -g -Wall $(CINCS)
CFLAGS  = -O2 -Wall -g $(CINCS) -mmacosx-version-min=10.8 -m64


# list of source and include files

SRCS  = dtsdb.c
OBJS  = dtsdb.o
INCS  = dtsdb.h


LFLAGS 	= -L. -L./lib -lpthread -ldl
LIBS 	=


# targets

all:  base db

clean:
	(./mkclean)
	/bin/rm -rf Shared Static UnitTests/* *.o *.a *.e

install: base db
	(mv sqlite/.libs/libsqlite3.a ./libdb.a ; ar rv libdb.a dtsdb.o ; ranlib libdb.a ; cp libdb.a ../lib)
	(cp dtsdb.h sqlite/sqlite3.h ../include ; /bin/rm -rf bin share lib include)

World:
everything:
	make clean
	make db
	make install

include:

#
base:
	(./mklibs)
	(mv sqlite/.libs/libsqlite3.a ./libdb.a)

zztest: zztest.c db
	cc -g -o zztest zztest.c libdb.a $(LFLAGS)

db: $(SRCS:%.c=%.o)
	ar rv libdb.a $?


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
