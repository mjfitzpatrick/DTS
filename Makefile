#
#  Makefile for the DTS source tree.
#
# ---------------------------------------------------------------------------


# primary dependencies

NAME            = DTS
VERSION         = 1.3
HERE            := $(shell /bin/pwd)
PLATFORM        := $(shell uname -s)
PLMACH          := $(shell uname -m)


# Compiler Flags.

CFLAGS 		=
CDEBUGFLAGS 	= -O2 -Wall
BOOTSTRAPCFLAGS = -O2  -pipe -march=i386 -mcpu=i686 -pipe
        
CC 		= gcc -m32
AS 		= gcc -m32 -c -x assembler
AR 		= ar clq
CP 		= cp -p


RELEASE		= v$(VERSION)

LIBDIRS 	= libxrpc libdts
LIBXRPC  	= libxrpc
LIBDTS  	= libdts
LIBUDT  	= libudt
LIBDB   	= libdb
LIBFUSE   	= libfuse
APPDIRS 	= src
SUBDIRS 	= $(LIBDIRS) $(APPDIRS)

all:: libs apps

World::
	@echo "Building the DTS $(RELEASE) software tree"
	@echo "" ; date ; echo ""
	@echo ""
	@echo "static char *build_date = \""`date`"\";"  > build.h
	@if [ -d include ]; then \
	set +x; \
	else \
	if [ -h include ]; then \
	(set -x; rm -f include); \
	fi; \
	(set -x; $(MKDIRHIER) include); \
	fi
	$(MAKE) $(MFLAGS) libs      DTSDIR=$$PWD
	$(MAKE) $(MFLAGS) apps	    DTSDIR=$$PWD
	$(MAKE) $(MFLAGS) install   DTSDIR=$$PWD
	@echo "" ; date ; echo ""
	@echo "Done1."

update::
	@echo "Updating the DTS $(RELEASE) software tree"
	@echo "" ; date ; echo ""
	@echo ""
	@echo "static char *build_date = \""`date`"\";"  > build.h
	$(MAKE) $(MFLAGS) libdts    DTSDIR=$$PWD
	$(MAKE) $(MFLAGS) apps      DTSDIR=$$PWD
	$(MAKE) $(MFLAGS) install   DTSDIR=$$PWD
	@echo "" ; date ; echo ""
	@echo "Done2."

libdts::
	(cd libdts ; echo "making all in libdts...";\
	 	$(MAKE) $(MFLAGS) 'CDEBUGFLAGS=$(CDEBUGFLAGS)' all);

libxrpc::
	(cd libxrpc ; echo "making all in libxrpc...";\
	 	$(MAKE) $(MFLAGS) 'CDEBUGFLAGS=$(CDEBUGFLAGS)' everything);

libudt::
	(cd libudt ; echo "making all in libudt...";\
	 	$(MAKE) $(MFLAGS) 'CDEBUGFLAGS=$(CDEBUGFLAGS)' all);

libfuse::
	(cd libfuse ; echo "making all in libfuse..."; \
		./configure --prefix=`pwd`/../; \
	 	$(MAKE) $(MFLAGS) 'CDEBUGFLAGS=$(CDEBUGFLAGS)' all;\
		cp lib/.libs/libfuse.a ../lib;\
		cp -rp include/fuse ../include);

libdb::
	(cd libdb ; echo "making all in libdb...";\
	 	$(MAKE) $(MFLAGS) 'CDEBUGFLAGS=$(CDEBUGFLAGS)' install);


libs::
	$(MAKE) $(MFLAGS) libdb     DTSDIR=$$PWD
	$(MAKE) $(MFLAGS) libudt    DTSDIR=$$PWD
	$(MAKE) $(MFLAGS) libxrpc   DTSDIR=$$PWD
	$(MAKE) $(MFLAGS) libdts    DTSDIR=$$PWD
ifneq ($(PLATFORM),Darwin)
	$(MAKE) $(MFLAGS) libfuse   DTSDIR=$$PWD
endif

apps::
	(cd src ; echo "making all in src...";\
	 	$(MAKE) $(MFLAGS) 'CDEBUGFLAGS=$(CDEBUGFLAGS)' all);


patch::
	(tar -czf dtp.tgz */*.[ch] */*/*.[ch])


archive::
	$(MAKE) $(MFLAGS) pristine
	@(tar -cf - . | gzip > ../dts-$(RELEASE)-src.tar.gz)

pristine::
	$(MAKE) $(MFLAGS) cleandir
	$(MAKE) $(MFLAGS) generic
	$(RM) -rf bin.[a-fh-z]* lib.[a-fh-z]* bin.tar* include *spool* \
		Makefile makefile Makefile.bak */Makefile */Makefile.bak \
		**/Makefile.bak
	$(RM) -f   bin/*
	$(RM) -rf  lib/*
	$(RM) -rf  */core* */*/core* */*/*/core*



# ----------------------------------------------------------------------
# common rules for all Makefiles - do not edit

.c.i:
	$(RM) $@
	$(CC) -E $(CFLAGS) $(_NOOP_) $*.c > $@

.SUFFIXES: .s

.c.s:
	$(RM) $@
	$(CC) -S $(CFLAGS) $(_NOOP_) $*.c

emptyrule::

cleandir::
	(cd libdts  ; make clean)
	(cd libxrpc ; make clean)
	(cd libdb   ; make clean)
	(cd libudt  ; make clean)
ifneq ($(PLATFORM),Darwin)
	(cd libfuse ; make clean)
endif
	(cd tests   ; make clean)
	(cd src     ; make clean)
	$(RM) *.CKP *.ln *.BAK *.bak *.o core errs ,* *~ *.a .emacs_* tags TAGS make.log MakeOut   "#"*
	$(RM) -rf   bin/* lib/* include/* *spool* */*spool* */*/*spool*


Makefile::
	-@if [ -f Makefile ]; then set -x; \
	$(RM) Makefile.bak; $(MV) Makefile Makefile.bak; \
	else exit 0; fi
	$(IMAKE_CMD) -DTOPDIR=$(TOP) -DCURDIR=$(CURRENT_DIR)

tags::
	$(TAGS) -w *.[ch]
	$(TAGS) -xw *.[ch] > TAGS


clean:: cleandir

distclean:: cleandir



# ----------------------------------------------------------------------
# rules for building in SUBDIRS - do not edit

install::
	@for flag in ${MAKEFLAGS} ''; do \
	case "$$flag" in *=*) ;; --*) ;; *[ik]*) set +e;; esac; done; \
	for i in $(SUBDIRS) ;\
	do \
	echo "installing" "in $(CURRENT_DIR)/$$i..."; \
	$(MAKE) -C $$i $(MFLAGS) $(PARALLELMFLAGS) DESTDIR=$(DESTDIR) install; \
	done

install.man::
	@for flag in ${MAKEFLAGS} ''; do \
	case "$$flag" in *=*) ;; --*) ;; *[ik]*) set +e;; esac; done; \
	for i in $(SUBDIRS) ;\
	do \
	echo "installing man pages" "in $(CURRENT_DIR)/$$i..."; \
	$(MAKE) -C $$i $(MFLAGS) $(PARALLELMFLAGS) DESTDIR=$(DESTDIR) install.man; \
	done

Makefiles::
	-@for flag in ${MAKEFLAGS} ''; do \
	case "$$flag" in *=*) ;; --*) ;; *[ik]*) set +e;; esac; done; \
	for flag in ${MAKEFLAGS} ''; do \
	case "$$flag" in *=*) ;; --*) ;; *[n]*) executeit="no";; esac; done; \
	for i in $(SUBDIRS) ;\
	do \
	case "$(CURRENT_DIR)" in \
	.) curdir= ;; \
	*) curdir=$(CURRENT_DIR)/ ;; \
	esac; \
	echo "making Makefiles in $$curdir$$i..."; \
	itmp=`echo $$i | sed -e 's;^\./;;g' -e 's;/\./;/;g'`; \
	curtmp="$(CURRENT_DIR)" \
	toptmp=""; \
	case "$$itmp" in \
	../?*) \
	while echo "$$itmp" | grep '^\.\./' > /dev/null;\
	do \
	toptmp="/`basename $$curtmp`$$toptmp"; \
	curtmp="`dirname $$curtmp`"; \
	itmp="`echo $$itmp | sed 's;\.\./;;'`"; \
	done \
	;; \
	esac; \
	case "$$itmp" in \
	*/?*/?*/?*/?*)	newtop=../../../../..;; \
	*/?*/?*/?*)	newtop=../../../..;; \
	*/?*/?*)	newtop=../../..;; \
	*/?*)		newtop=../..;; \
	*)		newtop=..;; \
	esac; \
	newtop="$$newtop$$toptmp"; \
	case "$(TOP)" in \
	/?*) imaketop=$(TOP) \
	imakeprefix= ;; \
	.) imaketop=$$newtop \
	imakeprefix=$$newtop/ ;; \
	*) imaketop=$$newtop/$(TOP) \
	imakeprefix=$$newtop/ ;; \
	esac; \
	$(RM) $$i/Makefile.bak; \
	if [ -f $$i/Makefile ]; then \
	echo "	$(MV) Makefile Makefile.bak"; \
	if [ "$$executeit" != "no" ]; then \
	$(MV) $$i/Makefile $$i/Makefile.bak; \
	fi; \
	fi; \
	$(MAKE) $(MFLAGS) $(MAKE_OPTS) ONESUBDIR=$$i ONECURDIR=$$curdir IMAKETOP=$$imaketop IMAKEPREFIX=$$imakeprefix $$i/Makefile; \
	if [ -d $$i ] ; then \
	cd $$i; \
	$(MAKE) $(MFLAGS) Makefiles; \
	cd $$newtop; \
	else \
	exit 1; \
	fi; \
	done

includes::
	@for flag in ${MAKEFLAGS} ''; do \
	case "$$flag" in *=*) ;; --*) ;; *[ik]*) set +e;; esac; done; \
	for i in $(SUBDIRS) ;\
	do \
	echo including "in $(CURRENT_DIR)/$$i..."; \
	$(MAKE) -C $$i $(MFLAGS) $(PARALLELMFLAGS)  includes; \
	done

distclean::
	@for flag in ${MAKEFLAGS} ''; do \
	case "$$flag" in *=*) ;; --*) ;; *[ik]*) set +e;; esac; done; \
	for i in $(SUBDIRS) ;\
	do \
	echo "cleaning" "in $(CURRENT_DIR)/$$i..."; \
	$(MAKE) -C $$i $(MFLAGS) $(PARALLELMFLAGS)  distclean; \
	done


# ----------------------------------------------------------------------
# dependencies generated by makedepend

