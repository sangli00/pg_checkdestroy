MODULE_big = pg_checkdestroy

PG_CHECKDESTROY_VERSION=1.0

OBJS = pg_checkdestroy.o

EXTENSION = pg_checkdestroy
DATA = pg_checkdestroy--1.0.sql
REGRESS = check sql drop&delete&truncate.

SHLIB_LINK += $(filter -lm, $(LIBS)) 
USE_PGXS=1
ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_checkdestroy
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

distrib:
	rm -f *.o
	rm -rf results/ regression.diffs regression.out tmp_check/ log/
	cd .. ; tar --exclude=.svn -chvzf pg_checkdestroy-$(PG_CHECKDESTROY_VERSION).tar.gz pg_checkdestroy
