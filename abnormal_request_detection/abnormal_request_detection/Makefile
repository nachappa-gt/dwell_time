# Makefile wrapper for gradle/sbt
#

SBT = sbt
PYTHON_DIR = src/main/python
#LIBS_DIR = build/libs
LIBS_DIR = target

CACHE_PATH = $(LIBS_DIR)/python-cache.tgz
ZIP_PATH = $(LIBS_DIR)/xad-bidding.zip

PLUGINS = project/plugins.sbt
S3CREDENTIALS = ~/.sbt/.s3credentials

.PHONY: build pkg

default: jar
	@echo "Done!"

pkg rp lp li gw:
	make -C pkg $@

test: compileTest

check:
	@if [ ! -f ${PLUGINS} ]; then \
		echo "ERROR - Missing $(PLUGINS); See COMPILE.md";\
		exit 1; \
	fi
	@if [ ! -f ${S3CREDENTIALS} ]; then \
		echo "ERROR - Missing $(S3CREDENTIALS); See COMPILE.md";\
		exit 1; \
	fi

jar package: check
	$(SBT) package

clean:
	$(SBT) $@

%:
	$(SBT) $@


# Python tgz file
#cache:
#	rm -f $(CACHE_PATH)
#	tar zcvf $(CACHE_PATH) -C $(PYTHON_DIR) xad
#
#zip z: 
#	rm -f $(ZIP_PATH)
#	python2.7 -m compileall $(PYTHON_DIR)
#	cd $(PYTHON_DIR); zip -r ../../../$(ZIP_PATH) *
#
#lszip lsz lz: 
#	unzip -l $(ZIP_PATH)
#

