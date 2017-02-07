#
# Makefile for checking script syntax
#

PERL5LIB = .:/home/xad/share/perl5

PYTHON_DIR = ../python
PY_FILES = $(shell find $(PYTHON_DIR) -name '*.py') 


# Check perl code
c: c-py

c-py:
	for f in ${PY_FILES}; do\
	    echo "Checking $${f}..."; \
	    python2.7 -m py_compile $${f}; \
	    python2.7 -m compileall $${f}; \
	done



