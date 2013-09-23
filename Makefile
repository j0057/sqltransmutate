QUIET = --quiet

run: env/.requirements drop-database create-database
	env/bin/python sqltransmutate.py $(SOURCE) $(TARGET)

env:
	virtualenv env $(QUIET)

env/.requirements: env requirements.txt
	env/bin/pip install -r requirements.txt -U $(QUIET)
	touch env/.requirements

clean: drop-database

really-clean:
	rm -rf env
	rm -f *.pyc

include Makefile.inc

