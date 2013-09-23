QUIET = --quiet

include Makefile.inc

run: env/.requirements
	env/bin/python sqltransmutate.py $(SOURCE) $(TARGET)

env:
	virtualenv env $(QUIET)

env/.requirements: env requirements.txt
	env/bin/pip install -r requirements.txt -U $(QUIET)
	touch env/.requirements

clean:
	rm -rf env
	rm -f *.pyc

