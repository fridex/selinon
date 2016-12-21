TEMPFILE := $(shell mktemp -u)

.PHONY: install clean uninstall venv check doc docs html

install:
	sh ./bump-version.sh
	pip3 install -r requirements.txt
	python3 setup.py install

uninstall:
	python3 setup.py install --record ${TEMPFILE} && \
		cat ${TEMPFILE} | xargs rm -rf && \
		rm -f ${TEMPFILE}

devenv:
	@echo "Installing latest development requirements"
	pip3 install -U -r dev_requirements.txt

venv:
	python3 -m venv venv && source venv/bin/activate && pip3 install -r requirements.txt
	@echo "Run 'source venv/bin/activate' to enter virtual environment and 'deactivate' to return from it"

clean:
	find . -name '*.pyc' -or -name '__pycache__' -print0 | xargs -0 rm -rf
	rm -rf venv coverage.xml
	rm -rf dist selinon.egg-info build docs.source/api docs/build/

check:
	@# We have to adjust PYTHONPATH so we use our own Celery and modified Selinon Dispatcher for testing
	@# Make sure we have -p no:celery otherwise py.test is trying to do dirty stuff with loading celery.contrib
	PYTHONPATH="test/:${PYTHONPATH}" py.test -s --cov=./selinon -vvl --timeout=2 -p no:celery test/*.py
	@[ -n "${NOPYLINT}" ] || pylint selinon

doc:
	@sphinx-apidoc -e -o docs.source/api selinon -f
	@make -f Makefile.docs html
	@echo "Documentation available at 'docs/index.html'"

docs: doc
html: doc
test: check
