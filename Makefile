PYTHON_EXE := $(shell which python3)

help:
	@echo "clean  - remove all build packages and python artifacts"
	@echo "dev    - create a dev virtualenv"
	@echo "prod   - create a prod virtualenv"
	@echo "build  - package pyspark code"
	@echo "submit - submit pyspark code"

clean:
	rm -rf build
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

dev:
	$(PYTHON_EXE) -m virtualenv --clear .devenv
	. .devenv/bin/activate && pip install --no-cache-dir -r dev-requirements.txt

lint:
	pylint src/main.py

run:
	python3 src/main.py

prod:
	$(PYTHON_EXE) -m virtualenv --clear .devenv
	. .devenv/bin/activate && pip install --no-cache-dir -r requirements.txt

build: build/packages.zip $(shell find src)
	cd ./src/ && zip -x main.py -r ../build/src.zip .

build/packages.zip: requirements.txt
	mkdir -p ./build
	$(PYTHON_EXE) -m virtualenv --clear .venv
	. .venv/bin/activate && pip install --no-cache-dir -r requirements.txt && (cd .venv/lib/python*/site-packages/ && zip -r - *) > build/packages.zip

submit: build
	PYSPARK_DRIVER_PYTHON=$(PYTHON_EXE) PYSPARK_DRIVER_PYTHON_OPTS="" ${SPARK_HOME}/bin/spark-submit \
		--master local[*] \
		--py-files build/packages.zip,build/src.zip \
		--files configs/jobs_config.json \
		src/main.py

