PYTHON_EXE := $(shell which python3)

help:
	@echo "clean  - remove all python artifacts"
	@echo "dev    - create a dev virtualenv"
	@echo "prod   - create a prod virtualenv"
	@echo "build  - package pyspark code"
	@echo "submit - submit pyspark code"

clean:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

dev:
	$(PYTHON_EXE) -m virtualenv --clear .devenv
	. .devenv/bin/activate && pip install --no-cache-dir -r dev-requirements.txt

lint:
	$(PYTHON_EXE) -m pylint src/main.py

run:
	$(PYTHON_EXE) src/main.py

prod:
	$(PYTHON_EXE) -m virtualenv --clear .devenv
	. .devenv/bin/activate && pip install --no-cache-dir -r requirements.txt

build: $(shell find src) requirements.txt
	mkdir -p ./build
	rm -f ./build/*
	make clean && cd ./src/ && zip -x main.py -r ../build/src.zip .
	. .venv/bin/activate && pip install -r requirements.txt && (cd .venv/lib/python*/site-packages/ && zip -r - *) > build/packages.zip

submit: build
	PYSPARK_DRIVER_PYTHON=$(PYTHON_EXE) PYSPARK_DRIVER_PYTHON_OPTS="" ${SPARK_HOME}/bin/spark-submit \
		--master local[*] \
		--py-files build/packages.zip,build/src.zip \
		--files configs/jobs_config.json \
		src/main.py

