.PHONY: clean clean-test clean-pyc clean-build docs help test test-cov
.DEFAULT_GOAL := help

clean: ## remove all build, test, coverage and Python artifacts
	@echo -----------------------------------------------------------------
	@echo CLEANING UP ...
	make clean-build clean-pyc clean-test
	@echo ALL CLEAN.
	@echo -----------------------------------------------------------------

clean-build: ## remove build artifacts
	@echo cleaning build artifacts ...
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	@echo cleaning pyc file artifacts ...
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	@echo cleaning test artifacts ...
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache
	rm -fr .hypothesis
	rm -fr .mypy_cache


test: ## run tests (and coverage if configured in setup.cfg) with the default Python
	@echo -----------------------------------------------------------------
	@echo RUNNING TESTS...
	uv run --group test --extra pyspark pytest -v --cov=sparkdantic
	@echo ✅ Tests have passed! Nice work!
	@echo -----------------------------------------------------------------


coverage: ## check code coverage quickly with the default Python
	@echo producing coverage report at COVERAGE.txt...
	coverage report > COVERAGE.txt

test-ci:
	uv run --group test --extra pyspark pytest --cov=sparkdantic --cov-report=json


dist: clean ## builds source and wheel package
	uv build
	ls -l dist


install: clean ## install the package to the active Python's site-packages via pip
	uv sync


install-e: clean ## install via pip in editable mode this see https://pip.pypa.io/en/stable/reference/pip_install/#editable-installs
	uv pip install -e .

test-cov: test ## run tests locally and output coverage file
	uv run --group test --extra pyspark coverage report > COVERAGE.txt

commit-cov:
	git add COVERAGE.txt --force

install-docs:
	uv sync --group docs

install-tests:
	uv sync --group test --extra pyspark

install-all-pyspark3:
	uv sync --group dev --group test --group docs --extra pyspark
	uv run --group dev --group test --group docs --extra pyspark pip install pyspark==3.5.5

install-all-pyspark4:
	uv sync --group dev --group test --group docs --extra pyspark
	uv run --group dev --group test --group docs --extra pyspark pip install pyspark==4.1.1

install-dev-local: ## install all the stuff you need to develop locally
	uv sync --group dev --group test --group docs --extra pyspark
	pre-commit install

publish: dist ## publish the package to PyPI
	uv publish

run-infra:
	docker-compose -f docker/dev/docker-compose.yaml up --remove-orphans -d

stop-infra:
	docker-compose -f docker/dev/docker-compose.yaml down

docs: ## generate Sphinx HTML documentation, including API docs
	uv run --group docs mkdocs build
