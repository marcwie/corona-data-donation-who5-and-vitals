install:
	poetry install

activate:
	poetry shell

notebook:
	poetry shell
	jupyter notebook

download:
	poetry run python src/download.py

preprocess:
	poetry run python src/preprocess.py

merge:
	poetry run python src/merge.py

output:
	sh scripts/execute_notebooks.sh

pipeline: download preprocess merge output

setup: install download

.PHONY: output
