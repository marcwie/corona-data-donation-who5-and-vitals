install:
	poetry install

activate:
	poetry shell

download:
	poetry run python datenspende_who5/load_raw_data.py

preprocess:
	poetry run python datenspende_who5/preprocess.py

compute:
	poetry run python datenspende_who5/compute.py

output:
	sh scripts/execute_notebooks.sh

pipeline: download preprocess compute output

setup: install download