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

pipeline: download preprocess

setup: install download