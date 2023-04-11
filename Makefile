install:
	poetry install

activate:
	poetry shell

download:
	poetry run python datenspende_who5/load_raw_data.py

pipeline: download 

setup: install download