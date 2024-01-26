# Premise

Contains all software and notebooks to reproduce the figures for a yet to be published study on the interrelation between wearable-derived physiology and self-assessed wellbeing. 
Unfortunately, the input data is not yet publicly available but will hopefully be publised together with the paper.
Package structure is based on an earlier version of my personal [data science template](https://github.com/marcwie/datascience-template).

# Purpose

Parse wearable sensors (e.g, Apple Watch, Fitbit, Garmin) data and survey responses collected within the Corona Data Donation project (*Corona Datenspende*, click [here](https://corona-datenspende.github.io/en/) for more information). 
The Corona Data Donation Project is one of the largest citizen science initiatives worldwide. 
From 2020 to 2022, more than 120,000 German residents donated continuous daily measurements of resting heart rate, physical activity and sleep timing for the advancement of public health research. 
These data streams were collected passively through a dedicated smartphone app, seamlessly connecting with participants’ fitness trackers and smartwatches. 
Additionally, participants actively engaged in regular surveys, sharing insights into their health and well-being during the COVID-19 pandemic. 

This package analyzes the interrelation between objectively measured physiology from wearable devices, i.e., resting heart rate, step count and sleep timing, and self-assessed well-being according to the [WHO-5 Well-Being Index](https://www.corc.uk.net/outcome-experience-measures/the-world-health-organisation-five-well-being-index-who-5/) 

# Installation

The package requires [poetry](https://python-poetry.org/). Make sure to have it installed and run `make install` after cloning this repository to install all dependencies.

# Repository structure

```
.
├── Makefile                                                       # setup, download data and run analysis 
├── README.md                                                      # README file as displayed on github
├── config                                                         # config files to be parsed by hydra
│   └── main.yaml                                                  #
├── notebooks                                                      # notebooks for analysis
│   ├── 0.01-compare_old_and_new_data_after_refactoring.ipynb      #
│   ├── 1.01-plot_raw_survey_data.ipynb                            #
│   ├── 1.02-analyze_vitals_vs_survey.ipynb                        #
│   ├── 1.03-individual-interrelations-and-correlations.ipynb      #
│   ├── 2.01-figures_for_paper.ipynb                               #
│   └── X.0X-template.ipynb                                        # a template notebooks with some defaults and presets
├── poetry.lock                                                    # poetry configurations
├── pyproject.toml                                                 #   
├── scripts                                                        # bash scrits  
│   └── execute_notebooks.sh                                       # run all jupyter notebooks from the command line
└── src                                                            # package source code to be used in notebooks
    ├── __init__.py                                                #
    ├── analyze.py                                                 # compute results
    ├── download.py                                                # load data from database
    ├── merge.py                                                   # merge input data into single file for later use
    ├── preprocess.py                                              # data cleaning and preprocessing
    └── utils                                                      #
        ├── __init__.py                                            #
        ├── colors.py                                              # some custom colors
        └── styling.py                                             # custom styling for figures
```

# Setup

After gaining data access you will receive instructions for setting up a VPN. You can then interact with the database by creating a file named `.env` in the root of the repository using the following template and filling in your credentials. Do not add this file to your git repository.

```
HOST = 
PORT = 
DBNAME = 
DBUSER = 
PASSWORD = 
```

# Analysis

In your command line type:
```
$ make activate
$ jupyter notebook
```
The first command activates the virtual environment for the project (see `Makefile` for details). You can then run all notebooks from the ``notebooks`` folder.

Alternatively you can run the entire analysis pipeline by simply typing
```
$ make pipeline
```
This downloads the raw data, performs necessary pre-processing steps, computes the final data set and runs the relevant jupyter notebooks. All output files are stored in a folder under ``output`` that is named according to the current time to prevent overwriting of previous outputs.
