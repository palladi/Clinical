# ClinicalTrials.gov Data Pipeline

## Overview
This script fetches data from the ClinicalTrials.gov API and stores it in Parquet files with the provision to filter studies
by their last update post date.

## main.py
Running main.py fetches 150 records (15 pages) of most recently published studies.
- To fetch all the available studies from the API, please change line 26 to "while True:"
 

## Prerequisites
- Python 3.x
- Required libraries: `requests`, `pandas`, `pyarrow`

## Installation
Install the required libraries by running following line in the command prompt.

pip install -r requirements.txt
