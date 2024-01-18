# TCAD

This repositority contains code to download and partially parse the database exports from the Travis County Appraisal District.

## Features

* Downloads the "2023 Certified Export (July)" from [TCAD](https://traviscad.org/publicinformation)
* Parses and transforms selected texted files (property and improvements) into parquet files.
* Select properties by zip code and building type.

## Dependencies 

See environment.yml. 

Jupyter notebook or equivalent.

If you're regenerating the parquet files, you will have to extract the downloaded zip in the .cache folder unless you are using Windows and have 7-Zip installed.

## Installation

Clone this repository and use `pip install -e .` on the root folder.

## Usage

1. (optional) Downloading data and converting into parquet files. This step is optional since the parquet files have already been stored in data/processed/TCAD. See `1-tcad-parser.ipynb`.
   
2. See `2-tcad-data-preparation.ipynb` for examples of selecting by zip code and building type. 
