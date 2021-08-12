# dask-data-pipeline

This project was inspired by Street Group. This repo leverages on dask functionalities to load, extract and transform data as a JSON file.

## Directory structure
------------

```
|-- README.md                  <- The top-level README file for developers.
|-- data                       <- The data directory
|   |-- processed              <- processed data
|   |-- raw                    <- raw data
|-- processing                 <- Data processing occurs here
|   |-- __init__.py            <- folder initialisation
|   |-- columns.py             <- Handler for the column names used for the data
|   |-- data_pipeline.py       <- processes and extract the data
|-- run_pipeline.py            <- Run the data pipeline
|-- tests                      <- Upcoming the tests folder to ensure robust working flow for the functions and classes
|-- pyproject.toml             <- Toml file containing project dependencies.
```


## Usage
------------
To run load, process and transform data, first of all place the data in the data/raw folder to make things neat then:
> python run_pipeline.py --filename=.data/raw/file.csv --output-folder=.data/processed/output_folder
