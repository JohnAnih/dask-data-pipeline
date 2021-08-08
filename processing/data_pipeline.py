from typing import Dict

import dask.dataframe as dd

from .columns import Columns


class DataPipeline:
    """Implements the data pipeline interface for extracting, loading and transforming data
    The DataPipeline utilizes Dask DataFrame as against Apache Beam beacause of its able to handle big data and its API is built with Python

    Parameters:
    filename (str): The name of the file that contains the data to load
    dtypes(dict(str, str)): The data types for the columns
    kwargs: other parameters from pandas class

    Attributes:
    columns (list(str)): contains the list of column names
    """

    def __init__(self, filename: str, dtypes: Dict[str, str] = None, **kwargs) -> None:
        self.filename = filename
        self.kwargs = kwargs
        self.columns = Columns().names()

        if dtypes is None:
            self.dtypes = {"paon": "object", "saon": "object", "price": "float64"}
        else:
            self.dtypes = dtypes

        # load the dask dataframe and assign it
        self._load_dataframe()

    def _load_dataframe(self) -> None:
        self._data = dd.read_csv(
            self.filename,
            names=self.columns,
            dtype=self.dtypes,
            header=None,
            **self.kwargs,
        )
        self._create_unque_id()
        self._rearrange_column_names()

    def _create_unque_id(self):
        # create unique id from column names
        self._data["id"] = (
            (self._data.groupby(self.columns[1:]).cumcount() == 0).astype(int).cumsum()
        )
        self._data["id"] = self._data["id"].cumsum()

    def _rearrange_column_names(self):
        columns = list(self._data.columns)
        columns = [columns[-1]] + columns[:-1]
        self._data = self._data[columns]

    def data(self):
        return self._data

    def save_data_as_json(self, output_file: str):
        print(f"saving JSON data: {output_file}")
        self._data.to_json(
            output_file,
            orient="records",
            date_format="epoch",
            double_precision=10,
            force_ascii=True,
            date_unit="ms",
            default_handler=None,
        )
        print("data saved...")
