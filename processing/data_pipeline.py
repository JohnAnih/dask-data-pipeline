import os
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

        # load the dask dataframe and assign it as an attribute ready to be accessed
        self._load_dataframe()

    def _load_dataframe(self) -> None:
        self._data = dd.read_csv(
            self.filename,
            names=self.columns,
            dtype=self.dtypes,
            header=None,
            **self.kwargs,
        )
        self._create_transaction_id()

    def _create_transaction_id(self):
        self._data["transaction_id"] = self._data.apply(
            lambda row: row["unique_id"].split("-")[0].strip("{"), axis=1
        )
        self.property_groups = self._data.groupby("transaction_id")
        self.groups = self._data.transaction_id.unique().compute().tolist()

    def data(self):
        return self._data

    def save_data_as_json(self, folder_name: str):
        print(f"saving JSON data to folder: {folder_name}")

        for group in self.groups:
            data = self.property_groups.get_group(group).compute()

            if not os.path.exists(folder_name):
                os.makedirs(folder_name)

            filename = folder_name + "/" + group + ".json"

            data.to_json(
                filename,
                orient="records",
                date_format="epoch",
                double_precision=10,
                force_ascii=True,
                date_unit="ms",
                default_handler=None,
            )

        print("data saved...")
