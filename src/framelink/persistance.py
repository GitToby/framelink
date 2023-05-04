import abc
from abc import ABC
from pathlib import Path
from typing import Generic, Literal, Optional, TYPE_CHECKING

import pandas as pd
import polars as pl

if TYPE_CHECKING:
    from framelink.core import T


class ModelPersistence(abc.ABC, Generic[T]):
    """
    The interface for interacting with a specific persistence medium for each model.
    """

    @abc.abstractmethod
    def store_frame(self, name: str, result_frame: "T") -> None:
        ...

    @abc.abstractmethod
    def load_frame(self, name: str) -> Optional["T"]:
        ...


class FilePersistence(ModelPersistence, ABC):
    def __init__(self, file_type: Literal["csv", "json", "parquet"]):
        self.file_type = file_type


class NoPersistence(ModelPersistence):
    def store_frame(self, name: str, result_frame: "T"):
        pass

    def load_frame(self, name: str):
        pass


class LocalFilePersistence(FilePersistence):
    def __init__(self, data_dir: Path, file_type: Literal["csv"]):
        data_dir.mkdir(parents=True, exist_ok=True)  # todo: make this not so brazen
        self.data_dir = data_dir
        super().__init__(file_type)

    def store_frame(self, name: str, result_frame: "T") -> None:
        _path = self.data_dir / f"{name}.{self.file_type}"
        if isinstance(result_frame, pd.DataFrame):
            result_frame.to_csv(_path)
        elif isinstance(result_frame, pl.DataFrame):
            result_frame.to_pandas().to_csv(_path)
        elif isinstance(result_frame, pl.LazyFrame):
            result_frame.collect().to_pandas().to_csv(_path)
        else:
            raise NotImplementedError(f"No file persistence found for result of type {type(result_frame)}")

    def load_frame(self, name: str) -> Optional["T"]:
        pass


class LocalSQLitePersistence(ModelPersistence):
    pass
