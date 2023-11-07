from pathlib import Path
from typing import Literal, Optional, TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from framelink import FramelinkModel
    from framelink.storage.interfaces import FileStorage


class PandasLocalFilePersistence(FileStorage):
    def __init__(self, data_dir: Path, file_type: Literal["csv"]):
        data_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir = data_dir
        self.file_type = file_type
        super().__init__(data_dir, file_type)

    def _data_path_for(self, model: FramelinkModel) -> Path:
        _path = self.data_dir / f"{model.name}.{self.file_type}"
        return _path

    def _frame_store(self, model: "FramelinkModel", result_frame: pd.DataFrame) -> None:
        _path = self._data_path_for(model)
        result_frame.to_csv(_path)

    def _frame_lookup(self, model: "FramelinkModel", *args: Any, **kwargs: Any) -> Optional[pd.DataFrame]:
        path_ = self._data_path_for(model)
        return pd.read_csv(path_, **kwargs) if path_.exists() else None
