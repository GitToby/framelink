import functools
import pickle
from pathlib import Path
from typing import Optional, TYPE_CHECKING

from framelink.storage.interfaces import FileStorage, FramelinkStorage
from framelink.types import T

if TYPE_CHECKING:
    from framelink.core import FramelinkModel


class _NoStorage(FramelinkStorage):
    def __init__(self) -> None:
        super().__init__()

    def _store_frame(self, model: "FramelinkModel", result_frame: "T"):
        pass

    def _frame_lookup(self, model: "FramelinkModel", *args, **kwargs) -> None:
        return None


_no_storage_singleton = _NoStorage()


def NoStorage() -> _NoStorage:
    return _no_storage_singleton


class InMemory(FramelinkStorage):
    def __init__(self, lookup_from_store: bool = True) -> None:
        super().__init__(lookup_from_store)
        self._cache = functools.lru_cache()

    def _store_frame(self, model: "FramelinkModel", result_frame: "T"):
        # This is stored for us by the lru cache
        pass

    def _frame_lookup(self, model: "FramelinkModel", *args, **kwargs) -> Optional["T"]:
        cache = self._cache(model.build)
        return cache()


class PickleStorage(FileStorage):
    def __init__(self, data_dir: Path):
        super().__init__(data_dir, "pickle")

    def _store_frame(self, model: "FramelinkModel", result_frame: "T"):
        path = self._get_model_path(model)
        with path.open("wb") as f:
            pickle.dumps(f)

    def _frame_lookup(self, model: "FramelinkModel", *args, **kwargs) -> Optional["T"]:
        path = self._get_model_path(model)
        with path.open("rb") as f:
            res = pickle.load(f)
        return res
