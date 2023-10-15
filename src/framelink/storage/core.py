import functools
import pickle
from pathlib import Path
from typing import Optional, TYPE_CHECKING
from framelink.storage.exceptions import StorageReadException

from framelink.storage.interfaces import FileStorage, FramelinkStorage
from framelink.types import T

if TYPE_CHECKING:
    from framelink.core import FramelinkModel


class _NoStorage(FramelinkStorage):
    def __init__(self) -> None:
        super().__init__()

    def _frame_store(self, *args, **kwargs):
        pass

    def _frame_lookup(self, *args, **kwargs) -> None:
        return None


_no_storage_singleton = _NoStorage()


def NoStorage() -> _NoStorage:
    return _no_storage_singleton


class InMemory(FramelinkStorage):
    def __init__(self) -> None:
        super().__init__()
        self._cache = functools.lru_cache()

    def _frame_store(self, model: "FramelinkModel[T]", result_frame: "T"):
        # This is stored for us by the lru cache
        pass

    def _frame_lookup(self, model: "FramelinkModel[T]", *args, **kwargs) -> Optional["T"]:
        cache = self._cache(model.build)
        return cache()


class PickleStorage(FileStorage):
    def __init__(self, data_dir: Path):
        super().__init__(data_dir, "pickle")

    def _frame_store(self, model: "FramelinkModel[T]", result_frame: "T"):
        path = self._get_model_path(model)
        with path.open("wb") as f:
            pickle.dump(result_frame, f)

    def _frame_lookup(self, model: "FramelinkModel[T]", *args, **kwargs) -> Optional["T"]:
        path = self._get_model_path(model)

        if not path.exists():
            return None

        try:
            with path.open("rb") as f:
                res = pickle.load(f)
            return res
        except EOFError as e:
            raise StorageReadException from e
