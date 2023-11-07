import functools
import pickle
from pathlib import Path
from typing import Optional, TYPE_CHECKING
from framelink.storage.exceptions import StorageReadException

from framelink.storage.interfaces import FileStorage, FramelinkStorage
from framelink.types import T

if TYPE_CHECKING:
    from framelink._core import FramelinkModel


class _NoStorage(FramelinkStorage):
    def __init__(self) -> None:
        super().__init__()

    def _frame_store(self, *args, **kwargs):
        pass

    def _frame_lookup(self, *args, **kwargs) -> None:
        return None


_no_storage_singleton = _NoStorage()


def NoStorage() -> _NoStorage:
    """
    All models will be computed every time they are ref'd.
    """
    return _no_storage_singleton


class InMemory(FramelinkStorage):
    """
    Use the `functools.lru_cache` to store the results of our models. Useful when theres more computation/IO than data.
    """

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
    """
    Stores the resulting python object as a pickle object on disk at the specified location
    """

    def __init__(self, data_dir: Path | str):
        if type(data_dir) == str:
            data_dir = Path(data_dir)
        super().__init__(data_dir, "pickle")  # type: ignore

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
