import abc
from abc import ABC
from pathlib import Path
from typing import Any, Generic, Optional, TYPE_CHECKING

from framelink.types import T

if TYPE_CHECKING:
    from framelink.core import FramelinkModel, FramelinkPipeline


class FramelinkStorage(abc.ABC, Generic[T]):
    """
    The interface for interacting with a specific persistence medium for each model.
    """

    def __init__(self, lookup_from_store: bool = True) -> None:
        self.lookup_from_store = lookup_from_store

    @abc.abstractmethod
    def _store_frame(self, model: "FramelinkModel", result_frame: "T"):
        ...

    @abc.abstractmethod
    def _frame_lookup(self, model: "FramelinkModel", *args, **kwargs) -> Optional["T"]:
        ...

    def __call__(self, model: "FramelinkModel", ctx: "FramelinkPipeline", *args, **kwargs) -> "T":
        """
        This method should act as the retrieval function. A cache hit from `_store_frame` should return the stored
         frame, a cache miss will then just call the model and store its result using the `_store_frame` method.

        :param model: the model who may have a resulting frame to look up.
        :param ctx: the framelink context to execute against.
        :return: a cached frame or a new frame.
        """
        result = self._frame_lookup(model, *args, **kwargs) if self.lookup_from_store else None
        if result is None:
            result = model.callable(ctx)
            self._store_frame(model, result)
        return result


class FileStorage(FramelinkStorage, ABC):
    def __init__(self, data_dir: Path, file_suffix: str = ""):
        super().__init__()
        self.data_dir = data_dir
        self.file_suffix = file_suffix

    def _get_model_path(self, model: "FramelinkModel") -> Path:
        return self.data_dir / f"{model.name}.{self.file_suffix}"


class SQLPersistence(FramelinkStorage, ABC):
    def __init__(self, engine: Any):
        super().__init__()
        self.engine = engine
