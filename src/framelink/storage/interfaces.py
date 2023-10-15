import abc
from abc import ABC
import logging
from pathlib import Path
from typing import Any, Generic, Optional, TYPE_CHECKING
from framelink.storage.exceptions import StorageReadException

from framelink.types import T

if TYPE_CHECKING:
    from framelink.core import FramelinkModel, FramelinkPipeline

LOG = logging.getLogger("storage")


class FramelinkStorage(abc.ABC, Generic[T]):
    """
    The interface for interacting with a specific persistence medium for each model.
    """

    @abc.abstractmethod
    def _frame_store(self, model: "FramelinkModel[T]", result_frame: "T"):
        ...

    @abc.abstractmethod
    def _frame_lookup(self, model: "FramelinkModel[T]", *args, **kwargs) -> Optional["T"]:
        ...

    def retrieve_or_build(self, model: "FramelinkModel[T]", ctx: "FramelinkPipeline", *args, **kwargs) -> "T":
        """
        This method should act as the retrieval function. A cache hit from `_store_frame` should return the stored
         frame, a cache miss will then just call the model and store its result using the `_store_frame` method.

        :param model: the model who may have a resulting frame to look up.
        :param ctx: the framelink context to execute against.
        :return: a cached frame or a new frame.
        """
        LOG.info(f"looking up model {model.name}")
        try:
            result = self._frame_lookup(model, *args, **kwargs)
        except StorageReadException as e:
            # todo: allow users to bubble up read exceptions?
            LOG.warning(f"Failed read for model {model.name} with error {str(e)}")
            result = None

        if result is None:
            LOG.info(f"no result for {model.name}")
            result = model.callable(ctx)
            self._frame_store(model, result)
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
