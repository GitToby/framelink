import inspect
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable, Generic, Iterator, Mapping, Optional, TypeVar

import networkx as nx
import polars as pl


@dataclass
class FramelinkSettings:
    """Settings to be applied by the  """
    name: str = "default"
    persist_models_dir: Path = Path(__file__).parent.parent / "data"


FRAME = TypeVar("FRAME", pl.DataFrame, pl.DataFrame, pl.LazyFrame)


class _Model(Generic[FRAME]):
    """ """
    _callable: "PYPE_MODEL"
    graph_ref: nx.DiGraph
    call_perf: tuple[float, ...] = tuple()

    def __init__(
            self,
            model_func: "PYPE_MODEL",
            graph: nx.DiGraph,
            *,
            persist_after_run: bool = False,
            cache_result: bool = True
    ):
        # These are more "model settings"
        self.persist_after_run = persist_after_run
        self.cache_result = cache_result

        # question: is cache strategy right?
        if self.cache_result:
            self._callable = lru_cache()(model_func)
        else:
            self._callable = model_func

        # These are the core attributes of the Model
        self.graph_ref = graph

    @property
    def name(self) -> str:
        """ """
        name__ = self._callable.__name__
        return name__

    @property
    def docstring(self) -> Optional[str]:
        """ """
        doc__ = self._callable.__doc__
        return doc__.strip() if doc__ else None

    @property
    def source(self) -> str:
        """ """
        source__ = inspect.getsource(self._callable)
        return source__.strip()

    @property
    def call_count(self) -> int:
        """ """
        return len(self.call_perf)

    @property
    def perf_stats(self) -> tuple[float, ...]:
        """ """
        return self.call_perf

    @property
    def dependencies(self) -> tuple[float, ...]:
        """ """
        return self.graph_ref.successors(self)

    def build(self, ctx: "FramelinkPipeline") -> FRAME:
        """

        :param ctx: "FramelinkPipeline": 

        """
        return self(ctx)

    # todo: make async?
    def __call__(self, ctx: "FramelinkPipeline") -> FRAME:
        start_time = time.perf_counter()
        res = self._callable(ctx)
        self.call_perf += (time.perf_counter() - start_time,)
        if self.persist_after_run:
            out_dir = ctx.settings.persist_models_dir
            res.to_csv(out_dir / f"{self.name}.csv")
        return res

    def __key(self) -> tuple[str, str, bool]:
        """
        The uniqueness of a Model should be defined as its config and execution. This is used to determine the cache
        settings for the model when it is run
        """
        return self.name, self.source, self.persist_after_run

    def __hash__(self) -> int:
        return hash(self.__key())


class FramelinkPipeline(Mapping, Generic[FRAME]):
    """The core class for building DAGs of models and producing links of the results.

    Each model linked to the pipeline will have context onto their upstream and downstream dependencies.
    """
    _models: dict["PYPE_MODEL", _Model]
    graph: nx.DiGraph

    def __init__(self, settings: FramelinkSettings = FramelinkSettings()):
        super().__init__()
        self._models = dict()
        self.graph = nx.DiGraph()
        self.settings = settings

    @property
    def model_names(self) -> list[str]:
        """Return a list of model names registered to this pipeline"""
        return sorted(m.name for m in self.keys())

    def model(
            self,
            *,
            persist_after_run=False,
            cache_result=True
    ) -> Callable[["PYPE_MODEL"], "PYPE_MODEL"]:
        """Annotation to register a model to the pypeline.

        :param persist_after_run: Write the file to disk after running this model. The approach to writing the model is
            defined in the :FramelinkSettings: (Default value = False)
        :param cache_result:  (Default value = True)
        """

        def _decorator(func: "PYPE_MODEL") -> "PYPE_MODEL":
            """Internal wrapping of the model function to produce the metadata about the model.

            :param func: "PYPE_MODEL":
            """
            m = _Model(func, self.graph, persist_after_run=persist_after_run, cache_result=cache_result)
            self._models[func] = m
            return func

        return _decorator

    def __getitem__(self, __k: "PYPE_MODEL") -> _Model:
        item = self._models.get(__k)
        if item:
            return item
        else:
            raise KeyError(
                f"Could not find model {__k.__name__} registered in this pypeline"
            )

    def __len__(self) -> int:
        return len(self._models.keys())

    def __iter__(self) -> Iterator[_Model]:
        return (self[k] for k in self._models.keys())

    def ref(self, model: "PYPE_MODEL") -> FRAME:
        """ref will return the (cached) frame result of the model, so you can extend the frame inside another model.

        :param model: "PYPE_MODEL": The model function whos output you want to use.

        Example:
        >>> import pandas as pd
        >>> def my_model_1(_: FramelinkPipeline) -> pd.DataFrame:
        >>>     return pd.read_csv("path/to/file.csv")
        >>>
        >>> def my_model_2(ctx: FramelinkPipeline) -> pd.DataFrame:
        >>>     return ctx.ref(my_model_1).head()
        """
        try:
            model_wrapper: _Model = self[model]
            return model_wrapper.build(self)
        except KeyError as ke:
            raise KeyError() from ke

    def build(self, model_name: "PYPE_MODEL") -> FRAME:
        """Building models is just proxied through to ref. Each build command should build only the given node in the
         graph up to the nearest cache or persisted cache.

        :param model_name: "PYPE_MODEL": the model to build in the context of this pipelin.
        """
        return self.ref(model_name)

    def __key(self) -> tuple[int, ...]:
        """
        The state of a pypeline should be the aggregation settings that determine the way models should behave.

        When we cache a model frame we need to hash the context for its run, which links to settings it should
        take while building.
        """
        return tuple(hash(m) for m in self._models.values())

    def __hash__(self) -> int:
        return hash(self.__key())


PYPE_MODEL = Callable[[FramelinkPipeline], FRAME]
