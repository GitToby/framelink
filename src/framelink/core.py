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
    persist_models = True
    persist_models_dir = Path(__file__).parent.parent / "data"


FRAME = TypeVar("FRAME", pl.DataFrame, pl.DataFrame, pl.LazyFrame)


class _Model(Generic[FRAME]):
    _callable: "PYPE_MODEL"
    graph_ref: nx.DiGraph
    call_perf: tuple[float, ...] = tuple()

    def __init__(self, model_func: "PYPE_MODEL", graph: nx.DiGraph):
        self._callable = model_func
        self.graph_ref = graph

    @property
    def name(self) -> str:
        name__ = self._callable.__name__
        return name__

    @property
    def docstring(self) -> Optional[str]:
        doc__ = self._callable.__doc__
        return doc__.strip() if doc__ else None

    @property
    def source(self) -> str:
        source__ = inspect.getsource(self._callable)
        return source__.strip()

    @property
    def call_count(self) -> int:
        return len(self.call_perf)

    @property
    def perf_stats(self) -> tuple[float, ...]:
        return self.call_perf

    @property
    def dependencies(self) -> tuple[float, ...]:
        return self.graph_ref.successors(self)

    def build(self, ctx: "FramelinkPipeline") -> FRAME:
        return self(ctx)

    # todo: make async?
    @lru_cache
    def __call__(self, ctx: "FramelinkPipeline") -> FRAME:
        start_time = time.perf_counter()
        res = self._callable(ctx)
        self.call_perf += (time.perf_counter() - start_time,)
        # settings = ctx.settings
        # if settings.persist_models:
        #     res.to_csv(settings.persist_models_dir / f"{self.name}.csv")
        return res

    def __key(self) -> tuple[str, str]:
        return self.name, self.source

    def __hash__(self) -> int:
        return hash(self.__key())


class FramelinkPipeline(Mapping, Generic[FRAME]):
    _models: dict["PYPE_MODEL", _Model]
    graph: nx.DiGraph

    def __init__(self, settings: FramelinkSettings = FramelinkSettings()):
        super().__init__()
        self._models = dict()
        self.graph = nx.DiGraph()
        self.settings = settings

    @property
    def model_names(self) -> list[str]:
        return sorted(m.name for m in self.keys())

    def model(self, *args, **kwargs) -> Callable[["PYPE_MODEL"], "PYPE_MODEL"]:
        """
        Annotation to add a method to the pypeline
        """

        def _decorator(func: "PYPE_MODEL") -> "PYPE_MODEL":
            m = _Model(func, self.graph)
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
        """
        ref will return the (cached) frame result of the model, so you can extend the frame inside another model.

        Example:
        >>> import pandas as pd
        >>> def my_model_1(_: FramelinkPipeline) -> pd.DataFrame:
        >>>     return pd.read_csv("path.to/cdv")
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
        """
        Building models is just proxied through to ref. Each build command should build only the given node in the graph
        """
        return self.ref(model_name)


PYPE_MODEL = Callable[[FramelinkPipeline], FRAME]
