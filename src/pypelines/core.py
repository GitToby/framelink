import inspect
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable, TypeVar

import networkx as nx
import pandas as pd
import polars as pl

FRAME = TypeVar("FRAME", pl.DataFrame, pl.LazyFrame, pd.DataFrame)


## sd


@dataclass
class PypelineSettings:
    persist_models = True
    persist_models_dir = Path(__file__).parent.parent / "data"


@dataclass
class _Model:
    _callable: "PYPE_MODEL"
    call_perf: tuple[float, ...] = tuple()

    @property
    def name(self) -> str:
        return self._callable.__name__

    @property
    def docs(self) -> str | None:
        doc__ = self._callable.__doc__
        return doc__.strip() if doc__ else None

    @property
    def source(self) -> str:
        return inspect.getsource(self._callable)

    @property
    def call_count(self) -> int:
        return len(self.call_perf)

    @property
    def perf_stats(self) -> tuple[float, ...]:
        return self.call_perf

    # todo: make async?
    @lru_cache
    def call(self, ctx: "Pypeline") -> FRAME:
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


class Pypeline:
    _models: dict[str, _Model] = dict()
    _graph: nx.DiGraph = nx.DiGraph()

    def __init__(self, settings: PypelineSettings = PypelineSettings()):
        self.settings = settings

    def add_model(self, func: "PYPE_MODEL"):
        # todo: add a metadata obj with implied schema?
        m = _Model(func)
        self._models[m.name] = m
        self._graph.add_node(m)

        # todo, scan node & add edges

    def get(self, model_name) -> _Model | None:
        return self._models.get(model_name)

    def __getitem__(self, item):
        return self.get(item)

    def select(self, term) -> _Model | None:
        # todo, add dbt syntax here
        return self.get(term)

    def ref(self, model_name: str) -> FRAME:
        model = self.get(model_name)
        assert model, "no model found"
        model_res = model.call(self)
        return model_res

    def build(self, model_name: str) -> FRAME:
        return self.ref(model_name)


PYPE_MODEL = Callable[[Pypeline], FRAME]
