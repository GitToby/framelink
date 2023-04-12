import inspect
import logging
import re
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Callable, Generic, Iterator, Optional, TYPE_CHECKING, TypeVar, Union

import networkx as nx

if TYPE_CHECKING:
    import pydot


@dataclass
class FramelinkSettings:
    """Settings to be applied by the"""

    persist_models_dir: Path = Path(__file__).parent.parent / "data"


FRAME = TypeVar("FRAME")  # usually one of pl.DataFrame, pl.DataFrame, pl.LazyFrame.. etc


class FramelinkModel(Generic[FRAME]):
    """
    A wrapper around a `PYPE_MODEL` that enables:
     - visibility onto the model DAG
     - cacheing of the model run
     - monitoring of run performance
     - adapters to run the model on various engines
    """

    _callable: "PYPE_MODEL"
    _graph_ref: nx.DiGraph
    call_perf: tuple[float, ...] = tuple()

    def __init__(
        self,
        model_func: "PYPE_MODEL",
        graph: nx.DiGraph,
        *,
        persist_after_run: bool = False,
        cache_result: bool = True,
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
        self._graph_ref = graph
        self.log = logging.getLogger(f"{self.__class__}.{self.name}")

    @property
    def __name__(self):
        return self._callable.__name__

    def __repr__(self):
        return self.name

    @property
    def upstreams(self) -> set["FramelinkModel"]:
        """
        Pull this model from the graph and pull out all models are direct predecessors to this model.
        :return: a set of models that this model depends on.
        """
        return set(self._graph_ref.predecessors(self))

    @property
    def downstreams(self) -> set["FramelinkModel"]:
        """
        Pull this model from the graph and pull out all models are direct sucessors to this model.
        :return: a set of models that depend on this model.
        """
        return set(self._graph_ref.successors(self))

    @property
    def name(self) -> str:
        """Returns the models name"""
        return self.__name__

    @property
    def docstring(self) -> Optional[str]:
        """ """
        doc__ = self._callable.__doc__
        return doc__.strip() if doc__ else None

    @property
    def source(self) -> str:
        """ """
        source__ = inspect.getsource(self._callable)
        source__ = source__.strip()
        return source__

    @property
    def call_count(self) -> int:
        """return the numer of times this model has been called"""
        return len(self.call_perf)

    @property
    def perf_stats(self) -> tuple[float, ...]:
        """ """
        return self.call_perf

    def build(self, ctx: "FramelinkPipeline") -> FRAME:
        """
        Build the current model with the context of the pipeline.
        :param ctx: "FramelinkPipeline": framelink pipeline context
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

    def __key(self) -> tuple[str, Optional[str], bool]:
        """
        The uniqueness of a Model should be defined as its config and execution. This is used to determine the cache
        settings for the model when it is run
        """
        return self.name, self.source, self.persist_after_run

    def __hash__(self) -> int:
        return hash(self.__key())


class FramelinkPipeline(Generic[FRAME]):
    """The core class for building DAGs of models and producing links of the results.

    Each model linked to the pipeline will have context onto their upstream and downstream dependencies.
    """

    _models: dict[Union[str], FramelinkModel]
    graph: nx.DiGraph

    def __init__(self, name: str = "default", settings: FramelinkSettings = FramelinkSettings()):
        super().__init__()
        self._name = name
        self._models = dict()
        self.graph = nx.DiGraph()
        self.settings = settings
        self.log = logging.getLogger()

    def __repr__(self):
        return f"<{self.name} with {len(self)} models at {hex(id(self))}>"

    @property
    def name(self) -> str:
        """
        Return a namespaced name of this pipeline
        """
        return f"{self.__class__.__name__}.{self._name}"

    @property
    def model_names(self) -> list[str]:
        """Return a list of model names registered to this pipeline"""
        return sorted(m.name for m in self._models.values())

    def graph_dot(self) -> pydot.Dot:
        """
        Using networkx and graphviz, create a DOT string representation of the model DAG.
        """
        return nx.drawing.nx_pydot.to_pydot(self.graph)

    def graph_plt(self):
        """
        Using networkx and matplotlib create an image representation of the model DAG.
        """
        # question: is there a better graph layout
        # pos = nx.multipartite_layout(self.graph)
        pos = nx.planar_layout(self.graph)
        return nx.draw_networkx(self.graph, pos)

    def model(self, *, persist_after_run=False, cache_result=True) -> Callable[["PYPE_MODEL"], FramelinkModel]:
        """Annotation to register a model to the pypeline.

        :param persist_after_run: Write the file to disk after running this model. The approach to writing the model is
            defined in the :FramelinkSettings: (Default value = False)
        :param cache_result:  (Default value = True)
        """

        def _decorator(func: "PYPE_MODEL") -> FramelinkModel:
            """Internal wrapping of the model function to produce the metadata about the model.

            :param func: "PYPE_MODEL": the callable function that defines the model
            :returns
            """

            model_wrapper: FramelinkModel = FramelinkModel(
                func,
                self.graph,
                persist_after_run=persist_after_run,
                cache_result=cache_result,
            )

            # todo: brainstorm more new ways of doing this.
            pattern = r"\.ref\((.*?)\)"
            matches = re.findall(pattern, model_wrapper.source)
            matched_models = (self.get(name) for name in matches)

            # we need to keep a ref to the underlying graph so we can access the models when we ask for them via
            # `ref()` or `build()`
            self._models[model_wrapper.name] = model_wrapper

            # calc max depth and add to the incoming node

            # question: would adding just the ref (String) to the graph and then computing from there be better?
            self.graph.add_node(model_wrapper)
            upstream_edges = ((upstream_model, model_wrapper) for upstream_model in matched_models)
            self.graph.add_edges_from(upstream_edges)

            if not nx.is_directed_acyclic_graph(self.graph):
                cycle = nx.find_cycle(self.graph, model_wrapper)
                raise ValueError(f"{model_wrapper.name} has a loop: {cycle}")

            return model_wrapper

        return _decorator

    def ref(self, model: "FRAMELINK_MODEL_REF") -> FRAME:
        """ref will return the (cached) frame result of the model, so you can extend the frame inside another model.

        :param model: _Model: The model function with output you want to use.
        Example:
        >>> import pandas as pd
        >>>
        >>> pipeline = FramelinkPipeline()
        >>>
        >>> @pipeline.model()
        >>> def my_model_1(_: FramelinkPipeline) -> pd.DataFrame:
        >>>     return pd.read_csv("path/to/file.csv")
        >>>
        >>> @pipeline.model()
        >>> def my_model_2(ctx: FramelinkPipeline) -> pd.DataFrame:
        >>>     return ctx.ref(my_model_1).head()
        """
        try:
            model_wrapper = self.get(model)
            return model_wrapper.build(self)
        except KeyError as ke:
            raise KeyError(f"No key {model}") from ke

    def build(self, model_name: Union[str, FramelinkModel]) -> FRAME:
        """Building models is just proxied through to ref. Each build command should build only the given node in the
         graph up to the nearest cache or persisted cache.

        :param model_name: "PYPE_MODEL": the model to build in the context of this pipeline.
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

    def __len__(self) -> int:
        return len(self._models.keys())

    def __iter__(self) -> Iterator[FramelinkModel]:
        return self._models.values().__iter__()

    def __contains__(self, item: Union[FramelinkModel, "PYPE_MODEL"]):
        return item.__name__ in self._models.keys() or item in self._models.values()

    def get(self, model: "FRAMELINK_MODEL_REF") -> FramelinkModel:
        """
        Given a `FramelinkModel`, or the model's name, return the`FramelinkModel`.


        :param model: Model function or the model name (function name).
        :returns: The `FramelinkModel` that wraps the model.
        """
        if type(model) != str:
            model = model.__name__  # type: ignore

        try:
            return self._models[model]
        except KeyError as k:
            raise KeyError(
                f"Could not locate the model '{model}' in the pipeline {self.name} models: {self.model_names}. "
                "Have you registered it yet?"
            ) from k


PYPE_MODEL = Callable[[FramelinkPipeline[FRAME]], FRAME]
# question: why does this not type check correctly on `FramelinkModel | str` only?
FRAMELINK_MODEL_REF = Union[FramelinkModel, str, PYPE_MODEL]
