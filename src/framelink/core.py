import contextlib
import inspect
import logging
import textwrap
import time
from dataclasses import dataclass
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any, Callable, Collection, Generator, Generic, Iterator, Optional, Protocol, Union

import networkx as nx
import pydot

from framelink._cli import CLI_CONTEXT
from framelink._util import parse_model_src_for_internal_refs
from framelink.storage.core import NoStorage
from framelink.storage.interfaces import FramelinkStorage
from framelink.types import F, T


@dataclass
class FramelinkSettings:
    """Settings to be applied by the"""

    persist_models_dir: Path = Path(__file__).parent.parent / "data"
    default_log_level: int = logging.WARNING
    default_storage: FramelinkStorage = NoStorage()


class _FramelinkComponent(Protocol):
    _name: str

    @property
    def __name__(self):
        return self._name

    @property
    def __loc__(self):
        return hex(id(self))

    @property
    def name(self) -> str:
        """
        Util method for __name__ for natural flow in writing code.
        """
        return self.__name__

    def __key(self) -> tuple[Any, ...]:
        ...

    def __hash__(self) -> int:
        return hash(self.__key())


class FramelinkModel(_FramelinkComponent, Generic[T]):
    """
    A wrapper around a callable model that enables:
     - visibility onto the model DAG
     - caching of the model run
     - monitoring of run performance
     - adapters to run the model on various engines
    """

    callable: F[T]
    _graph_ref: nx.DiGraph
    call_perf: tuple[float, ...] = tuple()

    def __init__(
        self,
        model_func: F[T],
        graph: nx.DiGraph,
        pipeline_settings: FramelinkSettings,
        *,
        logging_level: Optional[int] = None,
        store: FramelinkStorage[T] = NoStorage(),
    ):
        # These are more "model settings"
        self.callable = model_func
        self._name = model_func.__name__
        self._store = store

        # These are the core attributes of the Model
        self._graph_ref = graph
        self._log = logging.getLogger(self.name)
        self._log.setLevel(logging_level if logging_level else pipeline_settings.default_log_level)

    def __repr__(self):
        return f"<{self.name} at {self.__loc__}>"

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
    def docstring(self) -> Optional[str]:
        """ """
        doc__ = self.callable.__doc__
        return textwrap.dedent(doc__).strip() if doc__ else None

    @property
    def source(self) -> str:
        """ """
        source__ = inspect.getsource(self.callable)
        source__ = textwrap.dedent(source__).strip()
        return source__

    @property
    def call_count(self) -> int:
        """return the numer of times this model has been called"""
        return len(self.call_perf)

    @property
    def perf_stats(self) -> tuple[float, ...]:
        """ """
        return self.call_perf

    @contextlib.contextmanager
    def _own_logging(self, ctx: "FramelinkPipeline"):
        old_log, ctx.log = ctx.log, self._log
        yield
        ctx.log = old_log

    def build(self, ctx: "FramelinkPipeline") -> T:
        """
        Build the current model with the context of the pipeline.
        :param ctx: "FramelinkPipeline": framelink pipeline context
        """
        with self._own_logging(ctx):
            ctx.log.info(f"Building {self.name}...")

            start_time = time.perf_counter()
            res = self._store(self, ctx)
            # question: this timer is the whole stream, how can we isolate just the build time for this model?
            build_time = time.perf_counter() - start_time
            ctx.log.info(f"Finished building {self.name} in {build_time:.3}s")

            # post build steps
            self.call_perf += (build_time,)
        return res

    def __key(self) -> tuple[str, Optional[str]]:
        """
        The uniqueness of a Model should be defined as its config and execution. This is used to determine the cache
        settings for the model when it is run
        """
        return self.name, self.source


class FramelinkPipeline(_FramelinkComponent):
    """The core class for building DAGs of models and producing links of the results.

    Each model linked to the pipeline will have context onto their upstream and downstream dependencies.
    """

    _models: dict[str, FramelinkModel]
    graph: nx.DiGraph
    log: logging.Logger  # Placeholder for model logger injection
    _log: logging.Logger  # Used for pipeline logging.

    def __init__(self, name: str = "default", settings: FramelinkSettings = FramelinkSettings()):
        super().__init__()
        self._name = name
        self._models = dict()
        self.graph = nx.DiGraph()
        self.settings = settings
        self._log = logging.getLogger(self.name)
        self._log.setLevel(settings.default_log_level)
        self.log = self._log
        CLI_CONTEXT.fl_pipelines[self._name] = self

    def __repr__(self):
        return f"<{self.name} with {len(self)} models at {self.__loc__}>"

    @property
    def model_names(self) -> list[str]:
        """Return a list of model names registered to this pipeline"""
        return sorted(m.name for m in self._models.values())

    @property
    def trees(self):
        return

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

    def model(
        self, *, logging_level=None, storage: Optional[FramelinkStorage[T]] = None
    ) -> Callable[[F[T]], FramelinkModel[T]]:
        """
        Annotation to register a model to the framelink pipeline.

        :param logging_level: Sets the logging level specifically for this model. If no level is passed it will default
            to the default level as per the pipelines settings.
        :param storage: Sets the persistence approach used when storing the model.
        """
        if storage:
            model_store_unwrapped = storage
        else:
            model_store_unwrapped = self.settings.default_storage

        def _decorator(func: F[T]) -> FramelinkModel[T]:
            """Internal wrapping of the model function to produce the metadata about the model.

            :param func: "PYPE_MODEL": the callable function that defines the model
            :returns: the wrapped model with all the extra pieces
            """

            model_wrapper: FramelinkModel = FramelinkModel(
                func, self.graph, self.settings, logging_level=logging_level, store=model_store_unwrapped
            )
            self._log.info(f"Registering model '{model_wrapper.name}'")

            # we need to keep a ref to the underlying graph to access the models when we ask for them via
            # `ref()` or `build()`
            assert model_wrapper.name not in self._models.keys(), f"Model already registered under {model_wrapper.name}"
            self._models[model_wrapper.name] = model_wrapper

            # todo: brainstorm more new ways of doing this.
            matches = parse_model_src_for_internal_refs(model_wrapper.source)
            matched_models = (self.get(name) for name in matches)

            self.graph.add_node(model_wrapper)
            upstream_edges = ((upstream_model, model_wrapper) for upstream_model in matched_models)
            self.graph.add_edges_from(upstream_edges)
            self._log.debug(
                f"Model '{model_wrapper.name}' has {len(model_wrapper.upstreams)} upstreams: {model_wrapper.upstreams}"
            )

            if not nx.is_directed_acyclic_graph(self.graph):
                cycle = nx.find_cycle(self.graph, model_wrapper)
                raise ValueError(f"{model_wrapper.name} has a loop: {cycle}")

            return model_wrapper

        return _decorator

    def ref(self, model: Union[FramelinkModel[T], str]) -> T:
        """
        ref will return the (cached) frame result of the model, so you can extend the frame inside another model.

        Raises a KeyError if the model Key cant be found.

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
        model_wrapper = self.get(model)
        return model_wrapper.build(self)

    def build(self, model_name: Union[FramelinkModel[T], str]) -> T:
        """Building models is just proxied through to ref. Each build command should build only the given node in the
         graph up to the nearest cache or persisted cache.

        :param model_name: "PYPE_MODEL": the model to build in the context of this pipeline.
        :param overrides: A mapping of models whos result should be overridden for this build.
        """
        return self.ref(model_name)

    def topological_sorted_nodes(self) -> Generator[tuple[FramelinkModel, ...], None, None]:
        """
        This implementation is more parallel aware than the networkx implementation/

        Basically a repeatable version of the example at https://docs.python.org/3/library/graphlib.html
        :return:
        """
        topological_sorter: TopologicalSorter = TopologicalSorter()
        for node in self._models.values():
            topological_sorter.add(node, *node.upstreams)

        topological_sorter.prepare()
        while topological_sorter.is_active():
            nodes = topological_sorter.get_ready()
            yield nodes
            topological_sorter.done(*nodes)

    @classmethod
    def merge_pipelines(
        cls,
        new_name: str,
        settings: FramelinkSettings = FramelinkSettings(),
        *,
        collection: Collection["FramelinkPipeline"],
    ) -> "FramelinkPipeline":
        new_pipeline = FramelinkPipeline(new_name, settings)

        for other in collection:
            print(other.name)
            assert len(set(new_pipeline.model_names) & set(other.model_names)) == 0
            new_pipeline._models = new_pipeline._models | other._models

        return new_pipeline

    def __key(self) -> tuple[int, ...]:
        """
        The state of a pypeline should be the aggregation settings that determine the way models should behave.

        When we cache a model frame we need to hash the context for its run, which links to settings it should
        take while building.
        """
        return tuple(hash(m) for m in self._models.values())

    def __len__(self) -> int:
        return len(self._models.keys())

    def __iter__(self) -> Iterator[FramelinkModel]:
        return self._models.values().__iter__()

    def __contains__(self, item: Union[FramelinkModel, F[T]]) -> bool:
        return item.__name__ in self._models.keys() or item in self._models.values()

    def get(self, model: Union[FramelinkModel, str]) -> FramelinkModel:
        """
        Given a `FramelinkModel`, or the model's name, return the`FramelinkModel`.


        :param model: Model function or the model name (function name).
        :returns: The `FramelinkModel` that wraps the model.
        """

        try:
            model_name = model if type(model) == str else model.name  # type: ignore
            return self._models[model_name]
        except (KeyError, AttributeError) as k:
            raise KeyError(
                f"Could not locate the model '{model}' in the pipeline {self.name} models: {self.model_names}. "
                "Have you registered it correctly?"
            ) from k
