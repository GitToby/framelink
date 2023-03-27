import inspect
import time
from dataclasses import dataclass
from functools import lru_cache
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Callable, Generic, Iterator, Optional, TypeVar, Union


@dataclass
class FramelinkSettings:
    """Settings to be applied by the"""

    name: str = "default"
    persist_models_dir: Path = Path(__file__).parent.parent / "data"


FRAME = TypeVar("FRAME")  # usually one of pl.DataFrame, pl.DataFrame, pl.LazyFrame.. etc


class FramelinkModel(Generic[FRAME]):
    """ """

    _callable: "PYPE_MODEL"
    graph_ref: TopologicalSorter
    call_perf: tuple[float, ...] = tuple()

    def __init__(
        self,
        model_func: "PYPE_MODEL",
        graph: TopologicalSorter,
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
        self.graph_ref = graph

    def __name__(self):
        return self._callable.__name__

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
    def source(self) -> str | None:
        """ """
        try:
            source__ = inspect.getsource(self._callable)
            source__ = source__.strip()
        except OSError:
            source__ = None
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

    def __key(self) -> tuple[str, str | None, bool]:
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

    _model_link: dict["PYPE_MODEL", FramelinkModel]
    _models: TopologicalSorter[FramelinkModel]

    def __init__(self, settings: FramelinkSettings = FramelinkSettings()):
        super().__init__()
        self._model_link = dict()
        self._models = TopologicalSorter()
        self.settings = settings

    def __repr__(self):
        return f"<{self.__class__.__name__} with {len(self)} models at {hex(id(self))}>"

    @property
    def model_names(self) -> list[str]:
        """Return a list of model names registered to this pipeline"""
        return sorted(m.name for m in self._model_link.values())

    def model(self, *, persist_after_run=False, cache_result=True) -> Callable[["PYPE_MODEL"], "PYPE_MODEL"]:
        """Annotation to register a model to the pypeline.

        :param persist_after_run: Write the file to disk after running this model. The approach to writing the model is
            defined in the :FramelinkSettings: (Default value = False)
        :param cache_result:  (Default value = True)
        """

        def _decorator(func: "PYPE_MODEL") -> "PYPE_MODEL":
            """Internal wrapping of the model function to produce the metadata about the model.

            :param func: "PYPE_MODEL":
            """

            # todo: parse model and work out upstreams.
            model_wrapper: FramelinkModel = FramelinkModel(
                func,
                self._models,
                persist_after_run=persist_after_run,
                cache_result=cache_result,
            )
            # we need to keep a ref to the underlying graph
            self._model_link[func] = model_wrapper
            self._models.add(model_wrapper)
            return func

        return _decorator

    def __len__(self) -> int:
        return len(self._model_link.keys())

    def __iter__(self) -> Iterator[FramelinkModel]:
        return self._model_link.values().__iter__()

    def __contains__(self, item: Union[FramelinkModel, "PYPE_MODEL"]):
        return item in self._model_link.keys() or item in self._model_link.values()

    def get(self, model: "PYPE_MODEL") -> FramelinkModel:
        return self._model_link[model]

    # question: why does this not type check correctly on `FramelinkModel` model
    def ref(self, model: "PYPE_MODEL") -> FRAME:
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
            raise KeyError(f"No key {model.__name__}") from ke

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
        return tuple(hash(m) for m in self._model_link.values())

    def __hash__(self) -> int:
        return hash(self.__key())


PYPE_MODEL = Callable[[FramelinkPipeline[FRAME]], FRAME]
