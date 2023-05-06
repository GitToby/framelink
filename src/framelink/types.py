from typing import Callable, TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from framelink.core import FramelinkPipeline

T = TypeVar("T")  # usually one of pl.DataFrame, pl.DataFrame, pl.LazyFrame.. etc
F = Callable[["FramelinkPipeline"], T]  # the definition of one of the models users will write
