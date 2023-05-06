from unittest.mock import MagicMock

import pandas as pd
import polars as pl
import pytest as pytest

from framelink.core import FramelinkPipeline
from framelink.storage.core import _NoStorage


def test_no_persistence(initial_framelink):
    pipeline, src_frame = initial_framelink

    assert isinstance(pipeline.settings.default_storage, _NoStorage)

    wrapped_store = MagicMock(wraps=pipeline.settings.default_storage)
    pipeline.settings.default_storage = wrapped_store

    @pipeline.model()
    def head_model(ctx: FramelinkPipeline) -> pl.DataFrame:
        df_out = ctx.ref(src_frame).head()
        pl_df = pl.from_pandas(df_out)
        return pl_df

    @pipeline.model()
    def tail_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        """
        Returns the bottom 5 records of the src_model
        """
        df_out = ctx.ref(src_frame).tail()
        return df_out

    _ = pipeline.build(tail_model)
    assert wrapped_store.call_count > 0


@pytest.mark.skip(reason="todo")
def test_in_memory_store():
    pass


@pytest.mark.skip(reason="todo")
def test_pickle_store():
    pass


@pytest.mark.skip(reason="todo")
def test_override_cacheing():
    pass


@pytest.mark.skip(reason="todo")
def test_model_run_to_nearest_cache():
    pass
