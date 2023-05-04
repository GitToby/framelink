import polars as pl
import pandas as pd
import pytest as pytest

from framelink.core import FramelinkPipeline


def test_no_persistence(initial_framelink):
    pipeline, src_frame = initial_framelink

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


@pytest.mark.skip(reason="todo")
def test_cacheing_off():
    pass


@pytest.mark.skip(reason="todo")
def test_override_cacheing():
    pass


@pytest.mark.skip(reason="todo")
def test_model_run_to_nearest_cache():
    pass
