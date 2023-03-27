import pandas as pd
import polars as pl
import pytest

from framelink.core import FramelinkPipeline


def test_model_registration(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def src_frame(_: FramelinkPipeline) -> pl.LazyFrame:
        """
        Im a docstring!
        """
        return pl.LazyFrame()

    # pypeline registration
    assert "src_frame" in pipeline.model_names
    assert src_frame in pipeline.keys()

    src_frame_model = pipeline[src_frame]
    assert src_frame_model.name == "src_frame"
    assert src_frame_model.call_count == 0


def test_model_linking_linear(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def model_1(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    assert len(pipeline) == 2
    assert pipeline.get(model_1)


def test_model_ref(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def model_head(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    def model_tail(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).tail()
        return df_out

    assert len(pipeline) != 3
    assert pipeline.get(model_head)
    assert not pipeline.get(model_tail)  # return None

    ref_res = pipeline.ref(model_head)
    assert ref_res is not None
    assert len(ref_res) == 5

    with pytest.raises(KeyError) as e:
        pipeline.ref(model_tail)
        assert "model_tail" in e.value


def test_get_metadata(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def model_head(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    @pipeline.model()
    def model_tail(ctx: FramelinkPipeline) -> pd.DataFrame:
        """
        Returns the bottom 5 records of the src_model
        """
        df_out = ctx.ref(src_frame).tail()
        return df_out

    assert pipeline[model_head].docstring is None
    assert pipeline[model_tail].docstring == "Returns the bottom 5 records of the src_model"
    assert pipeline[model_tail].source == ('@pipeline.model()\n'
                                           '    def model_tail(ctx: FramelinkPipeline) -> pd.DataFrame:\n'
                                           '        """\n'
                                           '        Returns the bottom 5 records of the src_model\n'
                                           '        """\n'
                                           '        df_out = ctx.ref(src_frame).tail()\n'
                                           '        return df_out')
