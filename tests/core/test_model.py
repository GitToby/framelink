from typing import Callable

import pandas as pd
import polars as pl
import pytest

from framelink.core import FramelinkModel, FramelinkPipeline


def test_model_registration(empty_framelink):
    pipeline = empty_framelink

    @pipeline.model(persist_after_run=False, cache_result=True)
    def src_frame(_: FramelinkPipeline) -> pl.LazyFrame:
        """
        Im a docstring!
        """
        return pl.LazyFrame()

    assert "src_frame" in pipeline.model_names
    assert src_frame in pipeline
    src_frame_framelink_model = pipeline.get(src_frame)
    assert src_frame_framelink_model.name == src_frame.__name__
    # assert src_frame_framelink_model.docstring == src_frame.__doc__.strip()
    assert src_frame_framelink_model.persist_after_run is False
    assert src_frame_framelink_model.cache_result is True
    assert src_frame_framelink_model.call_count == 0
    assert src_frame_framelink_model.perf_stats == tuple()

    # todo: this should be the functionality, but my brain cant work out how decorators alter the underlying func object
    # Altering the pipeline.model() func to return the wrapped function makes the below work
    # assert src_frame.name == "src_frame"
    # assert src_frame.call_count == 0
    # However, it then makes the following fail and type hints to stop
    # pipeline.ref(src_frame)
    # This means theres an extra step to fetch all the meta details of the model with pipeline.get(src_frame)


def test_model_linking_linear(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def model_1(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    print(type(model_1))

    @pipeline.model()
    def model_2(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(model_1).sort_values("int_col").tail(2)
        return df_out

    assert len(pipeline) == 3  # including the initial one
    assert model_1 in pipeline
    assert model_2 in pipeline
    assert pipeline.model_names == ["model_1", "model_2", "src_frame"]
    # assert pipeline.get(model_1).upstream == pipeline.get("")
    # assert pipeline.get(model_1).downstream == pipeline.get(model_2)
    # assert pipeline.get(model_1).upstream


def test_model_ref(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    def model_tail(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).tail()
        return df_out

    assert len(pipeline) != 3
    assert head_model in pipeline
    # assert type(head_model) == PYPE_MODEL
    assert model_tail not in pipeline

    ref_res = pipeline.ref(head_model)
    assert ref_res is not None
    assert len(ref_res) == 5

    with pytest.raises(KeyError) as e1:
        pipeline.ref(model_tail)
        assert "model_tail" in str(e1.value)

    with pytest.raises(KeyError) as e2:

        @pipeline.model()
        def downstream_model(ctx: FramelinkPipeline):
            return ctx.ref(model_tail).head(2)

        pipeline.build(downstream_model)
        assert "model_tail" in str(e2.value)


def test_get_metadata(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    @pipeline.model()
    def tail_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        """
        Returns the bottom 5 records of the src_model
        """
        df_out = ctx.ref(src_frame).tail()
        return df_out

    assert head_model in pipeline
    assert tail_model in pipeline
    assert pipeline.get(head_model).docstring is None
    assert pipeline.get(tail_model).docstring == "Returns the bottom 5 records of the src_model"
    assert pipeline.get(tail_model).source == (
        "@pipeline.model()\n"
        "    def tail_model(ctx: FramelinkPipeline) -> pd.DataFrame:\n"
        '        """\n'
        "        Returns the bottom 5 records of the src_model\n"
        '        """\n'
        "        df_out = ctx.ref(src_frame).tail()\n"
        "        return df_out"
    )


def test_type_values(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    assert isinstance(head_model, FramelinkModel)

    def head_model_2(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    assert isinstance(head_model_2, Callable)
    head_model_2_wrapped = pipeline.model()(head_model_2)
    assert isinstance(head_model_2, Callable)
    assert isinstance(head_model_2_wrapped, FramelinkModel)
