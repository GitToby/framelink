import pandas as pd
import polars as pl
import pytest

from framelink.core import FramelinkModel, FramelinkPipeline


def test_model_registration(empty_framelink):
    pipeline = empty_framelink

    @pipeline.model()
    def src_frame(_: FramelinkPipeline) -> pl.LazyFrame:
        """
        Im a docstring!
        """
        return pl.LazyFrame()

    # naming things
    assert src_frame.name == "src_frame"
    assert src_frame.name == src_frame.__name__

    # metadata of model
    assert "Im a docstring!" in src_frame.docstring
    assert src_frame.call_count == 0
    assert src_frame.perf_stats == tuple()

    # model settings
    assert src_frame._log is not None

    # association to pipeline
    assert src_frame in pipeline
    assert src_frame.name in pipeline.model_names


def test_model_linking_linear(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def model_1(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    @pipeline.model()
    def model_2(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(model_1).sort_values("int_col").tail(2)
        return df_out

    assert len(pipeline) == 3  # including the initial one
    assert model_1 in pipeline
    assert model_2 in pipeline
    assert pipeline.model_names == ["model_1", "model_2", "src_frame"]
    assert src_frame in model_1.upstreams
    assert model_2 in model_1.downstreams


def test_model_ref(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    def model_tail(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).tail()
        return df_out

    # basic assertions for registered models
    assert len(pipeline) == 2
    assert type(head_model) == FramelinkModel
    assert type(src_frame) == FramelinkModel
    assert head_model in pipeline
    assert src_frame in pipeline
    assert pipeline.model_names == ["head_model", "src_frame"]

    # ref models
    ref_res = pipeline.ref(head_model)
    assert ref_res is not None
    assert len(ref_res) == 5

    # missing models
    assert model_tail not in pipeline
    with pytest.raises(KeyError) as e1:
        pipeline.ref(model_tail)

    assert "model_tail" in str(e1.value)

    # missing intermediate model.
    with pytest.raises(KeyError) as e2:

        @pipeline.model()
        def downstream_model(ctx: FramelinkPipeline):

            return ctx.ref(model_tail).head(2)  # type: ignore this is expected

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
    assert head_model.docstring is None
    assert tail_model.docstring == "Returns the bottom 5 records of the src_model"
    assert tail_model.source == (
        "@pipeline.model()\n"
        "def tail_model(ctx: FramelinkPipeline) -> pd.DataFrame:\n"
        '    """\n'
        "    Returns the bottom 5 records of the src_model\n"
        '    """\n'
        "    df_out = ctx.ref(src_frame).tail()\n"
        "    return df_out"
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

    assert not isinstance(head_model_2, FramelinkModel)
    head_model_2_wrapped = pipeline.model()(head_model_2)
    assert isinstance(head_model_2_wrapped, FramelinkModel)


def test_get_lookups(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    lookup_model = pipeline.get(head_model)
    assert lookup_model
    assert lookup_model is head_model

    lookup_str = pipeline.get("head_model")
    assert lookup_str
    assert lookup_str is head_model


def test_ref_string(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref("src_frame").head()
        return df_out

    # black should keep the quotes in the below model as '
    # fmt: off
    @pipeline.model()
    def tail_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref('src_frame').head()
        return df_out

    # fmt: on

    assert head_model
    assert head_model in src_frame.downstreams
    assert src_frame in head_model.upstreams
