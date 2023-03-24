from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from framelink.core import FramelinkPipeline

root = Path(__file__)
data_ = root.parent.parent.parent / "data"


@pytest.fixture
def pipeline():
    return FramelinkPipeline()


def test_model_registration(pipeline):
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


def test_model_linking_linear(pipeline):
    @pipeline.model()
    def src_frame(_: FramelinkPipeline) -> pd.DataFrame:
        return pd.DataFrame()

    @pipeline.model()
    def model_1(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    assert len(pipeline) == 2
    assert pipeline.get(
        model_1,
    )


# @pypeline.model()
# def model_2(ctx: Pypeline) -> pl.LazyFrame:
#     """
#     I'm a docstring too!!
#     """
#     df_out = ctx.ref(src_frame).select(
#         [
#             pl.col("start_station_name"),
#             pl.col("start_station_name").str.lengths().alias("start_station_name_len"),
#         ]
#     )
#     return df_out
#
#
# @pypeline.model()
# def model_3(ctx: Pypeline) -> pl.DataFrame:
#     df_out = ctx.ref(model_1).collect().filter(pl.col("start_lng") > 40.9).collect()
#     return df_out
#
#
# print(pypeline.model_names)
# print(pypeline.build(src_frame))
# pypeline.values()
# print(pypeline.build(model_1))
# print(pypeline.build(model_2))
# print(pypeline.build(model_3).collect())
#
# get = pypeline.get("model_2")
# get = pypeline["model_1"]
# assert get
# print()


def test_persistance():
    pass


def test_cacheing_on():
    pass


def test_cacheing_off():
    pass


def test_override_cacheing():
    pass


def test_merge_multiple_FramelinkPipelines():
    pass


def test_model_links():
    pass


def test_model_run_to_nearest_cache():
    pass


def test_model_run_downstream():
    pass
