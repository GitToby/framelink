import uuid
from typing import Generator

import networkx as nx
import pandas as pd
import pytest

from framelink import FramelinkPipeline


def test_merge_pipelines_missing_merge(src_frame):
    pipeline_1 = FramelinkPipeline("pipeline_1")
    pipeline_2 = FramelinkPipeline("pipeline_2")

    src_frame = pipeline_1.model()(src_frame)

    @pipeline_1.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    with pytest.raises(KeyError) as ke:

        @pipeline_2.model()
        def tail_model(ctx: FramelinkPipeline) -> pd.DataFrame:
            df_out = ctx.ref(src_frame).tail()
            return df_out

    assert "src_frame" in str(ke.value)
    assert "could not locate" in str(ke.value).lower()


def test_topological_sorted_nodes():
    pipeline = FramelinkPipeline()

    @pipeline.model()
    def src_frame_1(_: FramelinkPipeline) -> pd.DataFrame:
        n = 10
        return pd.DataFrame({"id_col": [uuid.uuid4() for _ in range(n)]})

    @pipeline.model()
    def src_frame_2(_: FramelinkPipeline) -> pd.DataFrame:
        n = 10
        return pd.DataFrame({"id_col2": [uuid.uuid4() for _ in range(n)]})

    @pipeline.model()
    def merge_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        src_1 = ctx.ref(src_frame_1)
        src_2 = ctx.ref(src_frame_2)
        return pd.merge(src_1, src_2)

    @pipeline.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        merged = ctx.ref(merge_model)
        return merged.head()

    assert len(pipeline) == 4

    ts = pipeline.topological_sorted_nodes()

    assert isinstance(ts, Generator)

    ts_l = list(ts)
    assert len(ts_l) == 3
    assert all(type(e) == tuple for e in ts_l)
    assert ts_l == [(src_frame_1, src_frame_2), (merge_model,), (head_model,)]


@pytest.mark.xfail(reason="joining isn't finished yet")
def test_merge_pipelines_distinct():
    pipeline_1 = FramelinkPipeline("pipeline_1")
    pipeline_2 = FramelinkPipeline("pipeline_2")

    @pipeline_1.model()
    def src_frame_1(_: FramelinkPipeline) -> pd.DataFrame:
        n = 10
        return pd.DataFrame({"id_col": [uuid.uuid4() for _ in range(n)]})

    @pipeline_1.model()
    def src_frame_2(_: FramelinkPipeline) -> pd.DataFrame:
        n = 10
        return pd.DataFrame({"id_col2": [uuid.uuid4() for _ in range(n)]})

    @pipeline_1.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        src_1 = ctx.ref(src_frame_1)
        src_2 = ctx.ref(src_frame_2)
        merged = pd.merge(src_1, src_2)
        return merged.head()

    @pipeline_2.model()
    def src_frame_3(_: FramelinkPipeline) -> pd.DataFrame:
        n = 10
        return pd.DataFrame({"id_col3": [uuid.uuid4() for _ in range(n)]})

    @pipeline_2.model()
    def tail_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame_3).tail()
        return df_out

    res = FramelinkPipeline.merge_pipelines("merged", collection=[pipeline_1, pipeline_2])

    ts_1_g = pipeline_1.topological_sorted_nodes()
    list(ts_1_g)
    list(pipeline_2.topological_sorted_nodes())

    assert res.name == "merged"
    assert nx.is_directed_acyclic_graph(res.graph)
    assert nx.number_weakly_connected_components(pipeline_1.graph) == 1
    assert nx.number_weakly_connected_components(pipeline_2.graph) == 1
    assert nx.number_weakly_connected_components(res.graph) == 2
    assert len(res) == 4
    assert all(
        n in res.model_names
        for n in (
            "src_frame_1",
            "head_model",
            "src_frame_2",
            "tail_model",
        )
    )
    assert head_model in src_frame_1.downstreams
    assert src_frame_1 in head_model.upstreams
    assert tail_model in src_frame_2.downstreams
    assert src_frame_2 in tail_model.upstreams
