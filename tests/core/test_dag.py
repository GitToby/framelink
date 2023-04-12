import random
import uuid

import networkx as nx
import pandas as pd
import pytest as pytest

from framelink.core import FramelinkPipeline


def test_model_link_dag(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def src_model_2(_: FramelinkPipeline) -> pd.DataFrame:
        n = 10
        data = {
            "id_col": list(range(n)),
            "colour_col": [random.choice(["red", "green", "blue"]) for _ in range(n)],
            "uuid_col": [str(uuid.uuid4()) for _ in range(n)],
            "int_col": [random.randint(0, 100) for _ in range(n)],
        }
        return pd.DataFrame(data)

    @pipeline.model()
    def only_blue_records(ctx: FramelinkPipeline) -> pd.DataFrame:
        src_2 = ctx.ref(src_model_2)
        src_2_blue = src_2.loc[src_2["colour_col"] == "blue", :]
        return src_2_blue

    @pipeline.model()
    def simple_src_1_filter(ctx: FramelinkPipeline) -> pd.DataFrame:
        frame = ctx.ref(src_frame)
        frame = frame.loc[:10]
        return frame

    @pipeline.model()
    def simple_src_1_filter2(ctx: FramelinkPipeline) -> pd.DataFrame:
        frame = ctx.ref(only_blue_records)
        ctx.ref(simple_src_1_filter)
        return frame

    assert nx.is_directed(pipeline.graph)
    assert nx.is_directed_acyclic_graph(pipeline.graph)
    assert src_model_2 in only_blue_records.upstreams
    assert simple_src_1_filter in src_frame.downstreams
    assert simple_src_1_filter2 in only_blue_records.downstreams
    assert simple_src_1_filter2 in simple_src_1_filter.downstreams


def test_deg_raises_error_on_premature_import(initial_framelink):
    pipeline, _ = initial_framelink

    with pytest.raises(KeyError) as e:

        @pipeline.model()
        def model_1(ctx: FramelinkPipeline):
            m_2 = ctx.ref(model_2)
            return m_2.head()

        @pipeline.model()
        def model_2(ctx: FramelinkPipeline):
            m_1 = ctx.ref(model_1)
            return m_1.head

        assert "model_2" in e.value, "We should fail when trying to ref model_2 before import"


@pytest.mark.skip(reason="todo")
def test_model_run_downstream():
    pass
