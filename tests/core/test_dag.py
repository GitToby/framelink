import random
import uuid

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

    pipeline.build(only_blue_records)


@pytest.mark.skip(reason="todo")
def test_model_run_downstream():
    pass
