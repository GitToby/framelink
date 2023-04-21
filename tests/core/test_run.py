import pandas as pd

from framelink.core import FramelinkPipeline


def test_model_run(initial_framelink):
    pipeline, src_frame = initial_framelink

    @pipeline.model()
    def head_model(ctx: FramelinkPipeline) -> pd.DataFrame:
        df_out = ctx.ref(src_frame).head()
        return df_out

    res = pipeline.build(head_model)
    assert len(res) == 5
    assert type(res) == pd.DataFrame
    assert head_model.call_count == 1
