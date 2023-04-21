import logging

import pandas as pd

from framelink.core import FramelinkPipeline, FramelinkSettings


def test_basic_logging(src_frame, caplog):
    logging.basicConfig(level=logging.DEBUG)
    model_1_msg = "message from model_1!"
    model_1_msg_2 = "second part!"
    model_2_msg = "message from model_2!"

    pipeline = FramelinkPipeline(settings=FramelinkSettings(default_log_level=logging.INFO))
    src_frame = pipeline.model()(src_frame)
    og_logger = pipeline.log

    @pipeline.model()  # will be INFO level, as per default passed
    def model_1(ctx: FramelinkPipeline) -> pd.DataFrame:
        model_1_log = ctx.log
        assert ctx.log != og_logger

        ctx.log.warning(model_1_msg)
        df_out = ctx.ref(src_frame).head()

        assert ctx.log == model_1_log
        ctx.log.debug(model_1_msg_2)

        return df_out

    @pipeline.model(logging_level=logging.DEBUG)
    def model_2(ctx: FramelinkPipeline) -> pd.DataFrame:
        ctx.log.info(model_2_msg + " info!")
        ctx.log.debug(model_2_msg + " debug!")

        assert ctx.log != og_logger
        model_2_log = ctx.log

        df_out = ctx.ref(model_1).sort_values("int_col").tail(2)

        assert ctx.log == model_2_log
        return df_out

    all_messages = ", ".join(caplog.messages).lower()
    assert "register" in all_messages and "model_1" in all_messages
    assert "register" in all_messages and "model_2" in all_messages
    assert model_2_msg not in all_messages

    assert pipeline.log == og_logger
    pipeline.build(model_2)
    assert pipeline.log == og_logger
    all_messages_post_build = ", ".join(caplog.messages).lower()

    print(caplog.record_tuples)
    assert model_2_msg + " debug!" in all_messages_post_build
