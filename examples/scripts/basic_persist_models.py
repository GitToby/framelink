import logging
from pathlib import Path
import random
import time
import uuid

import pandas as pd

from framelink._core import FramelinkPipeline, FramelinkSettings
from framelink.storage._core import PickleStorage, NoStorage
from typing import reveal_type

logging.basicConfig()

random.seed(123)

DATA_DIR = Path(__file__).parent.resolve() / "data"
DATA_DIR.mkdir(exist_ok=True)

settings = FramelinkSettings(default_log_level=logging.INFO, default_storage=NoStorage())
pipeline = FramelinkPipeline(settings=settings)
pickle_storage = PickleStorage(data_dir=DATA_DIR)


@pipeline.model()
def src_model_1(_: FramelinkPipeline) -> pd.DataFrame:
    """
    Produce a random dataframe of values for examples, ids 0 -> 9999
    """
    n = 10000
    data = {
        "id_col": list(range(n)),
        "colour_col": [random.choice(["red", "green", "blue"]) for _ in range(n)],
        "uuid_col": [str(uuid.uuid4()) for _ in range(n)],
        "int_col": [random.randint(0, 100) for _ in range(n)],
    }
    return pd.DataFrame(data)


@pipeline.model(storage=pickle_storage)
def src_model_2(_: FramelinkPipeline) -> pd.DataFrame:
    """
    Produce a random dataframe of values for examples, ids 10000 -> 19999
    """
    n = 10000
    data = {
        "id_col": list(x + 100 for x in range(n)),
        "colour_col": [random.choice(["yellow", "grey", "blue"]) for _ in range(n)],
        "uuid_col": [str(uuid.uuid4()) for _ in range(n)],
        "int_col": [random.randint(-100, 100) for _ in range(n)],
    }
    time.sleep(2)
    return pd.DataFrame(data)


@pipeline.model()
def concat_src_frame(ctx: FramelinkPipeline) -> pd.DataFrame:
    f1: pd.DataFrame = ctx.ref(src_model_1)
    f2: pd.DataFrame = ctx.ref(src_model_2)
    return pd.concat((f1, f2), ignore_index=True)


@pipeline.model()
def only_blue_records_by_int_col(ctx: FramelinkPipeline) -> pd.DataFrame:
    res: pd.DataFrame = ctx.ref(concat_src_frame)
    src_blue = res.loc[res["colour_col"] == "blue", :]
    src_blue = src_blue.sort_values(by=["int_col"])
    return src_blue


@pipeline.model()
def blue_head(ctx: FramelinkPipeline) -> pd.DataFrame:
    frame: pd.DataFrame = ctx.ref(only_blue_records_by_int_col)
    return frame.head()


@pipeline.model()
def blue_tail(ctx: FramelinkPipeline) -> pd.DataFrame:
    """
    Only the blue ones
    """
    ctx.log.info("logging inside model..")
    frame: pd.DataFrame = ctx.ref(only_blue_records_by_int_col)
    return frame.tail()


print(blue_tail.__module__)
print(blue_tail.__name__)
print(blue_tail.__qualname__)
print(blue_tail.__doc__)
print(blue_tail.__annotations__)
reveal_type(blue_tail)
# res = blue_head(pipeline)
# pipeline.build(
#     blue_tail,
# )
