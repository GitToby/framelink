import random
import uuid
from pathlib import Path

import pandas as pd
import pytest

from framelink.core import FramelinkModel, FramelinkPipeline, F

TEST_ROOT_DIR = Path(__file__).parent
DATA_DIR = TEST_ROOT_DIR.parent.parent / "data"

random.seed(0)


@pytest.fixture
def src_frame() -> F[pd.DataFrame]:
    def src_frame(_: FramelinkPipeline) -> pd.DataFrame:
        """
        Mock data frame for testing purposes
        :return: mock data
        """
        n = 10
        return pd.DataFrame(
            {
                "id_col": list(range(n)),
                "string_col": [random.choice(["apple", "banana", "cherry"]) for _ in range(n)],
                "uuid_col": [str(uuid.uuid4()) for _ in range(n)],
                "int_col": [random.randint(0, 100) for _ in range(n)],
            }
        )

    return src_frame


@pytest.fixture
def empty_framelink() -> FramelinkPipeline:
    return FramelinkPipeline()


@pytest.fixture
def initial_framelink(src_frame) -> tuple[FramelinkPipeline, FramelinkModel[pd.DataFrame]]:
    """
    :return: pipeline with a src frame already attached
    """
    pipeline: FramelinkPipeline = FramelinkPipeline()
    src_frame_model = pipeline.model()(src_frame)
    return pipeline, src_frame_model
