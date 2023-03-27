import random
import uuid
from pathlib import Path

import pandas as pd
import pytest

from framelink.core import FramelinkPipeline, PYPE_MODEL

TEST_ROOT_DIR = Path(__file__).parent
DATA_DIR = TEST_ROOT_DIR.parent.parent / "data"


@pytest.fixture
def src_frame() -> PYPE_MODEL:
    def inner(_: FramelinkPipeline):
        """
        Mock data frame for testing purposes
        :return: mock data result
        """
        data = {'string_col': [random.choice(['apple', 'banana', 'cherry']) for _ in range(10)],
                'uuid_col': [str(uuid.uuid4()) for _ in range(10)],
                'int_col': [random.randint(0, 100) for _ in range(10)]}
        return pd.DataFrame(data)

    return inner


@pytest.fixture
def empty_pipeline():
    return FramelinkPipeline()


@pytest.fixture
def initial_framelink(src_frame):
    """
    :return: pipeline with a src frame already attached
    """
    pipeline = FramelinkPipeline()
    pipeline.model()(src_frame)
    return pipeline, src_frame
