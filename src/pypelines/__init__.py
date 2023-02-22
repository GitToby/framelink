from pathlib import Path

import polars as pl

from pypelines.core import Pypeline

root = Path(__file__)
data_ = root.parent.parent.parent / "data"


def src_frame(_: Pypeline) -> pl.LazyFrame:
    """
    Im a docstring!!
    """
    return pl.scan_csv(data_ / "*")


def model_1(ctx: Pypeline) -> pl.DataFrame:
    df_out = ctx.ref("src_frame").limit(10).collect()
    return df_out


def model_2(ctx: Pypeline) -> pl.LazyFrame:
    """
    I'm a docstring too!!
    """
    df_out = ctx.ref("src_frame").select(
        [
            pl.col("start_station_name"),
            pl.col("start_station_name").str.lengths().alias("start_station_name_len"),
        ]
    )
    return df_out


def model_3(ctx: Pypeline) -> pl.DataFrame:
    df_out = ctx.ref("model_1").collect().filter(pl.col("start_lng") > 40.9).collect()
    return df_out


pypeline = Pypeline()
pypeline.add_model(src_frame)
pypeline.add_model(model_1)
pypeline.add_model(model_2)
pypeline.add_model(model_3)

print(pypeline.build("src_frame"))
print(pypeline.build("model_1"))
print(pypeline.build("model_2"))
print(pypeline.build("model_3").collect())

get = pypeline.get("model_2")
get = pypeline["model_1"]
assert get
print()
