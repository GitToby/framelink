![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/gittoby/framelink/lint_test_build.yml)
![GitHub Release Date](https://img.shields.io/github/release-date/GitToby/framelink)

Famelink is a simple wrapper that will provide context into pandas, polars and other Dataframe engines. See roadmap
below for future goals of the project.

**This project is still in prerelease, consider the API unstable. Any usage should be pinned.**

```bash
pip install framelink
```

## Goals

Framelink should provide a way for collaborating teams to write python or SQL models to see their data flow easily and get the a whole load of stuff for free!

- **Simple to write**, writing models should be no harder than a function implementation but provide a dependancy tree, schemas & model metadata.
- **Simple to run**, Writing models should be agnostic of running models, once the models are written execution wrappers with diagnostics, tracing & liniage should be easy to derive for the execution platform any team is running without having any special requirments for running locally. 

## Concepts

- A **Pipeline** is a DAG of _Models_ that can be executed in a particular way.
- A **Model** is a definition of sourcing data and a transform.
- A **Frame** is a result of a model run.

## Example

```python
import os
from pathlib import Path

import pandas as pd
import polars as pl

from framelink.core import FramelinkPipeline, FramelinkSettings

settings = FramelinkSettings(
    persist_models_dir=Path(os.getcwd()) / "data"
)

pipeline = FramelinkPipeline(settings)


@pipeline.model(cache_result=True)
def src_frame_1(_: FramelinkPipeline) -> pd.DataFrame:
    return pd.DataFrame(data={
        "name": ["amy", "peter"],
        "age": [31, 12],
    })


@pipeline.model(cache_result=False)
def src_frame_2(_: FramelinkPipeline) -> pd.DataFrame:
    return pd.DataFrame(data={
        "name": ["amy", "peter", "helen"],
        "fave_food": ["oranges", "chocolate", "water"],
    })


@pipeline.model(persist_after_run=True)
def merge_model(ctx: FramelinkPipeline) -> pl.DataFrame:
    res_1 = ctx.ref(src_frame_1)
    res_2 = ctx.ref(src_frame_2)
    return pl.from_pandas(res_1).join(pl.from_pandas(res_2), on="name")


print(pipeline.model_names)
# build with implicit context
r_1 = pipeline.build(merge_model)
# build with explicit context
r_2 = merge_model(pipeline)

print(r_1)
print(r_2)
# shape: (2, 3)
# ┌───────┬─────┬───────────┐
# │ name  ┆ age ┆ fave_food │
# │ ---   ┆ --- ┆ ---       │
# │ str   ┆ i64 ┆ str       │
# ╞═══════╪═════╪═══════════╡
# │ amy   ┆ 31  ┆ oranges   │
# │ peter ┆ 12  ┆ chocolate │
# └───────┴─────┴───────────┘
```

## Roadmap

This could change...

### v0.1.1

- [ ] Diagramming and tracking of the model DAG
- [ ] Test Coverage reporting

### v1.0.0
- [ ] Runtime adoption of the `FramelinkPipeline` by Airflow, Dagster, Prefect etc
- [ ] Open Tracing integration
