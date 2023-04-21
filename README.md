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

- **Simple to write** - writing models should be no harder than a function implementation but provide a dependency tree,
  schemas & model metadata.
- **Simple to run** - writing models should be agnostic of running models, once the models are written execution
  wrappers with diagnostics, tracing & lineage should be easy to derive for the execution platform any team is running without having any special requirements for running locally.
- **Scheduler agnostic** - we are not making a new airflow, dagster etc. Framelink serves to add metadata to a project
  for free.

## Concepts

- A **Pipeline** is a DAG of _models_ that can be executed in a particular way.
- A **Model** is a definition of sourcing data and, potentially, a transform. It's an ETL in its most basic form.
- A **Frame** is a result of a _model_ run.

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

pipeline = FramelinkPipeline(settings=settings)


@pipeline.model()
def src_frame_1(_: FramelinkPipeline) -> pd.DataFrame:
    return pd.DataFrame(data={
        "name": ["amy", "peter"],
        "age": [31, 12],
    })


@pipeline.model()
def src_frame_2(_: FramelinkPipeline) -> pd.DataFrame:
    return pd.DataFrame(data={
        "name": ["amy", "peter", "helen"],
        "fave_food": ["oranges", "chocolate", "water"],
    })


@pipeline.model()
def merge_model(ctx: FramelinkPipeline) -> pl.DataFrame:
    res_1 = ctx.ref(src_frame_1)
    res_2 = ctx.ref(src_frame_2)
    key = "name"
    ctx.log.info(f"Merging both sources on {key}")
    return pl.from_pandas(res_1).join(pl.from_pandas(res_2), on=key)


# build with implicit context
r_1 = pipeline.build(merge_model)
print(r_1)
# shape: (2, 3)
# ┌───────┬─────┬───────────┐
# │ name  ┆ age ┆ fave_food │
# │ ---   ┆ --- ┆ ---       │
# │ str   ┆ i64 ┆ str       │
# ╞═══════╪═════╪═══════════╡
# │ amy   ┆ 31  ┆ oranges   │
# │ peter ┆ 12  ┆ chocolate │
# └───────┴─────┴───────────┘

print(merge_model.upstreams)
# {<src_frame_2 at 0x1477c2c90>, <src_frame_1 at 0x144f0ab50>}

print(src_frame_1.downstreams)
# {<merge_model at 0x1477c2910>}

print(pipeline.model_names)
# ['merge_model', 'src_frame_1', 'src_frame_2']

print(list(pipeline.topological_sorted_nodes()))
# [(<src_frame_1 at 0x144f0ab50>, <src_frame_2 at 0x1477c2c90>), (<merge_model at 0x1477c2910>,)]

# if you have the graphing options engaged.
pipeline.graph_plt()  # will draw you a matplotlib of the DAG
dot = pipeline.graph_dot()  # will provide a DOT language representation of the DAG
```

## Feature Roadmap

This could change...

### v0.2.0

- [x] Model links & DAG implemented
- [x] Context logger available
- [x] Diagramming and tracking of the model DAG

### v0.3.0

- [ ] Cleaner graph results
- [ ] Merging of multiple framelink pipelines enabling
- [ ] Orchestration passthrough and local execution.

### v0.4.0

- [ ] Caches and auto-persistence
- [ ] Dynamic sourcing for models
- [ ] Cli to run a project

### v0.5.0

- [ ] SQL models & dbt, sqlmesh compatability
- [ ] Open Tracing integration
