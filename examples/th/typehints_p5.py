import logging
import pandas as pd

from framelink import FramelinkPipeline, FramelinkSettings
from framelink.storage import PickleStorage, NoStorage

# This has essentially been the development process of a tool i made called framelink, it allows you pythonic control
#  over you modeling in a way that gives you lots for free.
# There is a question around why - well having lots of metadata is a data engineers dream. Having this information
# lets you optimise models and produce results faster.
# It also allows us to use some of the useful tools such as deployments to different platforms all for free, as
# demonstrated below.

settings = FramelinkSettings(default_log_level=logging.INFO, default_storage=NoStorage())
pipeline = FramelinkPipeline(settings=settings)
pickle_storage = PickleStorage(data_dir="./.data")


@pipeline.model()
def df_parents(_: FramelinkPipeline) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "Parent Name": ["John", "Sarah", "Michael", "Emily", "David"],
            "Age": [40, 35, 42, 37, 39],
            "Job": ["Engineer", "Teacher", "Doctor", "Designer", "Lawyer"],
            "City": ["New York", "Los Angeles", "Chicago", "San Francisco", "Boston"],
            "Favorite Food": ["Ice Cream", "Sushi", "Burgers", "Pasta", "Burgers"],
        }
    )
    return df


@pipeline.model()
def df_children(_: FramelinkPipeline) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "Child Name": ["Anna", "Ben", "Olivia", "Ethan", "Sophia"],
            "Parent Name": ["John", "Sarah", "Michael", "John", "John"],
            "Age": [8, 6, 9, 7, 8],
            "Favorite Food": ["Ice Cream", "Burgers", "Ice Cream", "Mac and Cheese", "Burgers"],
            "Favorite School Subject": ["Maths", "Art", "Science", "History", "English"],
            "Has Siblings": [True, False, True, True, True],
        }
    )
    return df


@pipeline.model()
def df_foods(_: FramelinkPipeline) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "Food Name": [
                "Sushi",
                "Pasta",
                "Burgers",
                "Ice Cream",
                "Mac and Cheese",
            ],
            "Calories": [330, 250, 550, 220, 320],
            "Average Price": [35.99, 15.99, 9.99, 4.99, 7.99],
        }
    )
    return df


@pipeline.model()
def join_children_with_favorite_food(ctx: FramelinkPipeline) -> pd.DataFrame:
    # Extract data from the context which potentially has been prebuilt
    _df_foods = pipeline.ref(df_foods)
    _df_children = pipeline.ref(df_children)

    # Merge Children and Foods on 'Favorite Food'
    merged_df = _df_children.merge(_df_foods, left_on="Favorite Food", right_on="Food Name", how="left")

    # Drop the redundant 'Food Name' column, if needed
    merged_df = merged_df.drop(columns=["Food Name"])
    return merged_df


food_res_df = join_children_with_favorite_food.build(pipeline)
print(food_res_df.head(1))
print(join_children_with_favorite_food.upstreams)
print(pipeline.graph_dot)

# with the shipped cli we can also do things such as
# $ framelink list-models
# or, in the future something like
# $ framelink deploy-to airflow
