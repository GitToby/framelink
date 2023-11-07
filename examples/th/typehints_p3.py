from typing import Callable
import pandas as pd


# Now lets see if we can provide the context of the models we have already registered. This way we can start
# articulating the links between them and understand their relationships with natural language.
# It will also let us do extra pieces, such as if we call a model a second time we want to return a cached result.
#  So if 2 downstream models require the same upstream we only build it once.
# This should let us skip doing things like this:


# def wrong_join_children_with_favorite_food():
#     # Merge Children and Foods on 'Favorite Food'
#     merged_df = df_children().merge(df_foods(), left_on="Favorite Food", right_on="Food Name", how="left")

#     # Drop the redundant 'Food Name' column, if needed
#     merged_df = merged_df.drop(columns=["Food Name"])

#     return merged_df


# and allow us to do this:


# @model()
# def right_join_children_with_favorite_food(ctx: SomeCtxType) -> pd.DataFrame:
#     # Extract data from the context which potentially has been prebuilt
#     _df_foods = ctx.get(df_foods)
#     _df_children = ctx.get(df_children)

#     # Merge Children and Foods on 'Favorite Food'
#     merged_df = _df_children.merge(_df_foods, left_on="Favorite Food", right_on="Food Name", how="left")

#     # Drop the redundant 'Food Name' column, if needed
#     merged_df = merged_df.drop(columns=["Food Name"])

#     return merged_df


# so how do we do this? lets break it up into parts - we need the context object to store and retrieve the
# dataframes produced and we need to register models against the context.
# Lets start witht the latter, registering models in a way that lets us inject them to the ctx.
# The type hinting is a little messy but it will be cleaned up down the line

ctx = {}


class Context:
    _models: dict[str, Callable[["Context"], pd.DataFrame]] = {}
    _cache: dict[str, pd.DataFrame] = {}

    # This is our decorator
    def model(self, func: Callable[["Context"], pd.DataFrame]):
        self._models[func.__name__] = func
        return func

    # this is the get functionality
    def get(self, model: str | Callable[["Context"], pd.DataFrame]) -> pd.DataFrame:
        # extract model name as needed
        # pyrte complains about the str type not having __name__ but holistically htis is correct
        model_name: str = model if type(model) == str else model.__name__  # type: ignore

        # check cache for res
        if model_name in self._cache:
            return self._cache[model_name]
        else:
            # on miss build the model and cache it
            model = self._models[model_name]
            res = model(self)
            self._cache[model_name] = res
            return res


# Now using this context we are able to register models against it and make calls that can leverage this cacheing
ctx = Context()


@ctx.model
def df_parents(_: Context) -> pd.DataFrame:
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


@ctx.model
def df_children(_: Context) -> pd.DataFrame:
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


@ctx.model
def df_foods(_: Context) -> pd.DataFrame:
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


@ctx.model
def join_children_with_favorite_food(ctx: Context) -> pd.DataFrame:
    # Extract data from the context which potentially has been prebuilt
    _df_foods = ctx.get(df_foods)
    _df_children = ctx.get(df_children)

    # Merge Children and Foods on 'Favorite Food'
    merged_df = _df_children.merge(_df_foods, left_on="Favorite Food", right_on="Food Name", how="left")

    # Drop the redundant 'Food Name' column, if needed
    merged_df = merged_df.drop(columns=["Food Name"])
    return merged_df


food_res_df = ctx.get(join_children_with_favorite_food)
print(food_res_df.head(1))
# ...
