from typing import Callable
import pandas as pd


# Now lets suppose we have a few models repesenting the parents and their children and some extra data about them.
#  These can be linked downstream to answer questions like "how many children does Sarah have?" or
#  "how much does Johns family spend on food?"

# We can keep track of these with a decorator that links the model name to its function.
#  This seems like a silly step but will make sense later on.

model_type = Callable[[], pd.DataFrame]
models: dict[str, model_type] = {}


def model(func: model_type):
    models[func.__name__] = func
    return func


@model
def df_parents() -> pd.DataFrame:
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


@model
def df_children() -> pd.DataFrame:
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


@model
def df_foods() -> pd.DataFrame:
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


# this will let us register all our models and see them in one place. By typehinting correctly we can repack the
# types down the line too
print(models)
child_model = models["df_children"]
print(child_model().head(1))
# ...
