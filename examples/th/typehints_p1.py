import pandas as pd


# Inital function, one that creates and returns a dataframe, you can think of this as the first step in making a
# batched data pipeline.


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


parents = df_parents()
# ...

# This is simple analytics that we can do anywhere, but lets pretend were more than just a person at a computer,
#  we want to make mode models.
#  Some of these models interact with eachother a bit like sql tables interact with eachother.
