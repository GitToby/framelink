from time import perf_counter
from typing import Any, Callable, Generic, TypeVar
import pandas as pd


# Great now how about we wrap the callable in an isolated object which allows us to understand its frame of refeance
# in the call graph that we just made
# We also want to type this class in such a way that the original functions information is not lost, or that we can
#  mix and match return types and get nice type hinting.
# The last piece of code shows what information we can attain from having this wrappping, while having the usefulness
#  of the build

RT = TypeVar("RT")


class WrappedModel(Generic[RT]):
    def __init__(self, callable: Callable[["Context"], RT]) -> None:
        self.__name__ = callable.__name__
        # note, here is where we can update the wrapper in a simmilar mannar to @wraps or update_wrapper
        self._callable = callable
        self._calls = []

    def __call__(self, ctx: "Context") -> RT:
        start_time = perf_counter()
        res = self._callable(ctx)
        total_time = perf_counter() - start_time
        self._calls.append(total_time)
        return res

    @property
    def name(self) -> str:
        return self.__name__

    @property
    def call_count(self) -> int:
        return len(self._calls)

    @property
    def avg_call_duration(self) -> float:
        return sum(self._calls) / self.call_count


class Context:
    _models: dict[str, WrappedModel[Any]] = {}
    _cache: dict[str, Any] = {}

    # This is our decorator
    def model(self, func: Callable[["Context"], RT]) -> WrappedModel[RT]:
        _wrapped_model = WrappedModel[RT](func)
        self._models[func.__name__] = _wrapped_model
        return _wrapped_model

    # this is the get functionality
    def get(self, model: str | WrappedModel[RT]) -> RT:
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
            "Parent Name": ["John", "Sarah", "Michael", "Emily", "David"],
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
print(
    f"so far we have called {join_children_with_favorite_food.name} {join_children_with_favorite_food.call_count}"
    f" with an average time of {join_children_with_favorite_food.avg_call_duration}ms"
)
