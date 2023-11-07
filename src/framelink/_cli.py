import logging
from dataclasses import dataclass, field
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from typing import Optional, TYPE_CHECKING

import typer
from rich import print

if TYPE_CHECKING:
    from framelink._core import FramelinkPipeline

_app = typer.Typer()


@dataclass
class CliContext:
    fl_pipelines: dict[str, "FramelinkPipeline"] = field(default_factory=dict)
    verbose: bool = False
    pipeline_name: Optional[str] = None


CLI_CONTEXT = CliContext()


@_app.callback()
def set_up(
    main_file: Path = typer.Argument(..., exists=True, readable=True, writable=False, file_okay=True, dir_okay=False),
    verbose: bool = typer.Option(False, "-v"),
    pipeline_name: Optional[str] = typer.Option(None),
):
    """
    The main setup script that's run before all commands.
    """
    main_file = main_file.resolve()
    print(f"Using pipelines defined from {main_file}")
    _import_pipelines_from_file(main_file)

    logging.basicConfig(level=logging.INFO)
    if verbose:
        print("Using verbose output")
        print(f"{CLI_CONTEXT=}")
        logging.basicConfig(level=logging.DEBUG)

    if len(CLI_CONTEXT.fl_pipelines) > 1 and not pipeline_name:
        raise ValueError("More than one pipeline to work with, please pass in a pipeline name.")

    CLI_CONTEXT.verbose = verbose
    CLI_CONTEXT.pipeline_name = pipeline_name


@_app.command()
def list_models(graph: bool = typer.Option(False, "-g")):
    """
    lists the models in the current framelink
    """

    print(f"listing models {graph=}")
    print(f"Model names: {list(CLI_CONTEXT.fl_pipelines.values())[0].model_names}")


def _import_pipelines_from_file(main_file: Path) -> None:
    """
    This function will, given a python file, import the code. If it contains a Framelink pipeline we will have the
    context available for inspection and execution.
    :param main_file: the path to the python file to be imported.
    """
    if main_file.suffix != ".py":
        raise ImportError(f"{main_file} is not a .py file and is unsupported in this version of framelink")

    loader = SourceFileLoader("scripts", str(main_file))
    spec = spec_from_loader("scripts", loader)
    if spec:
        mymodule = module_from_spec(spec)
        loader.exec_module(mymodule)
    else:
        raise ImportError("Failed import :(")


@_app.command()
def build(model_name: str = typer.Argument(...)):
    """

    :param model_name:
    :return:
    """
    print(f"building {model_name}")


@_app.command()
def deploy_to(target_name: str = typer.Argument(...)):
    """

    :param model_name:
    :return:
    """
    print(f"building deployment target {target_name}")


if __name__ == "__main__":
    _app()
