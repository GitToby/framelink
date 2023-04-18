import re


def parse_model_src_for_internal_refs(model_src: str) -> set[str]:
    """
    This utility is for extracting the names any time a model has the line `.ref(<model_name>)` in. It's a util not part
        of the FramelinkModel as we don't really want this to be called more than once when registering the model. Once
        a model is registered we can identify the `.ref`d models with `.upstreams`.

    :param model_src: the FramelinkModel model source to search through.
    :return: a list of all the found model names which are `ref`d in the model source.
    """
    pattern = r"\.ref\(['\"]?(.*?)['\"]?\)"
    matches = re.findall(pattern, model_src)
    return set(matches)
