import pytest

from framelink._util import parse_model_src_for_internal_refs


@pytest.mark.parametrize(
    "in_str, model_names",
    [
        (".ref(%s)", ("my_model",)),
        (".ref('%s')", ("my_model",)),
        ('.ref("%s")', ("my_model",)),
        ('ref.ref("%s")', ("my_model",)),
        ('.ref("%s").ref(%s)', ("my_model", "model_two")),
        ('.ref("%s").ref(%s)', ("my_ref_model", "ref_model")),
    ],
)
def test_model_body_ref_parsing(in_str, model_names):
    in_str = in_str % model_names  # format the string with the given model name
    res = parse_model_src_for_internal_refs(in_str)
    assert len(res) == len(model_names)
    assert all(model_name in res for model_name in model_names)
