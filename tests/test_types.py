from pydantic import BaseModel

from dynamantic.expressions import Expr2
from tests.conftest import RangeKeyModel


def test_type():
    expr = (
        Expr2(RangeKeyModel)
        .field("my_nested_model_required")
        .field("deep_nested")
        .index(0)
        .field("another_field_float_list")
        .index(0)
        .set(1.1)
    )
    print(expr.update_expression)
    print(expr.expression_attribute_values)
    assert False
