import pytest

from dynamantic.expressions import Expr
from dynamantic.exceptions import AttributeTypeInvalidError
from tests.conftest import RangeKeyModel


def test_condition_expression_set_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_str").set("hello")
    assert ce.update_expression == "SET my_str = :my_str"
    assert ce.expression_attribute_values == {":my_str": {"S": "hello"}}


def test_condition_expression_set_type_fails():
    with pytest.raises(AttributeTypeInvalidError, match=".* input not compatible with .*"):
        Expr(RangeKeyModel).field("my_str").set(1)


def test_condition_expression_set_add_pos_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_int").set_add(1)
    assert ce.update_expression == "SET my_int = my_int + :my_int"
    assert ce.expression_attribute_values == {":my_int": {"N": "1"}}


def test_condition_expression_set_subtract_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_int").set_add(-1)
    assert ce.update_expression == "SET my_int = my_int + :my_int"
    assert ce.expression_attribute_values == {":my_int": {"N": "-1"}}


def test_condition_expression_set_add_float_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_float").set_add(1.5)
    assert ce.update_expression == "SET my_float = my_float + :my_float"
    assert ce.expression_attribute_values == {":my_float": {"N": "1.5000000000"}}


def test_condition_expression_set_add_int_list_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_int_list").set_append([7])
    assert ce.update_expression == "SET my_int_list = list_append(my_int_list, :my_int_list)"
    assert ce.expression_attribute_values == {":my_int_list": {"L": [{"N": "7"}]}}


def test_condition_expression_set_add_int_list_multiple_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_int_list").set_append([7, 9, 10])
    assert ce.update_expression == "SET my_int_list = list_append(my_int_list, :my_int_list)"
    assert ce.expression_attribute_values == {":my_int_list": {"L": [{"N": "7"}, {"N": "9"}, {"N": "10"}]}}


def test_condition_expression_set_add_int_set_multiple_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_int_set").set_append({7, 9, 10})
    assert ce.update_expression == "SET my_int_set = list_append(my_int_set, :my_int_set)"
