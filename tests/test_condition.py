import pytest
import datetime

from dynamantic.expressions import Expr
from dynamantic.exceptions import AttributeTypeInvalidError, AttributeInvalidError
from tests.conftest import RangeKeyModel


def test_no_fields_model_condition_fails():
    with pytest.raises(AttributeInvalidError, match=f"Attribute: .*doesnt_exist.* not allowed."):
        Expr(RangeKeyModel).field("doesnt_exist").set("hello")


def test_condition_expression_set_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_str").set("hello")
    assert ce.update_expression == "SET my_str = :my_str"
    assert ce.expression_attribute_values == {":my_str": {"S": "hello"}}


def test_condition_expression_set_bytes_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_simple_bytes").set(b"hello")
    assert ce.update_expression == "SET my_simple_bytes = :my_simple_bytes"
    assert ce.expression_attribute_values == {":my_simple_bytes": {"B": b"hello"}}


def test_condition_expression_set_datetime_syntax_succeeds():
    d = datetime.datetime.utcnow()
    ce = Expr(RangeKeyModel).field("my_datetime").set(d)
    assert ce.update_expression == "SET my_datetime = :my_datetime"
    assert ce.expression_attribute_values == {":my_datetime": {"S": d.isoformat()}}


def test_condition_expression_set_date_syntax_succeeds():
    d = datetime.date.today()
    ce = Expr(RangeKeyModel).field("my_date").set(d)
    assert ce.update_expression == "SET my_date = :my_date"
    assert ce.expression_attribute_values == {":my_date": {"S": str(d)}}


def test_condition_expression_set_time_syntax_succeeds():
    t = datetime.time(hour=12)
    ce = Expr(RangeKeyModel).field("my_time").set(t)
    assert ce.update_expression == "SET my_time = :my_time"
    assert ce.expression_attribute_values == {":my_time": {"S": str(t)}}


def test_condition_expression_set_type_int_fails():
    with pytest.raises(AttributeTypeInvalidError, match="integer input not compatible with .*"):
        Expr(RangeKeyModel).field("my_str").set(1)


def test_condition_expression_set_type_float_fails():
    with pytest.raises(AttributeTypeInvalidError, match="number input not compatible with .*"):
        Expr(RangeKeyModel).field("my_str").set(1.50)


def test_condition_expression_set_type_datetime_fails():
    with pytest.raises(AttributeTypeInvalidError, match="date-time input not compatible with .*"):
        Expr(RangeKeyModel).field("my_datetime").set("hello")


def test_condition_expression_set_type_date_fails():
    with pytest.raises(AttributeTypeInvalidError, match="date input not compatible with .*"):
        Expr(RangeKeyModel).field("my_date").set("hello")


def test_condition_expression_set_type_time_fails():
    with pytest.raises(AttributeTypeInvalidError, match="time input not compatible with .*"):
        Expr(RangeKeyModel).field("my_time").set("hello")


def test_condition_expression_set_not_list():
    with pytest.raises(AttributeTypeInvalidError, match=".* input not compatible with .*"):
        Expr(RangeKeyModel).field("my_datetime").set(["hello"])


def test_condition_expression_set_not_set():
    with pytest.raises(AttributeTypeInvalidError, match=".* input not compatible with .*"):
        Expr(RangeKeyModel).field("my_datetime").set({"hello"})


def test_condition_expression_set_type_unsupported_fails():
    class Test:
        pass

    non_supported = Test()
    with pytest.raises(AttributeTypeInvalidError, match=f".* input not compatible with .*"):
        Expr(RangeKeyModel).field("my_datetime").set(non_supported)


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
    assert ce.expression_attribute_values == {":my_float": {"N": "1.5"}}


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
    assert ce.update_expression == "ADD my_int_set :my_int_set"


def test_condition_expression_set_add_str_list_required_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_required_str_list").set_append(["hello"])
    assert ce.update_expression == "SET my_required_str_list = list_append(my_required_str_list, :my_required_str_list)"
    assert ce.expression_attribute_values == {":my_required_str_list": {"L": [{"S": "hello"}]}}


def test_condition_expression_set_add_str_set_required_syntax_succeeds():
    ce = Expr(RangeKeyModel).field("my_required_str_set").set_append({"hello"})
    assert ce.update_expression == "ADD my_required_str_set :my_required_str_set"
    assert ce.expression_attribute_values == {":my_required_str_set": {"SS": ["hello"]}}


def test_condition_expression_deep():
    ce = Expr(RangeKeyModel).field("my_nested_model").field("sample_field").set("foobaz")
    assert ce.update_expression == "SET my_nested_model.sample_field = :sample_field"
    assert ce.expression_attribute_values == {":sample_field": {"S": "foobaz"}}


def test_condition_expression_deep_dict():
    ce = Expr(RangeKeyModel).field("my_required_dict").field("new_field").set("foobaz")
    assert ce.update_expression == "SET my_required_dict.new_field = :new_field"
    assert ce.expression_attribute_values == {":new_field": {"S": "foobaz"}}


def test_condition_expression_set_dict():
    ce = Expr(RangeKeyModel).field("my_required_dict").set({"hello": "world"})
    assert ce.update_expression == "SET my_required_dict = :my_required_dict"
    assert ce.expression_attribute_values == {":my_required_dict": {"M": {"hello": {"S": "world"}}}}
