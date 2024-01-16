from typing import List, Union, Tuple, Type

from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.db.models import Count, Q
from django.db.models.fields.json import KeyTextTransform
from django.db.models.functions import Cast

from core.models import ExtendableModel, HistoryModel


def get_beneficiary_duplication_aggregation(columns: List[str] = None, benefit_plan_id: str = None):
    from social_protection.models import Beneficiary

    if not columns:
        raise ValueError("At least one column required")
    if not benefit_plan_id:
        raise ValueError("Benefit Plan not specified")

    db_columns, json_keys = _resolve_columns(Beneficiary, columns)
    filters = [Q(benefit_plan_id=benefit_plan_id)]

    return get_duplication_aggregation(Beneficiary, columns=db_columns, json_ext_keys=json_keys, filters=filters)


def get_duplication_aggregation(model: Union[Type[ExtendableModel], Type[HistoryModel]], columns: List[str] = None,
                                json_ext_keys: List[str] = None, filters: List = None):
    queryset = model.objects.filter(*filters)

    if json_ext_keys:
        json_ext_aggr = {key: Cast(KeyTextTransform(key, 'json_ext'), models.TextField())
                         for key in json_ext_keys}
        queryset = queryset.annotate(**json_ext_aggr)

    values = (columns or list()) + (json_ext_keys or list())

    if not values:
        raise ValueError("At least one column required")

    queryset = queryset.values(*values).annotate(id_count=Count('id')).filter(id_count__gt=1).order_by()

    return queryset


def _resolve_columns(model: Union[Type[ExtendableModel], Type[HistoryModel]], columns: List[str]) -> Tuple[
    List[str], List[str]]:
    fields = []
    json_fields = []

    for column in columns:
        if _is_model_column(model, column.split('__', 1)[0]):
            fields.append(column)
        else:
            json_fields.append(column)

    return fields, json_fields


def _is_model_column(model: Union[Type[ExtendableModel], Type[HistoryModel]], column: str) -> bool:
    try:
        model._meta.get_field(column)
        return True
    except FieldDoesNotExist:
        return False
