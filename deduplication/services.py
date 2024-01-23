import logging
from functools import lru_cache
from typing import List, Union, Tuple, Type

from django.contrib.postgres.aggregates import ArrayAgg
from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.db.models import Count, Q
from django.db.models.fields.json import KeyTextTransform
from django.db.models.functions import Cast

from core.models import ExtendableModel, HistoryModel, User
from tasks_management.models import Task
from tasks_management.services import TaskService

logger = logging.getLogger(__name__)


class CreateDeduplicationReviewTasksService:
    def __init__(self, user):
        self.user = user

    def create_beneficiary_duplication_tasks(self, summary):
        task_service = TaskService(self.user)
        tasks = []
        for task_data in summary:
            task_data = {
                'source': "CreateDeduplicationReviewTasksService",
                'business_data_serializer': 'deduplication.CreateDeduplicationReviewTasksService'
                                            '.create_beneficiary_duplication_task_serializer',
                'business_event': '',  # to be filled
                'data': task_data,
            }
            tasks.append(task_service.create(task_data))
        return {
            "success": True,
            "message": "Ok",
            "detail": "",
            "data": tasks,
        }

    def create_beneficiary_duplication_task_serializer(self, key, value):
        # think of what data should be displayed when creating frontend
        return value


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

    queryset = queryset.values(*values).annotate(id_count=Count('id'), ids=ArrayAgg('id', distinct=True)).filter(
        id_count__gt=1).order_by()

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


def on_deduplication_task_complete_service_handler(service):
    def func(**kwargs):
        try:
            result = kwargs.get('result', {})
            task = result['data']['task']
            business_event = task['business_event']
            if result and result['success'] \
                    and task['status'] == Task.Status.COMPLETED:
                operation = business_event.split(".")[1]
                if hasattr(service, operation):
                    user = User.objects.get(id=result['data']['user']['id'])
                    data = task['data']['incoming_data']
                    getattr(service(user), operation)(data)
        except Exception as e:
            logger.error("Error while executing on_task_complete", exc_info=e)
            return [str(e)]

    return func
