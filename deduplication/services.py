import copy
import logging
from functools import lru_cache
from typing import List, Union, Tuple, Type

from django.contrib.postgres.aggregates import ArrayAgg
from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.db.models import Count, Q
from django.db.models.fields.json import KeyTextTransform
from django.db.models.functions import Cast

from core.models import ExtendableModel, HistoryModel, User, HistoryBusinessModel
from core.services.utils import model_representation
from individual.models import Individual
from social_protection.models import Beneficiary, BenefitPlan
from tasks_management.models import Task
from tasks_management.services import TaskService, non_serializable_types

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
                'business_data_serializer': (
                    'deduplication.services.CreateDeduplicationReviewTasksService'
                    '.create_beneficiary_duplication_task_serializer'
                ),
                'business_event': '',  # TODO to be filled in CM-449
                'data': task_data,
            }
            tasks.append(task_service.create(task_data))

        return {
            "success": True,
            "message": "Ok",
            "detail": "",
            "data": tasks,
        }

    def create_beneficiary_duplication_task_serializer(self, data):
        def serialize_individual(value):
            excluded_fields = self._get_excluded_model_fields()
            individual = Individual.objects.filter(id=value).first()
            individual_dict = model_representation(individual)

            exclude_fields_from_dict(individual_dict, excluded_fields)

            for k, v in individual_dict.items():
                individual_dict[k] = serialize_value(v)

            return individual_dict

        def serialize_benefit_plan(value):
            benefit_plan = BenefitPlan.objects.filter(id=value).first()
            return benefit_plan.__str__()

        def serialize_value(value):
            if any(isinstance(value, t) for t in non_serializable_types):
                return str(value)
            return value

        def exclude_fields_from_dict(dictionary, fields_to_exclude):
            for field in fields_to_exclude:
                dictionary.pop(field, None)

        def beneficiary_serializer(data):
            for key, value in data.items():
                if key == 'individual':
                    data[key] = serialize_individual(value)
                elif key == 'benefit_plan':
                    data[key] = serialize_benefit_plan(value)
                else:
                    data[key] = serialize_value(value)
            return data

        def get_headers(benefit_plan):
            individual_fields = [field.name for field in Individual._meta.fields]
            beneficiary_fields = [field.name for field in Beneficiary._meta.fields]
            beneficiary_data_schema_fields = list(benefit_plan.beneficiary_data_schema.get('properties', {}).keys())

            excluded_fields = self._get_excluded_model_fields()
            headers = [field for field in individual_fields + beneficiary_fields + beneficiary_data_schema_fields if
                       field not in excluded_fields]

            return headers



        def serializer(key, value):
            excluded_fields = self._get_excluded_model_fields(exclude_json_ext=False)
            if key == 'ids':
                beneficiary_list = []
                beneficiaries = Beneficiary.objects.filter(id__in=value)

                for beneficiary in beneficiaries:
                    beneficiary_dict = model_representation(beneficiary)
                    exclude_fields_from_dict(beneficiary_dict, excluded_fields)

                    beneficiary_dict = beneficiary_serializer(beneficiary_dict)
                    beneficiary_list.append(beneficiary_dict)

                return beneficiary_list
            else:
                return serialize_value(value)

        serialized_data = copy.deepcopy(data)
        beneficiary_id = serialized_data['ids'][0]
        benefit_plan = BenefitPlan.objects.filter(beneficiary__id=beneficiary_id).first()
        for key, value in data.items():
            serialized_data[key] = serializer(key, value)

        serialized_data['headers'] = get_headers(benefit_plan)

        return serialized_data

    @lru_cache(maxsize=None)
    def _get_excluded_model_fields(self, exclude_json_ext=True):
        fields_to_exclude = set(field.name for field in HistoryBusinessModel._meta.fields)
        if not exclude_json_ext:
            fields_to_exclude.discard('json_ext')

        return fields_to_exclude



def get_beneficiary_duplication_aggregation(columns: List[str] = None, benefit_plan_id: str = None):
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
