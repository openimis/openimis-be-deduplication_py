import copy
import logging
from functools import lru_cache
from typing import List, Union, Tuple, Type
from datetime import datetime

from django.contrib.postgres.aggregates import ArrayAgg
from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.db.models import Count, Q
from django.db.models.fields.json import KeyTextTransform
from django.db.models.functions import Cast
from django.db import transaction

from core.datetimes.ad_datetime import AdDate
from core.models import ExtendableModel, HistoryModel, User, HistoryBusinessModel
from core.services.utils import model_representation
from individual.models import Individual
from social_protection.models import Beneficiary, BenefitPlan
from tasks_management.apps import TasksManagementConfig
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
                'executor_action_event': TasksManagementConfig.default_executor_event,
                'business_data_serializer': (
                    'deduplication.services.CreateDeduplicationReviewTasksService'
                    '.create_beneficiary_duplication_task_serializer'
                ),
                'business_event': '',
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
            excluded_fields.add('benefit_plan')
            headers = [field for field in individual_fields + beneficiary_fields + beneficiary_data_schema_fields if
                       field not in excluded_fields]

            return headers

        def serializer(key, value):
            excluded_fields = self._get_excluded_model_fields(exclude_json_ext=False)
            excluded_fields.discard("date_created")
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


def _update_instance_if_different_value(instance, instance_kwargs, user):
    is_instance_updated = False
    if instance_kwargs:
        for field, field_value in instance_kwargs.items():
            if field == 'dob':
                date_object = datetime.strptime(field_value, "%Y-%m-%d").date()
                field_value = AdDate(date_object.year, date_object.month, date_object.day)
            if getattr(instance, field) != field_value:
                is_instance_updated = True
                setattr(instance, field, field_value)
        if is_instance_updated:
            instance.save(username=user.username)


def _update_instance_json_ext_if_different_value(instance, json_ext_kwargs, user):
    is_instance_updated = False
    if json_ext_kwargs:
        json_ext = instance.json_ext or {}
        for key, value in json_ext_kwargs.items():
            if json_ext.get(key) != value:
                is_instance_updated = True
                json_ext[key] = value
        instance.json_ext = json_ext
        if is_instance_updated:
            instance.save(username=user.username)


@transaction.atomic
def merge_duplicate_beneficiaries(task_data, user_id):
    individual_fields = {"first_name", "last_name", "dob"}
    beneficiary_fields = {"status"}

    user = User.objects.get(id=user_id)
    # additional_resolve_data is a key in task__json_ext that stores data selected by user during resolving a task
    additional_resolve_data = task_data.get("json_ext", {}).get("additional_resolve_data", {})
    merge_data = list(additional_resolve_data.values())[0]
    field_values = merge_data.get('values', {})
    beneficiary_ids = merge_data.get("beneficiaryIds", [])

    oldest_record_beneficiary = Beneficiary.objects.filter(id__in=beneficiary_ids).order_by('date_created').first()
    if oldest_record_beneficiary is None:
        return  # No beneficiaries found to merge

    beneficiaries_to_delete = Beneficiary.objects.filter(id__in=beneficiary_ids).exclude(
        id=oldest_record_beneficiary.id)

    individual_kwargs = {}
    beneficiary_kwargs = {}
    json_kwargs = {}

    for key, value in field_values.items():
        if key in individual_fields:
            individual_kwargs[key] = value
        elif key in beneficiary_fields:
            beneficiary_kwargs[key] = value
        else:
            json_kwargs[key] = value

    _update_instance_if_different_value(oldest_record_beneficiary.individual, individual_kwargs, user)
    _update_instance_if_different_value(oldest_record_beneficiary, beneficiary_kwargs, user)
    _update_instance_json_ext_if_different_value(oldest_record_beneficiary, json_kwargs, user)

    # needs to be done this way because overridden .delete() does not work on qs
    for beneficiary_to_delete in beneficiaries_to_delete:
        beneficiary_to_delete.delete(username=user.username)


def on_deduplication_task_complete_service_handler():
    def func(**kwargs):
        try:
            result = kwargs.get('result', {})
            task = result['data']['task']
            user_id = result['data']['user']["id"]
            if result and result['success'] and task['status'] == Task.Status.COMPLETED:
                merge_duplicate_beneficiaries(task, user_id)
        except Exception as e:
            logger.error("Error while executing on_task_complete", exc_info=e)
            return [str(e)]

    return func
