import json

import graphene
from django.contrib.auth.models import AnonymousUser

from deduplication.gql_queries import DeduplicationSummaryGQLType, DeduplicationSummaryRowGQLType


class Query(graphene.ObjectType):
    module_name = "tasks_management"

    beneficiary_deduplication_summary = graphene.Field(
        DeduplicationSummaryGQLType,
        columns=graphene.List(graphene.String, required=True),
        benefit_plan_id=graphene.UUID(required=True),
    )

    def resolve_beneficiary_deduplication_summary(self, info, columns=None, benefit_plan_id=None, **kwargs):
        from social_protection.apps import SocialProtectionConfig
        from deduplication.services import get_beneficiary_duplication_aggregation

        Query._check_permissions(info.context.user, SocialProtectionConfig.gql_beneficiary_search_perms)

        if not columns:
            return ["deduplication.validation.no_columns_provided"]

        individual_columns = ['first_name', 'last_name', 'dob']
        columns = [f'individual__{column}' if column in individual_columns else column for column in columns]

        aggr = get_beneficiary_duplication_aggregation(columns=columns, benefit_plan_id=benefit_plan_id)

        rows = list()
        for row in aggr:
            individual_columns = [f'individual__{column}' for column in individual_columns]
            count = row.pop('id_count')
            row_column_values = {column.split('__', 1)[1] if column in individual_columns else column: str(row[column]) for
                                 column in row}
            rows.append(DeduplicationSummaryRowGQLType(column_values=row_column_values, count=count))

        return DeduplicationSummaryGQLType(rows=rows)

    @staticmethod
    def _check_permissions(user, perms):
        if type(user) is AnonymousUser or not user.id or not user.has_perms(perms):
            raise PermissionError("Unauthorized")
