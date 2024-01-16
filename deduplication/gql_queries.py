import graphene


class DeduplicationSummaryRowGQLType(graphene.ObjectType):
    count = graphene.Int()
    column_values = graphene.JSONString()


class DeduplicationSummaryGQLType(graphene.ObjectType):
    rows = graphene.List(DeduplicationSummaryRowGQLType)
