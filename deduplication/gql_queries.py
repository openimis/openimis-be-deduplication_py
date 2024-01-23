import graphene


class DeduplicationSummaryRowGQLType(graphene.ObjectType):
    count = graphene.Int()
    ids = graphene.List(graphene.UUID)
    column_values = graphene.JSONString()


class DeduplicationSummaryGQLType(graphene.ObjectType):
    rows = graphene.List(DeduplicationSummaryRowGQLType)
