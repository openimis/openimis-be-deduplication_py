from decimal import Decimal

from social_protection.models import BenefitPlan

benefit_plan_data = {
    'code': 'code',
    'name': 'Name',
    'max_beneficiaries': 20,
    'ceiling_per_beneficiary': Decimal("10000.00"),
    'type': BenefitPlan.BenefitPlanType.INDIVIDUAL_TYPE,
    'description': 'test',
    'beneficiary_data_schema': {}
}

individuals_data = [
    {
        'first_name': 'first name 1',
        'last_name': 'last name 1',
        'dob': '1970-01-01',
        'json_ext': {
            'k1': 'k1 v1',
            'k2': 'k2 v1',
        },
    },
    {
        'first_name': 'first name 1',
        'last_name': 'last name 1',
        'dob': '1970-01-01',
        'json_ext': {
            'k1': 'k1 v1',
            'k2': 'k2 v1',
        },
    },
    {
        'first_name': 'first name 2',
        'last_name': 'last name 2',
        'dob': '1970-01-01',
        'json_ext': {
            'k1': 'k1 v2',
            'k2': 'k2 v2',
        },
    }
]
