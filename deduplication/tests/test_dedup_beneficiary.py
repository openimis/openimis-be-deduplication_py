from decimal import Decimal

from django.test import TestCase
from django.db import transaction
from django.db.models import Q

from deduplication.services import get_beneficiary_duplication_aggregation
from deduplication.tests.data.dedup_beneficiary import benefit_plan_data, individuals_data
from deduplication.tests.helpers import LogInHelper
from individual.models import Individual
from social_protection.models import Beneficiary, BenefitPlan, BeneficiaryStatus


class DedupBeneficiaryTestCase(TestCase):
    user = None
    bp = None
    inds = None
    benfs = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.user = LogInHelper().get_or_create_user_api()
        cls.bp = BenefitPlan(**benefit_plan_data)
        cls.bp.save(username=cls.user.username)
        cls.inds = list()
        cls.bens = list()

        for i_data in individuals_data:
            i = Individual(**i_data)
            i.save(username=cls.user.username)
            cls.inds.append(i)

        for i in cls.inds:
            b = Beneficiary(
                individual=i,
                benefit_plan=cls.bp,
                status=BeneficiaryStatus.ACTIVE,
                json_ext=i.json_ext
            )
            b.save(username=cls.user.username)
            cls.bens.append(b)

    def test_deduplication_aggregation(self):
        res = get_beneficiary_duplication_aggregation(['individual__first_name', 'k1'], self.bp.id)
        self.assertEquals(list(res), [{
            'id_count': 2,
            'ids': ['87ae3df2-b7de-42a3-a925-08591aa3672c', '9a9d7199-ffbd-47ec-b7ca-6848c9874bff'],
            'individual__first_name': 'first name 1',
            'k1': 'k1 v1'
        }])
