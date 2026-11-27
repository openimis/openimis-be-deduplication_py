"""
OP-3353 / CVE-2026-82492: deduplication column paths may only follow relations the caller
is allowed to read.

`_resolve_columns` used to validate only the first segment of a column
(``column.split('__', 1)[0]``), so ``individual__user_created__i_user__password`` passed
and the grouped password hashes came back to the client in ``columnValues``.

Behaviour required:

* a path matching `core.mutation_log_secrets.is_secret_key`, or that is not a forward
  field path, is refused outright;
* a column crossing a relation to a model the user cannot read still takes part in the
  grouping (finding duplicates matters more than hiding the column from it), but its
  values are masked in ``columnValues``;
* ``first_name`` and a json_ext key work as before.
"""

import json
from copy import deepcopy

from core.models.openimis_graphql_test_case import (
    BaseTestContext,
    openIMISGraphQLTestCase,
)
from core.mutation_log_secrets import MASK
from core.test_helpers import create_right_only_user
from deduplication.services import (
    get_beneficiary_duplication_aggregation,
    get_benefit_consumption_duplication_aggregation,
)
from deduplication.tests.data.dedup_beneficiary import benefit_plan_data, individuals_data
from deduplication.tests.helpers import LogInHelper
from individual.models import Individual
from social_protection.models import Beneficiary, BeneficiaryStatus, BenefitPlan

SUMMARY_QUERY = """
    query ($columns: [String]!, $bp: UUID!) {
      beneficiaryDeduplicationSummary(columns: $columns, benefitPlanId: $bp) {
        rows { count columnValues }
      }
    }
"""


class DeduplicationColumnRightsTest(openIMISGraphQLTestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.creator = LogInHelper().get_or_create_user_api()
        plan_data = deepcopy(benefit_plan_data)
        plan_data["code"] = "OP3353DD"
        cls.bp = BenefitPlan(**plan_data)
        cls.bp.save(username=cls.creator.username)
        for data in individuals_data:
            individual = Individual(**deepcopy(data))
            individual.save(username=cls.creator.username)
            Beneficiary(
                individual=individual,
                benefit_plan=cls.bp,
                status=BeneficiaryStatus.ACTIVE,
                json_ext=individual.json_ext,
            ).save(username=cls.creator.username)

        cls.beneficiary_only = create_right_only_user(
            "op3353DedupBene", ["gql_beneficiary_search_perms"]
        )
        cls.beneficiary_only_token = BaseTestContext(user=cls.beneficiary_only).get_jwt()
        cls.with_individual = create_right_only_user(
            "op3353DedupInd",
            ["gql_beneficiary_search_perms", "gql_individual_search_perms"],
        )
        cls.with_individual_token = BaseTestContext(user=cls.with_individual).get_jwt()

    def _summary(self, columns, token):
        response = self.query(
            SUMMARY_QUERY,
            variables={"columns": columns, "bp": str(self.bp.id)},
            headers={"HTTP_AUTHORIZATION": f"Bearer {token}"},
        )
        return json.loads(response.content)

    def _rows(self, columns, token):
        content = self._summary(columns, token)
        self.assertNotIn("errors", content, content)
        rows = content["data"]["beneficiaryDeduplicationSummary"]["rows"]
        return [
            {"count": row["count"], "values": json.loads(row["columnValues"])}
            for row in rows
        ]

    # --- refused outright -----------------------------------------------------
    def test_secret_path_is_refused_by_the_service(self):
        with self.assertRaises(ValueError):
            list(get_beneficiary_duplication_aggregation(
                ["individual__user_created__i_user__password"], self.bp.id
            ))

    def test_secret_path_is_refused_by_the_summary(self):
        password_hash = self.creator.i_user.password
        content = self._summary(
            ["individual__user_created__i_user__password"], self.with_individual_token
        )
        self.assertIn("errors", content)
        self.assertNotIn(password_hash, json.dumps(content))

    def test_secret_path_is_refused_by_the_benefit_service(self):
        """The payment-cycle summary reads through BenefitConsumption: same refusal."""
        with self.assertRaises(ValueError):
            list(get_benefit_consumption_duplication_aggregation(
                ["user_created__i_user__password"], "00000000-0000-0000-0000-000000000000"
            ))

    def test_reverse_relation_path_is_refused_by_the_service(self):
        with self.assertRaises(ValueError):
            list(get_beneficiary_duplication_aggregation(
                ["individual__groupindividuals__group__code"], self.bp.id
            ))

    # --- grouped, but masked ---------------------------------------------------
    def test_unreadable_column_still_groups_but_its_value_is_masked(self):
        rows = self._rows(["first_name", "k1"], self.beneficiary_only_token)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["count"], 2)
        self.assertEqual(rows[0]["values"]["individual__first_name"], MASK)
        # json_ext of the beneficiary itself is the root row's own column.
        self.assertEqual(rows[0]["values"]["k1"], "k1 v1")

    def test_column_to_a_model_without_any_declared_right_is_masked(self):
        rows = self._rows(
            ["first_name", "individual__user_created__username"],
            self.with_individual_token,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["count"], 2)
        self.assertEqual(rows[0]["values"]["individual__user_created__username"], MASK)
        self.assertEqual(rows[0]["values"]["individual__first_name"], "first name 1")

    # --- unchanged ------------------------------------------------------------
    def test_first_name_and_json_key_work_as_before(self):
        rows = self._rows(["first_name", "k1"], self.with_individual_token)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["count"], 2)
        self.assertEqual(
            rows[0]["values"],
            {"individual__first_name": "first name 1", "k1": "k1 v1"},
        )
