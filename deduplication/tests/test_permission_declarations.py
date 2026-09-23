"""
Garde-fous sur la declaration des droits de deduplication.

Meme structure que `claim` : `DJANGO_PERMS` par entite puis par action, `_PERM_CFG` qui
en derive les cles de config. Ce module n'a aucun modele (models.py est vide), donc pas
de `get_rights` a epingler : les objets dedupliques appartiennent a social_protection et
payroll, et la revue elle-meme est un tasks_management.Task.

Ce qui est verrouille ici :
  * un identifiant a un seul endroit (DJANGO_PERMS), donc pas de derive entre la
    declaration et le controle ;
  * une cle de config sans attribut de classe n'est jamais chargee par `__load_config`
    et sa lecture leve AttributeError - le droit devient inapplicable ;
  * `has_perms([])` renvoie True, donc une liste vide accorde a tous.
"""

import json
import os

from django.test import TestCase

from deduplication.apps import (
    DJANGO_PERMS,
    DeduplicationConfig,
    _PERM_CFG,
    configured_perms,
    django_perms,
    perms,
)

# The identifiers as deployed. Changing one is incompatible with the existing roles:
# this test has to be updated *and* the new right granted.
EXPECTED_RIGHTS = {
    "gql_create_deduplication_review_perms": ["172001"],
    "gql_create_deduplication_payment_review_perms": ["172002"],
}

# Nom historique dans permissions_map.json -> identifiant. Le nom django declare dans
# DJANGO_PERMS est neuf ; la carte, elle, porte encore les noms d'origine. C'est
# l'entier qui doit correspondre.
EXPECTED_MAP_ENTRIES = {
    "deduplication.create_deduplication_review": "172001",
    "deduplication.create_deduplication_payment_review": "172002",
}


class DeduplicationPermissionDeclarationTestCase(TestCase):
    def test_right_ids_unchanged(self):
        self.assertEqual(
            {key: getattr(DeduplicationConfig, key) for key in EXPECTED_RIGHTS},
            EXPECTED_RIGHTS,
        )

    def test_perm_cfg_covers_every_declared_action(self):
        declared = {
            (entity, action)
            for entity, actions in DJANGO_PERMS.items()
            for action in actions
        }
        self.assertEqual(set(_PERM_CFG.values()), declared)

    def test_perm_cfg_matches_config_attributes(self):
        """`__load_config` ignores the keys with no class attribute."""
        missing = [key for key in _PERM_CFG if not hasattr(DeduplicationConfig, key)]
        self.assertEqual(missing, [])

    def test_no_right_list_is_empty(self):
        empty = [key for key in _PERM_CFG if not getattr(DeduplicationConfig, key)]
        self.assertEqual(empty, [])

    def test_attributes_carry_the_declared_right(self):
        """
        The rights are constants set from DJANGO_PERMS: the attribute must equal the
        declaration, without going through the config.
        """
        for key, (entity, action) in _PERM_CFG.items():
            with self.subTest(key=key):
                self.assertEqual(getattr(DeduplicationConfig, key), perms(entity, action))

    def test_no_right_id_is_shared(self):
        """Les deux revues sont deux objets distincts : aucun partage d'identifiant."""
        seen = {}
        for entity, actions in DJANGO_PERMS.items():
            for action, (_, right_id) in actions.items():
                seen.setdefault(right_id, []).append((entity, action))
        shared = {rid: who for rid, who in seen.items() if len(who) > 1}
        self.assertEqual(shared, {})

    def test_django_permission_names_are_unique(self):
        seen = {}
        for entity, actions in DJANGO_PERMS.items():
            for action, (name, _) in actions.items():
                seen.setdefault(name, []).append(f"{entity}.{action}")
        shared = {name: who for name, who in seen.items() if len(who) > 1}
        self.assertEqual(shared, {})

    def test_django_permission_names_are_prefixed_by_the_app_label(self):
        for entity, actions in DJANGO_PERMS.items():
            for action, (name, _) in actions.items():
                with self.subTest(entity=entity, action=action):
                    self.assertTrue(name.startswith("deduplication."), name)

    def test_unknown_entity_or_action_raises(self):
        with self.assertRaises(KeyError):
            perms("nosuchentity", "create")
        with self.assertRaises(KeyError):
            perms("deduplicationReview", "nosuchaction")
        with self.assertRaises(KeyError):
            django_perms("deduplicationReview", "nosuchaction")

    def test_configured_reads_the_configured_value_not_the_declared_default(self):
        """
        ModuleConfiguration may override a right; a check must read the configured
        value, where `perms()` returns the declared default.
        """
        original = DeduplicationConfig.gql_create_deduplication_review_perms
        try:
            DeduplicationConfig.gql_create_deduplication_review_perms = ["999999"]
            self.assertEqual(
                configured_perms("deduplicationReview", "create"), ["999999"]
            )
            self.assertEqual(perms("deduplicationReview", "create"), ["172001"])
        finally:
            DeduplicationConfig.gql_create_deduplication_review_perms = original

    def test_configured_returns_none_for_an_undeclared_action(self):
        """None means "no rule": the caller must fail closed."""
        self.assertIsNone(configured_perms("deduplicationReview", "nosuchaction"))

    def test_right_ids_match_the_permissions_map(self):
        """L'entier est ce que portent les roles ; la carte doit dire la meme chose."""
        from django.conf import settings

        candidates = [
            os.path.join(str(settings.BASE_DIR), "permissions_map.json"),
            os.path.join(os.path.dirname(str(settings.BASE_DIR)), "permissions_map.json"),
        ]
        path = next((p for p in candidates if os.path.exists(p)), None)
        if path is None:
            self.skipTest("permissions_map.json introuvable depuis cet assemblage")
        with open(path) as handle:
            mapping = json.load(handle)
        declared = {
            str(right_id)
            for actions in DJANGO_PERMS.values()
            for _, right_id in actions.values()
        }
        self.assertEqual(
            {name: mapping.get(name) for name in EXPECTED_MAP_ENTRIES},
            EXPECTED_MAP_ENTRIES,
        )
        self.assertEqual(declared, set(EXPECTED_MAP_ENTRIES.values()))
