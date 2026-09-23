from django.apps import AppConfig

from core.rights_declaration import RightsDeclaration

MODULE_NAME = "deduplication"

# Droits, par entite puis par action.
#
# Deux entites et non une seule a deux actions : les deux droits ne portent pas sur le
# meme objet metier. 172001 cree des taches de revue de doublons de *beneficiaires*
# (social_protection.Beneficiary), 172002 des taches de revue de doublons de
# *paiements* (payroll.BenefitConsumption) ; mutations, services et validations sont
# distincts de bout en bout. Chacune n'a donc qu'une action, `create`.
#
# Les noms django restent purement declaratifs ici : ce module n'a aucun modele
# (models.py est vide), la revue est materialisee par un tasks_management.Task. Le
# prefixe reste `deduplication` parce que c'est ce module qui declare et applique le
# droit, meme si aucune permission django ne sera creee au post_migrate.
DJANGO_PERMS = {
    "deduplicationReview": {
        "create": ("deduplication.add_deduplicationreview", 172001),
    },
    # Droit dormant : declare et expose dans l'ecran des roles, mais aucun controle ne
    # le lit. `CreateDeduplicationPaymentReviewMutation._validate`
    # (gql_mutations.py) verifie 172001. L'ecart est signale, pas corrige : le corriger
    # retirerait l'acces aux roles qui n'ont que 172001.
    "paymentDeduplicationReview": {
        "create": ("deduplication.add_paymentdeduplicationreview", 172002),
    },
}

_PERM_CFG = {
    "gql_create_deduplication_review_perms": ("deduplicationReview", "create"),
    "gql_create_deduplication_payment_review_perms": ("paymentDeduplicationReview", "create"),
}

RIGHTS = RightsDeclaration(MODULE_NAME, DJANGO_PERMS, _PERM_CFG)

perms = RIGHTS.perms
django_perms = RIGHTS.django_perm_names
configured_perms = RIGHTS.configured
require = RIGHTS.require


DEFAULT_CONFIG = {
}


class DeduplicationConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = MODULE_NAME

    # Droits: constantes, plus surchargeables. Ils ne passent plus par le
    # DEFAULT_CFG ni par ready(): `ModuleConfiguration.get_or_default` ignore
    # desormais toute cle `_perms` stockee en base.
    gql_create_deduplication_review_perms = RIGHTS.perms("deduplicationReview", "create")
    gql_create_deduplication_payment_review_perms = RIGHTS.perms("paymentDeduplicationReview", "create")

    def ready(self):
        from core.models import ModuleConfiguration

        cfg = ModuleConfiguration.get_or_default(self.name, DEFAULT_CONFIG)
        self.__load_config(cfg)

    @classmethod
    def __load_config(cls, cfg):
        """
        Load all config fields that match current AppConfig class fields, all custom fields have to be loaded separately
        """
        for field in cfg:
            if hasattr(DeduplicationConfig, field):
                setattr(DeduplicationConfig, field, cfg[field])
