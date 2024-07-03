from django.apps import AppConfig

DEFAULT_CONFIG = {
    "gql_create_deduplication_review_perms": ["172001"],
    "gql_create_deduplication_payment_review_perms": ["172002"],
}


class DeduplicationConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'deduplication'

    gql_create_deduplication_review_perms = None
    gql_create_deduplication_payment_review_perms = None

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
