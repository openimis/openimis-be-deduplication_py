from core.service_signals import ServiceSignalBindType
from core.signals import bind_service_signal
from deduplication.services import on_deduplication_task_complete_service_handler, CreateDeduplicationReviewTasksService


def bind_service_signals():
    bind_service_signal(
        'task_service.complete_task',
        on_deduplication_task_complete_service_handler(CreateDeduplicationReviewTasksService),
        bind_type=ServiceSignalBindType.AFTER
    )
