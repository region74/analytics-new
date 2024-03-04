from logging import getLogger

from apps.choices import RoistatDimensionType
from apps.traffic.models import Channel
from apps.sources.models import RoistatDimension
from apps.sources.management.commands._base import BaseCommand


logger = getLogger(__name__)


class Command(BaseCommand):
    help = "Обновление списка каналов трафика"

    def handle(self, **kwargs):
        logger.info("Update traffic channels")

        channels = list(Channel.objects.values_list("key", flat=True))
        dimensions = dict(
            RoistatDimension.objects.filter(
                type=RoistatDimensionType.marker_level_1.name
            )
            .exclude(name__in=channels)
            .values_list("name", "title")
        )
        if dimensions:
            Channel.objects.bulk_create(
                list(
                    Channel(key=key, value=value)
                    for key, value in dimensions.items()
                ),
                batch_size=1000,
            )
