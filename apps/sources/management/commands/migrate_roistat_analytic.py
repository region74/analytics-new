import pytz
import datetime

from time import sleep
from typing import Dict, Any, List
from logging import getLogger

from django.db import transaction
from django.conf import settings

from apps.sources.models import RoistatAnalytic, RoistatDimension

from plugins.roistat.api import RoistatAPIClient

from ._base import BaseCommand


logger = getLogger(__name__)

ANALYTIC_TZ = pytz.timezone(settings.ANALYTIC_TIME_ZONE)


class Command(BaseCommand):
    help = "Сбор аналитики из Roistat"

    dimensions: Dict[str, Dict[str, List[RoistatDimension]]]

    def __init__(self, *args, **kwargs):
        self.update_dimensions()
        super().__init__(*args, **kwargs)

    def update_dimensions(self):
        self.dimensions = {}
        for item in RoistatDimension.objects.all():
            if item.name not in self.dimensions.keys():
                self.dimensions[item.name] = {}
            if item.type not in self.dimensions[item.name].keys():
                self.dimensions[item.name][item.type] = []
            self.dimensions[item.name][item.type].append(item)

    def parse_date(self, value: str) -> datetime.date:
        return datetime.date.fromisoformat(value)

    def add_arguments(self, parser):
        """
        Аргументы для команды django
        """
        parser.add_argument(
            "-df", "--date-from", required=True, type=self.parse_date
        )
        parser.add_argument(
            "-dt", "--date-to", required=False, type=self.parse_date
        )

    def get_api_data(self, date: datetime.date) -> List[Dict[str, Any]]:
        """
        Получение данных из api
        """
        logger.info("  ↳ Request API: %(date)s" % {"date": date})
        roistat_api = RoistatAPIClient()
        sleep(1)
        response = roistat_api.analytic.post(
            dimensions=[
                "landing_page",
                "marker_level_1",
                "marker_level_2",
                "marker_level_3",
                "marker_level_4",
                "marker_level_5",
                "marker_level_6",
                "marker_level_7",
            ],
            metrics=["visitsCost"],
            period={
                "from_": ANALYTIC_TZ.localize(
                    datetime.datetime.combine(date, datetime.time.min)
                ),
                "to_": ANALYTIC_TZ.localize(
                    datetime.datetime.combine(date, datetime.time.max)
                ),
            },
            filters=[{"field": "visitsCost", "operator": ">", "value": "0"}],
            interval="1d",
        )
        items = response.get("data", [{}])[0].get("items", [])
        logger.info("    ↳ Quantity: %(quantity)d" % {"quantity": len(items)})
        return items

    def filter_metrics(self, value: Dict[str, Any]) -> bool:
        return value.get("metric_name") == "visitsCost"

    def get_analytic(
        self, date_from: datetime.date, date_to: datetime.date
    ) -> List[Dict[str, Any]]:
        """
        Функция сборки записей целиком за период
        """
        data = []
        while date_from <= date_to:
            items = self.get_api_data(date_from)
            for item in items:
                metrics = list(
                    filter(self.filter_metrics, item.get("metrics", []))
                )
                expenses = float(metrics[0].get("value", 0) if metrics else 0)
                dimensions = dict(
                    [
                        (
                            f"dimension_{dimension}",
                            self.dimensions.get(value.get("value"), {}).get(
                                dimension, [None]
                            )[0]
                            or None,
                        )
                        for dimension, value in item.get(
                            "dimensions", {}
                        ).items()
                    ]
                )
                dimensions.update({"date": date_from, "expenses": expenses})
                data.append(dimensions)
            date_from += datetime.timedelta(days=1)
        return data

    def handle(
        self, date_from: datetime.date, date_to: datetime.date = None, **kwargs
    ):
        if date_to is None:
            date_to = datetime.datetime.now().astimezone(ANALYTIC_TZ).date()

        if isinstance(date_from, str):
            date_from = self.parse_date(date_from)
        if isinstance(date_to, str):
            date_to = self.parse_date(date_to)

        logger.info(
            "Update analytic by date range: %(from)s <-> %(to)s"
            % {"from": date_from, "to": date_to}
        )

        analytic = self.get_analytic(date_from, date_to)

        with transaction.atomic():
            RoistatAnalytic.objects.filter(
                date__gte=date_from, date__lte=date_to
            ).delete()
            instances = [RoistatAnalytic(**item) for item in analytic]
            RoistatAnalytic.objects.bulk_create(instances, batch_size=1000)
