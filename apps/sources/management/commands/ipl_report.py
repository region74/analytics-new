import pytz
import pandas
import datetime

from time import sleep
from typing import Dict
from logging import getLogger

from django.conf import settings
from django.core.management.base import BaseCommand

from apps.utils import (
    queryset_as_dataframe,
    detect_package,
    get_package_dimensions,
)
from apps.choices import LeadLevel, RoistatDimensionType
from apps.sources.models import RoistatAnalytic, RoistatDimension

from plugins.data import data_reader, data_writer


logger = getLogger(__name__)

FILENAME = "ipl_report.pkl"
ANALYTIC_TZ = pytz.timezone(settings.ANALYTIC_TIME_ZONE)
IPL_REPORT_COLUMNS = ["date", "expenses", "landing"] + [
    item.name for item in LeadLevel
]


class Command(BaseCommand):
    help = 'Расходы для отчета "IPL по каналам"'

    dimensions_level_1: Dict[int, RoistatDimension]

    def __init__(self, *args, **kwargs):
        self.update_dimensions_level_1()
        super().__init__(*args, **kwargs)

    def update_dimensions_level_1(self):
        self.dimensions_level_1 = dict(
            (
                (item.pk, item)
                for item in RoistatDimension.objects.filter(
                    type=RoistatDimensionType.marker_level_1.name
                )
            )
        )

    def parse_date(self, value: str) -> datetime.date:
        return datetime.date.fromisoformat(value)

    def add_arguments(self, parser):
        parser.add_argument(
            "-df", "--date-from", required=True, type=self.parse_date
        )
        parser.add_argument(
            "-dt", "--date-to", required=False, type=self.parse_date
        )

    def date_range(
        self,
        start: datetime.datetime,
        stop: datetime.datetime,
        step: datetime.timedelta,
    ) -> iter:
        while start < stop:
            date_next = start + step
            if date_next > stop:
                date_next = stop
            yield start, date_next
            start += step

    def get_roistat_analytic(self, date: datetime.date) -> pandas.DataFrame:
        data = queryset_as_dataframe(RoistatAnalytic.objects.filter(date=date))
        data.drop(columns=["id"], inplace=True)
        logger.info(
            "      ↳ Roistat analytic quantity: %(quantity)s"
            % {"quantity": len(data)}
        )
        return data

    def detect_levels(self, data: pandas.Series) -> pandas.Series:
        dimension = self.dimensions_level_1.get(
            data["dimension_marker_level_1_id"]
        )
        package = detect_package(
            dimension.name if dimension is not None else ""
        )
        levels = dict((item.name, None) for item in LeadLevel)
        levels.update(
            dict(
                (value.name, data[f"dimension_{key.name}_id"])
                for key, value in get_package_dimensions(package).items()
            )
        )
        return pandas.Series(levels)

    def create_report(self, roistat: pandas.DataFrame) -> pandas.DataFrame:
        if not len(roistat):
            return pandas.DataFrame(columns=IPL_REPORT_COLUMNS)
        logger.info("     ↳ Create report")
        roistat[[item.name for item in LeadLevel]] = roistat.apply(
            self.detect_levels, axis=1
        )
        roistat["landing"] = roistat["dimension_landing_page_id"]
        roistat.drop(
            columns=[
                "dimension_landing_page_id",
                "dimension_marker_level_1_id",
                "dimension_marker_level_2_id",
                "dimension_marker_level_3_id",
                "dimension_marker_level_4_id",
                "dimension_marker_level_5_id",
                "dimension_marker_level_6_id",
                "dimension_marker_level_7_id",
            ],
            inplace=True,
        )
        return roistat

    def save_levels(self, dataframe: pandas.DataFrame):
        for level in ["account", "campaign", "group", "ad", "landing"]:
            items = dataframe[level].unique().tolist()
            undefined = [(0, "Undefined")] if 0 in items else []
            data_writer.dict(
                dict(
                    undefined
                    + list(
                        RoistatDimension.objects.filter(pk__in=items)
                        .values_list("pk", "title")
                        .order_by("title")
                    )
                ),
                f"ipl_report_level_{level}.json",
            )

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
            "Update IPL report by date range: %(from)s <-> %(to)s"
            % {"from": date_from, "to": date_to}
        )

        try:
            dataframe = data_reader.dataframe(FILENAME)
        except FileNotFoundError:
            dataframe = pandas.DataFrame(columns=IPL_REPORT_COLUMNS)

        while date_from <= date_to:
            logger.info("  ↳ Update report: %(date)s" % {"date": date_from})

            roistat_analytic = self.get_roistat_analytic(date_from)
            dataframe_period = self.create_report(roistat_analytic)

            dataframe = dataframe[dataframe["date"] != date_from]
            dataframe = pandas.concat(
                [dataframe, dataframe_period], ignore_index=True
            ).sort_values(by="date", ignore_index=True)

            sleep(1)
            date_from += datetime.timedelta(days=1)

        rel_columns = ["landing"] + [item.name for item in LeadLevel]
        dataframe[rel_columns] = dataframe[rel_columns].fillna(0).astype(int)

        self.save_levels(dataframe)

        data_writer.dataframe(dataframe, FILENAME)
