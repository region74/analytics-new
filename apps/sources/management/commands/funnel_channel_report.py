import pandas

from typing import Optional
from logging import getLogger
from collections import defaultdict

from apps.choices import FunnelChannelUrlType
from apps.sources.models import RoistatDimension
from apps.traffic.models import FunnelChannelUrl

from plugins.data import data_writer, data_reader

from ._base import BaseCommand


logger = getLogger(__name__)

FILENAME_PROFIT = "funnel_channel_profit.pkl"
FILENAME_EXPENSES = "funnel_channel_expenses.pkl"


class Command(BaseCommand):
    help = "Формирование файлов оборота и расхода для отчета funnel_channel"

    def get_rels(self) -> dict:
        rels_data = list(FunnelChannelUrl.objects.values("group", "url"))
        grouped_rels = defaultdict(list)
        for item in rels_data:
            grouped_rels[item["group"]].append(item["url"])
        result = dict(grouped_rels)
        return result

    def parse_funnel(self, value: str, rels: dict, choices: dict) -> str:
        """
        Получение значения мероприятия по url
        """
        items = list(
            dict(filter(lambda item: value in item[1], rels.items())).keys()
        ) + [""]
        return choices.get(items[0])

    def parse_landing_expenses(self, value: list):
        """
        Обратное получение посадочной страницы
        """
        url = RoistatDimension.objects.filter(pk__in=value).values("pk", "name")
        url_dict = {item["pk"]: item["name"] for item in url}
        return url_dict

    def parse_account_expenses(self, value: list):
        """
        Обратное получение аккаунта
        """
        account = RoistatDimension.objects.filter(pk__in=value).values(
            "pk", "name"
        )
        account_dict = {item["pk"]: item["name"] for item in account}
        return account_dict

    def create_expenses_part(
        self, rels: dict, choices: dict
    ) -> pandas.DataFrame:
        """
        Сборка и подготовка расходной части отчета
        """
        logger.info("  ↳ Create expenses part started")

        filename = "ipl_report.pkl"
        try:
            expenses: pandas.DataFrame = data_reader.dataframe(filename)
        except FileNotFoundError:
            logger.error(f"Исходный файл {filename} не найден")
            return
        landing_parse = self.parse_landing_expenses(expenses["landing"])
        account_parse = self.parse_account_expenses(expenses["account"])
        expenses["landing"] = expenses["landing"].map(landing_parse)
        expenses["landing"] = expenses["landing"].apply(
            self.parse_funnel, args=(rels, choices)
        )
        expenses["account"] = expenses["account"].map(account_parse)
        expenses.drop(columns=["campaign", "group", "ad"], inplace=True)
        expenses.dropna(subset=["landing", "account"], inplace=True)
        expenses.rename(
            columns={
                "date": "lead_date",
                "landing": "url",
                "account": "channel",
            },
            inplace=True,
        )
        logger.info(
            "    ↳ Quantity: %(quantity)s" % {"quantity": len(expenses)}
        )

        return expenses

    def create_profit_part(
        self, rels: dict, choices: dict
    ) -> Optional[pandas.DataFrame]:
        """
        Сборка и подготовка доходной части отчета
        """
        logger.info("  ↳ Create profit part started")

        filename = "payment_channel.pkl"
        try:
            profit: pandas.DataFrame = data_reader.dataframe(filename)
        except FileNotFoundError:
            logger.error(f"Исходный файл {filename} не найден")
            return

        profit["url"] = profit["url"].apply(
            self.parse_funnel, args=(rels, choices)
        )
        profit.drop(columns=["amocrm_id"], inplace=True)
        profit.dropna(subset=["url", "channel"], inplace=True)
        profit.rename(
            columns={"last_lead_date": "lead_date"},
            inplace=True,
        )
        logger.info("    ↳ Quantity: %(quantity)s" % {"quantity": len(profit)})
        return profit

    def handle(self, **kwargs):
        logger.info("Create parts for funnel_channel report start")

        RELS = self.get_rels()
        GROUP_CHOICES = dict(FunnelChannelUrlType.choices())

        profit = self.create_profit_part(RELS, GROUP_CHOICES)
        if profit is None:
            return

        expenses = self.create_expenses_part(RELS, GROUP_CHOICES)
        if expenses is None:
            return

        data_writer.dataframe(profit, FILENAME_PROFIT)
        data_writer.dataframe(expenses, FILENAME_EXPENSES)
