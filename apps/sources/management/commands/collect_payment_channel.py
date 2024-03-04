import pandas

from typing import Dict, Any, Optional
from logging import getLogger
from urllib.parse import urlparse

from apps.utils import queryset_as_dataframe
from apps.sources.models import PaymentAnalytic

from plugins.data import data_writer

from ._base import BaseCommand


logger = getLogger(__name__)

FILENAME = "payment_channel.pkl"


class Command(BaseCommand):
    help = "Обработка данных по оплатам"

    def parse_url(self, value: str) -> str:
        url = urlparse(value)
        return url.netloc + url.path if url.netloc and url.path else None

    def detect_empty_params(
        self, value: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        return value if value.get("host") else None

    def get_channel_from_roistat(self, value: str) -> Optional[str]:
        if value is None:
            return
        output = None
        parts = value.split("_")
        if len(parts):
            output = parts[0]
        if not output:
            return
        return output

    def detect_channel_from_params(
        self, value: Optional[Dict[str, Any]]
    ) -> str:
        if value is None:
            return "Undefined"

        query = value.get("get", {})

        output = self.get_channel_from_roistat(query.get("roistat"))
        if output:
            return output

        output = self.get_channel_from_roistat(query.get("rs"))
        if output:
            return output

        utm_source = query.get("utm_source")
        if utm_source:
            return utm_source

        return "Undefined"

    def get_payment(self) -> pandas.DataFrame:
        """
        Получение и обработка сырых данных оплаты из БД
        """
        logger.info("  ↳ Get and preparing payment analytic")
        payments = queryset_as_dataframe(PaymentAnalytic.objects.all())[
            [
                "date_payment",
                "date_last_paid",
                "profit",
                "amocrm_id",
                "roistat_url",
                "params",
            ]
        ]
        payments["params"] = payments["params"].apply(self.detect_empty_params)
        payments["channel"] = payments["params"].apply(
            self.detect_channel_from_params
        )
        payments.dropna(subset=["roistat_url"], inplace=True)
        payments["roistat_url"] = payments["roistat_url"].apply(self.parse_url)
        payments.drop(columns=["params"], inplace=True)
        payments.rename(
            columns={
                "date_payment": "payment_date",
                "roistat_url": "url",
                "date_last_paid": "last_lead_date",
            },
            inplace=True,
        )
        logger.info(
            "    ↳ Quantity: %(quantity)s" % {"quantity": len(payments)}
        )
        return payments

    def handle(self, **kwargs):
        logger.info("Collect payment start")

        payments = self.get_payment()
        data_writer.dataframe(payments, FILENAME)
