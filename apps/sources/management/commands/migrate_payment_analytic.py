import re
import html
import pandas
import datetime

from typing import List, Dict, Any, Optional
from logging import getLogger
from urllib.parse import urlparse, parse_qsl

from django.db import transaction
from django.contrib.auth import get_user_model

from apps.utils import slugify, queryset_as_dataframe
from apps.choices import PaymentAnalyticType, RoistatDimensionType, UserGroup
from apps.sources.models import PaymentAnalytic, RoistatDimension, AmocrmLead
from plugins.webhooks.workers import WebhookWorker

from plugins.google.sheets import SheetsAPIClient

from ._base import BaseCommand

logger = getLogger(__name__)

User = get_user_model()


class Command(BaseCommand):
    help = 'Сбор данных из таблицы "Аналитика по оплатам"'

    users: Dict[str, User]

    def __init__(self, *args, **kwargs):
        self.update_users()
        super().__init__(*args, **kwargs)

    def update_users(self):
        self.users = dict(
            (f"{user.last_name} {user.first_name}", user)
            for user in User.objects.all()
        )
        self.users.update(
            dict(
                (f"{user.first_name} {user.last_name}", user)
                for user in self.users.values()
            )
        )

    def parse_email(self, value: str) -> str:
        if not value:
            return ""
        return value.strip()

    def parse_amocrm_id(self, value: str) -> str:
        if not value:
            return ""
        matched = re.match(
            r"^/leads/detail/(\d+).*$", urlparse(value.strip()).path
        )
        return str(matched.group(1)) if matched else ""

    def parse_manager(self, value: str) -> str:
        values = re.split(r"\s+", value.strip())
        if len(values) != 2:
            return ""
        return " ".join([item.strip() for item in values])

    def parse_group(self, value: str) -> str:
        try:
            return str(int(value))
        except ValueError:
            return ""

    def parse_user(self, value: str) -> Optional[User]:
        if value is None:
            return
        return self.users.get(value)

    def parse_profit(self, value: str) -> int:
        value = re.sub(r"\D+", "", value.strip())
        if not value:
            return 0
        return int(value)

    def parse_date(self, value: str) -> Optional[datetime.date]:
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(value, "%d.%m.%Y").date()
        except ValueError:
            pass

    def parse_type(self, value: str) -> str:
        try:
            return PaymentAnalyticType(value.strip().title()).name
        except ValueError:
            return PaymentAnalyticType.other.name

    def parse_url_params(self, value: str) -> dict:
        url = urlparse(html.unescape(value))
        result = {
            "host": url.netloc,
            "path": url.path,
            "get": dict(parse_qsl(url.query)),
        }
        return result

    def parse_str(self, value: str) -> str:
        if not value:
            value = ""
        return value.strip()

    def get_payments(self) -> pandas.DataFrame:
        logger.info("  ↳ Request API")
        sheets_api = SheetsAPIClient()
        worksheet = sheets_api.payments_analytic.worksheet("Все оплаты")
        values = worksheet.get_all_values()
        data = pandas.DataFrame(data=values[1:], columns=values[0])
        logger.info("    ↳ Quantity: %(quantity)d" % {"quantity": len(data)})
        return data

    def prepare_data(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data.rename(
            columns=dict((item, slugify(item)) for item in data.columns),
            inplace=True,
        )
        columns = {
            "pochta": "email",
            "ssylka_na_amocrm": "amocrm_id",
            "menedzher": "manager",
            "gr": "manager_group",
            "summa_vyruchki": "profit",
            "data_sozdanija_sdelki": "date_created",
            "data_poslednej_zajavki_platnoj": "date_last_paid",
            "data_oplaty": "date_payment",
            "data_zoom": "date_zoom",
            "mesjats_doplata": "type",
            "tselevaja_ssylka": "roistat_url",
        }

        data = data.rename(columns=columns)[columns.values()]
        data.fillna("", inplace=True)
        data["email"] = data["email"].apply(self.parse_email)
        data["amocrm_id"] = data["amocrm_id"].apply(self.parse_amocrm_id)
        data["manager"] = data["manager"].apply(self.parse_manager)
        data["manager_group"] = data["manager_group"].apply(self.parse_group)
        data["profit"] = data["profit"].apply(self.parse_profit)
        data["date_created"] = data["date_created"].apply(self.parse_date)
        data["date_last_paid"] = data["date_last_paid"].apply(self.parse_date)
        data["date_payment"] = data["date_payment"].apply(self.parse_date)
        data["date_zoom"] = data["date_zoom"].apply(self.parse_date)
        data["type"] = data["type"].apply(self.parse_type)
        data["roistat_url"] = data["roistat_url"].apply(self.parse_str)
        data["params"] = data["roistat_url"].apply(self.parse_url_params)
        data = data[data["amocrm_id"] != ""]
        return data

    def parse_landing(self, value: str) -> str:
        if not value:
            return ""
        url = urlparse(value)
        return f"{url.scheme}://{url.netloc}{url.path}"

    def get_diff(self, data_new: pandas.DataFrame) -> pandas.DataFrame:
        columns = ["email", "amocrm_id", "date_created", "date_payment"]

        data = queryset_as_dataframe(PaymentAnalytic.objects.all())[columns]
        data = data.fillna("").astype(str)
        data["id"] = data.apply(lambda item: tuple(item.to_list()), axis=1)

        data_new = data_new.fillna("").astype(str)
        data_new.loc[:, ["id"]] = data_new[columns].apply(
            lambda item: tuple(item.to_list()), axis=1
        )

        for _, row in data.iterrows():
            matched = data_new[data_new["id"] == row["id"]]
            if matched.empty:
                continue
            data_new = data_new.drop([matched.iloc[0].name])

        data_new.drop(columns=["id"], inplace=True)
        return data_new

    def add_webhook_queue(self, data: pandas.DataFrame):
        webhook_worker = WebhookWorker()
        for _, row in data.iterrows():
            roistat_url = row["roistat_url"]
            parsed_url = urlparse(roistat_url)
            query_params = dict(parse_qsl(parsed_url.query))
            webhook_worker(
                name="leadgrab_purchase",
                data_get={
                    "email": row["email"],
                    "action_id": f'neural-{row["amocrm_id"]}',
                    "sum": row["profit"],
                    "clickid": query_params.get("utm_content", ""),
                },
            )

    def compare_dicts(
        self, target: Dict[str, Any], source: Dict[str, Any]
    ) -> int:
        output = 0
        for key, value in target.items():
            if value == source.get(key):
                output += 1
        return output

    def get_available_marker_level_1(self, params: Dict[str, Any]) -> List[str]:
        available = []

        roistat = params.get("roistat") or None
        if roistat:
            roistat_split = roistat.split("_", 2)
            if roistat_split:
                available.append(roistat_split[0])

        rs = params.get("rs") or None
        if rs:
            rs_split = rs.split("_", 2)
            if rs_split:
                available.append(rs_split[0])

        utm_source = params.get("utm_source") or None
        if utm_source:
            utm_source_split = utm_source.split("_", 2)
            if utm_source_split:
                available.append(utm_source_split[0])

        return list(set(available))

    def get_group(self, user: User, record: Dict[str, Any]) -> Optional[str]:
        user_group = user.group if user else None
        if user_group:
            return user_group

        record_group = record.get("manager_group")
        try:
            return UserGroup[f"group_{record_group}"].name
        except KeyError:
            pass

    def get_instances(self, data: pandas.DataFrame) -> List[PaymentAnalytic]:
        instances = []
        records = data.to_dict(orient="records")

        amocrm_ids = list(
            filter(
                None, [record.get("amocrm_id") or None for record in records]
            )
        )
        amocrms = dict(
            [
                (item[0], {"utm_source": item[1], "roistat_url": item[2]})
                for item in AmocrmLead.objects.filter(
                    amocrm_id__in=amocrm_ids
                ).values_list("amocrm_id", "utm_source", "roistat_url")
            ]
        )
        for record in records:
            record["roistat_marker_level_1"] = []
            amocrm_id = record.get("amocrm_id") or None
            if amocrm_id:
                source = amocrms.get(int(amocrm_id)) or None
                if source:
                    url = urlparse(source.get("roistat_url"))
                    params = dict(parse_qsl(url.query))
                    record[
                        "roistat_marker_level_1"
                    ] += self.get_available_marker_level_1(params) + [
                        source.get("utm_source")
                    ]

        roistat_marker_level_1_available = []
        for record in records:
            params = record.get("params", {}).get("get", {})
            available = list(
                set(
                    self.get_available_marker_level_1(params)
                    + record.get("roistat_marker_level_1")
                )
            )
            roistat_marker_level_1_available += available
            record["roistat_marker_level_1"] = available
        roistat_marker_level_1_available = list(
            set(roistat_marker_level_1_available)
        )

        roistat_marker_level_1_dict_names = dict(
            map(
                lambda item: (item.name, item),
                RoistatDimension.objects.filter(
                    name__in=roistat_marker_level_1_available,
                    type=RoistatDimensionType.marker_level_1.name,
                ),
            )
        )
        roistat_marker_level_1_dict_titles = dict(
            map(
                lambda item: (item.title, item),
                RoistatDimension.objects.filter(
                    title__in=roistat_marker_level_1_available,
                    type=RoistatDimensionType.marker_level_1.name,
                ),
            )
        )
        roistat_marker_level_1_dict = {
            **roistat_marker_level_1_dict_titles,
            **roistat_marker_level_1_dict_names,
        }

        for record in records:
            roistat_marker_level_1_match = dict(
                filter(
                    lambda item: item[1] is not None,
                    {
                        key: roistat_marker_level_1_dict.get(key)
                        for key in record["roistat_marker_level_1"]
                    }.items(),
                )
            )
            if roistat_marker_level_1_match:
                roistat_marker_level_1 = list(
                    roistat_marker_level_1_match.values()
                )[0]
            else:
                roistat_marker_level_1 = None
            record.update({"roistat_marker_level_1": roistat_marker_level_1})
            user = self.parse_user(record.get("manager"))
            group = self.get_group(user, record)
            record.update({"user": user, "group": group})
            instances.append(PaymentAnalytic(**record))

        return instances

    def handle(self, **kwargs):
        logger.info("Update payment analytic")

        payments = self.get_payments()
        data = self.prepare_data(payments)

        data_new = self.get_diff(data)

        with transaction.atomic():
            PaymentAnalytic.objects.all().delete()
            instances = self.get_instances(data)
            PaymentAnalytic.objects.bulk_create(instances, batch_size=1000)
            self.add_webhook_queue(data_new)
