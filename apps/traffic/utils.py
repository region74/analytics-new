import html
from datetime import date, datetime, time, timedelta
from typing import Optional
from urllib.parse import parse_qsl, urlparse

import pandas
import pytz
from django.conf import settings
from django.utils import timezone

from apps.api.v1.tilda.views import AVAILABLE_FIELDS_NAME as AVAILABLE_FIELDS_NAME_BASE
from apps.api.v1.tilda.views import LeadAPIView as TildaLeadAPIView
from apps.carousel.management.commands.utils import HttpRequest as HttpRequestCommands
from apps.carousel.management.commands.utils import TildaLeadsParseData as TildaLeadsParseDataCommands
from apps.choices import TelegramSubscriptionType
from apps.sources.models import Lead, TelegramSubscription
from apps.utils import detect_channel_by_querystring
from plugins.data import data_reader

ANALYTIC_TZ = pytz.timezone(settings.ANALYTIC_TIME_ZONE)

AVAILABLE_FIELDS_NAME = AVAILABLE_FIELDS_NAME_BASE.copy()
AVAILABLE_FIELDS_NAME.update({"date_created": ["date_created"]})


def translate_channel(value: str, channels: dict) -> str:
    return channels[value] if value in channels else value


def detect_pay_traffic(value: str, landings: list) -> bool:
    return True if value in landings else False


def parse_url_params(value: str) -> dict:
    url = urlparse(html.unescape(value))
    result = {
        "host": url.netloc,
        "path": url.path,
        "get": dict(parse_qsl(url.query)),
    }
    return result


def parse_url(value: str) -> str:
    url = urlparse(value)
    return url.netloc + url.path if url.netloc and url.path else None


def get_event(value: str, channel_events: list[dict]) -> str:
    for item in channel_events:
        if item.get("url") == value:
            return item.get("group")
    return 'Undefined'


def detect_empty_params(value):
    return value if value.get("host") != "" else None


def detect_channel_from_params(value: [dict, None]) -> str:
    if value is None:
        return "Undefined"
    elif value.get("get").get("roistat"):
        channel = value.get("get").get("roistat")
        parts = channel.split("_")
        result = parts[0] if parts else parts
        return result

    elif value.get("get").get("rs"):
        channel = value.get("get").get("rs")
        parts = channel.split("_")
        result = parts[0] if parts else parts
        return result

    elif value.get("get").get("utm_source"):
        result = value.get("get").get("utm_source")
        return result
    else:
        return "Undefined"


class TildaLeadsParseData(TildaLeadsParseDataCommands):
    def get_sp_book_id(self):
        sp_book_id = self.tmp_dict.get("sp_book_id", "")
        if sp_book_id:
            return int(float(sp_book_id))
        return sp_book_id

    def to_dict(self):
        data = super().to_dict()
        data.pop("amocrm_id", None)
        data["date_created"] = self.tmp_dict.get("created", timezone.now())
        data["sp_book_id"] = self.get_sp_book_id()
        return data


class HttpRequest(HttpRequestCommands):
    pass


class LeadAPIView(TildaLeadAPIView):
    available_fields_name = AVAILABLE_FIELDS_NAME


def detect_pay_url_category(value: str, landings: list) -> str:
    association = {
        "intensive3day": "type_intensiv3",
        "intensive2day": "type_intensiv2",
        "neirostaff": "type_neirostaff",
    }

    if "baza" in value:
        return "Undefined"
    else:
        for item in landings:
            if item["url"] == value:
                return association.get(item["group"])
        return "Undefined"


def detect_channel_tgreport(value: str) -> str:
    url = urlparse(html.unescape(value))
    if "baza" in url.netloc + url.path:
        params = dict(parse_qsl(url.query))
        return (
            params.get("utm_campaign") if params.get("utm_campaign") else "Undefined"
        )
    else:
        params = dict(parse_qsl(url.query))
        result = detect_channel_by_querystring(params)
        return result


def _get_datetime_period_for_cr_report(from_date: date, to_date: date) -> tuple[datetime, datetime]:
    """Получение временного промежутка с from_date - 7 дней по to_date - 1 день"""
    from_date = from_date - timedelta(days=7)
    to_date = to_date - timedelta(days=1)

    from_date_time = ANALYTIC_TZ.localize(
        datetime.combine(from_date, time.min)
    )
    to_date_time = ANALYTIC_TZ.localize(
        datetime.combine(to_date, time.max)
    )

    return from_date_time, to_date_time


def get_members_for_cr(event_df: date, event_dt: date, courses: list):
    """
    Получение участников всех трёх курсов (изначальные данные в гугл таблице)
    см. https://docs.google.com/spreadsheets/d/1KdI82fdMge4PQ3FqLfQkYdUj28ogMLhh
    """
    members_df: pandas.DataFrame = data_reader.dataframe("intensives_members.pkl")
    members_df = members_df[
        (members_df["date"] >= event_df)
        & (members_df["date"] <= event_dt)
        ]
    association_map = {
        "Интенсив 2 дня": "type_intensiv2",
        "Интенсив 3 дня": "type_intensiv3",
        "Интенсив chatGPT": "type_neirostaff",
    }
    members_df['course'] = members_df['course'].map(association_map)
    members_df.dropna(subset="course", inplace=True)
    members_df.drop_duplicates(subset=['course', 'date', 'email'], inplace=True)

    if "type_all" not in courses:
        members_df = members_df[members_df["course"].isin(courses)]
    return members_df


def get_regs_for_cr(date_of_events: pandas.DataFrame, landings: list) -> Optional[pandas.DataFrame]:
    """
    Получение регистраций (лидов) по нужным курсам и их датам.

    Для каждого курса своя выборка лидов, которая определяется как лиды из бд за период
    - с дата_курса - 7 дней по дата_курса - 1 день.
    """
    if date_of_events.empty:
        return None

    from_date = date_of_events["date"].min()
    to_date = date_of_events["date"].max()

    from_date_time, to_date_time = _get_datetime_period_for_cr_report(from_date, to_date)

    leads_db = Lead.objects.filter(
        date_created__date__gte=from_date_time,
        date_created__date__lte=to_date_time,
    ).values("date_created", "email", "roistat_url")

    leads_df = pandas.DataFrame.from_records(leads_db)
    leads_df.drop_duplicates(inplace=True)
    leads_df["channel"] = leads_df["roistat_url"].apply(detect_channel_tgreport)
    leads_df["url"] = leads_df["roistat_url"].apply(parse_url)
    leads_df.dropna(subset=["url", "email"], inplace=True)
    leads_df["category"] = leads_df["url"].apply(detect_pay_url_category, args=(landings,))
    leads_df = leads_df[leads_df["category"] != "Undefined"]

    result = pandas.DataFrame()

    for item_date, course in date_of_events.itertuples(index=False):
        item_date_time_from, item_date_time_to = _get_datetime_period_for_cr_report(item_date, item_date)
        date_from_mask = leads_df["date_created"] >= item_date_time_from
        date_to_mask = leads_df["date_created"] <= item_date_time_to
        group_leads = leads_df[date_from_mask & date_to_mask].copy()
        group_leads = group_leads[group_leads["category"] == course]
        group_leads["date_event"] = item_date
        group_leads["is_duplicated"] = group_leads["email"].duplicated(keep=False)

        result = pandas.concat([result, group_leads], ignore_index=True)

    if not result.empty:
        result.drop(columns=["roistat_url", "url", "date_created"], inplace=True)
        return result


def get_subscriptions_for_cr(date_of_events: pandas.DataFrame, landings: list) -> Optional[pandas.DataFrame]:
    """
        Получение подписок по нужным курсам и их датам.

        Для каждого курса своя выборка подписок, которая определяется как подписки из бд за период
        - с дата_курса - 7 дней по дата_курса - 1 день.
        """
    if date_of_events.empty:
        return None

    from_date = date_of_events["date"].min()
    to_date = date_of_events["date"].max()

    from_date_time, to_date_time = _get_datetime_period_for_cr_report(from_date, to_date)

    subscriptions_db = TelegramSubscription.objects.filter(
        action=TelegramSubscriptionType.subscribe.name,
        created__date__gte=from_date_time,
        created__date__lte=to_date_time,
    ).values("created", "referrer", "email")

    subscriptions_df = pandas.DataFrame.from_records(subscriptions_db)
    subscriptions_df["channel"] = subscriptions_df["referrer"].apply(detect_channel_tgreport)
    subscriptions_df["url"] = subscriptions_df["referrer"].apply(parse_url)
    subscriptions_df.dropna(subset="url", inplace=True)
    subscriptions_df["category"] = subscriptions_df["url"].apply(detect_pay_url_category, args=(landings,))
    subscriptions_df = subscriptions_df[subscriptions_df["category"] != "Undefined"]
    subscriptions_df.sort_values(by="created", inplace=True)

    result = pandas.DataFrame()

    for item_date, course in date_of_events.itertuples(index=False):
        item_date_time_from, item_date_time_to = _get_datetime_period_for_cr_report(item_date, item_date)
        date_from_mask = subscriptions_df["created"] >= item_date_time_from
        date_to_mask = subscriptions_df["created"] <= item_date_time_to
        group_subscriptions = subscriptions_df[date_from_mask & date_to_mask].copy()
        group_subscriptions = group_subscriptions[group_subscriptions["category"] == course]
        group_subscriptions["date_event"] = item_date
        group_subscriptions.drop_duplicates(subset=("email", "channel"), inplace=True)

        result = pandas.concat([result, group_subscriptions], ignore_index=True)

    if not result.empty:
        result.drop(columns=["referrer", "url", "created"], inplace=True)
        return result