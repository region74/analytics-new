import html
from typing import Tuple, Dict
from datetime import date, timedelta
from urllib.parse import urlparse, parse_qsl

import pandas

from apps.utils import detect_channel_by_querystring


def detect_week(value: date) -> Tuple[date, date]:
    start_week = 3
    date_from = value - timedelta(
        days=value.weekday() + (7 if value.weekday() < start_week else 0) - start_week
    )
    date_to = date_from + timedelta(days=6)
    return date_from, date_to


def detect_category_url(value: str, landings: Dict) -> str:
    if value is None:
        return 'Undefined'
    association = {
        "intensive3day": "type_intensiv3",
        "intensive2day": "type_intensiv2",
        "chatgpt": "type_gpt_5lesson",
        "course7lesson": "type_ai_7lesson",
        "neirostaff": "type_neirostaff",
        "chatgptveb": "type_gpt_vebinar",
    }
    url = urlparse(html.unescape(value))
    result = association.get(landings.get(url.netloc + url.path, 'Undefined'), None)
    return result


def detect_channel_url(value: str, channel: Dict) -> str:
    url = urlparse(html.unescape(value))
    params = dict(parse_qsl(url.query))
    result = channel.get(detect_channel_by_querystring(params), 'Undefined')
    return result


def detect_expenses_channel(value: str, channel: Dict) -> str:
    return channel.get(value, 'Undefined')


def convert_to_romi(result_df: pandas.DataFrame) -> pandas.DataFrame:
    result_df['week1'] = result_df.apply(
        lambda row: ((row['week1'] - row['expenses']) / row['expenses']) * 100 if (row['week1'] != 0) and (
                row['expenses'] != 0) else 0, axis=1)
    result_df['week2'] = result_df.apply(
        lambda row: ((row['week2'] - row['expenses']) / row['expenses']) * 100 if (row['week2'] != 0) and (
                row['expenses'] != 0) else 0, axis=1)
    result_df['week4'] = result_df.apply(
        lambda row: ((row['week4'] - row['expenses']) / row['expenses']) * 100 if (row['week4'] != 0) and (
                row['expenses'] != 0) else 0, axis=1)
    result_df['week8'] = result_df.apply(
        lambda row: ((row['week8'] - row['expenses']) / row['expenses']) * 100 if (row['week8'] != 0) and (
                row['expenses'] != 0) else 0, axis=1)
    return result_df
