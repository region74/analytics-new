import pytz
import hashlib
import datetime
from logging import getLogger
import requests
import pandas
from io import BytesIO

from django.conf import settings
from django.db import transaction

from apps.sources.models import SipuniCall
from ._base import BaseCommand

logger = getLogger(__name__)


class Command(BaseCommand):
    help = "Сбор данных звонков"

    # Аргументы для команды django
    def add_arguments(self, parser):
        parser.add_argument(
            "-df", "--date-from", required=True, type=self.parse_date
        )
        parser.add_argument(
            "-dt", "--date-to", required=False, type=self.parse_date
        )

    def parse_date(self, value: str) -> datetime.date:
        return datetime.date.fromisoformat(value)

    def get_response(self, df: datetime.date, dt: datetime.date) -> pandas.DataFrame:

        hash_dict = {
            'anonymous': '1',
            'dtmfUserAnswer': '0',
            'firstTime': '0',
            'from': datetime.date.strftime(df, '%Y-%m-%d'),
            'fromNumber': '',
            'names': '1',
            'numbersInvolved': '1',
            'numbersRinged': '1',
            'outgoingLine': '1',
            'showTreeId': '0',
            'state': '0',
            'to': datetime.date.strftime(dt, '%Y-%m-%d'),
            'toAnswer': '',
            'toNumber': '',
            'tree': '000-478278',
            'type': '2',
            'user': settings.SIPUNI_API_USER,
            'hash': settings.SIPUNI_API_HASH,
        }
        hash_string = '+'.join([hash_dict.get(key, '') for key in hash_dict])
        hash = hashlib.md5(hash_string.encode()).hexdigest()
        hash_dict.update({'hash': hash})

        response = requests.post(settings.SIPUNI_API_URL, data=hash_dict)
        response_df: pandas.DataFrame = pandas.read_csv(BytesIO(response.content), delimiter=';', encoding='utf-8',
                                                        header=0)
        return response_df

    def prepare_data(self, response_df: pandas.DataFrame) -> pandas.DataFrame:
        response_df = response_df[
            ['Тип', 'Статус', 'Время', 'Исходящая линия', 'Откуда', 'Куда', 'Длительность звонка',
             'Длительность разговора', 'Время ответа']]
        new_columns_name = ['type', 'status', 'date', 'line', 'call_from', 'call_to', 'time_call',
                            'time_talk', 'time_answer']
        response_df = response_df.rename(columns=dict(zip(response_df.columns, new_columns_name)))
        response_df['dialing'] = [1 if timecalls > 10 and status == 'Отвечен' else 0 for timecalls, status in
                                  zip(response_df['time_call'], response_df['status'])]
        response_df['date'] = pandas.to_datetime(response_df['date'], format='%d.%m.%Y %H:%M:%S')
        response_df['date'] = response_df['date'].dt.tz_localize('Europe/Moscow')
        prepared_df: pandas.DataFrame = response_df.fillna(0)
        return prepared_df

    def handle(self, date_from: datetime.date, date_to: datetime.date = None, **kwargs):
        tz = pytz.timezone(settings.ANALYTIC_TIME_ZONE)
        if date_to is None:
            date_to = datetime.datetime.now().astimezone(tz).date()
        if isinstance(date_from, str):
            date_from = self.parse_date(date_from)
        if isinstance(date_to, str):
            date_to = self.parse_date(date_to)
        db_list = []
        select_date = date_from
        while select_date <= date_to:
            try:
                response_df = self.get_response(select_date, select_date)
                result: pandas.DataFrame = self.prepare_data(response_df)
                db_list.extend(result.to_dict(orient='records'))
            except Exception:
                print('Ошибка данных сипуни')
            finally:
                select_date += datetime.timedelta(days=1)
        with transaction.atomic():
            if db_list:
                SipuniCall.objects.filter(
                    date__gte=datetime.datetime.combine(date_from, datetime.datetime.min.time()),
                    date__lte=datetime.datetime.combine(date_to, datetime.datetime.max.time())
                ).delete()
                instances = [SipuniCall(**item) for item in db_list]
                SipuniCall.objects.bulk_create(instances, batch_size=1000)
