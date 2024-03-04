import html
import pandas

from logging import getLogger
from urllib.parse import urlparse

from django.db import transaction

from apps.choices import FunnelChannelUrlType
from apps.traffic.models import FunnelChannelUrl

from apps.sources.management.commands._base import BaseCommand
from plugins.google.sheets import SheetsAPIClient

logger = getLogger(__name__)


class Command(BaseCommand):
    help = "Обновление списка категорий url"

    def parse_url(self, value: str) -> str:
        parse_url = urlparse(html.unescape(value))
        url = parse_url.netloc + parse_url.path
        return url

    def translate_category(self, value: str) -> str:
        association = {
            'Нейростафф': FunnelChannelUrlType.neirostaff.name,
            'ChatGPT. Курс 5 уроков': FunnelChannelUrlType.chatgpt.name,
            'Курс AI. 7 уроков': FunnelChannelUrlType.course7lesson.name,
            'Интенсив 3 дня': FunnelChannelUrlType.intensive3day.name,
            'ChatGPT. Вебинар': FunnelChannelUrlType.chatgptveb.name,
            'Интенсив 2 дня': FunnelChannelUrlType.intensive2day.name,
            'Вселенная AI': FunnelChannelUrlType.universe.name,
        }
        return association.get(value) if association.get(value) else None

    def get_remote_url(self) -> list:
        sheets_api = SheetsAPIClient()
        worksheet = sheets_api.paid_urls.worksheet("Лендинги платный трафик и база")
        values = worksheet.get_all_values()
        data = pandas.DataFrame(data=values[1:], columns=values[0])
        new_column_names = {
            'Посадочная': 'url_default',
            'Продукт/Оффер': 'type',
        }
        data = data.rename(columns=new_column_names)
        data = data[["url_default", "type"]]
        data = data[data['type'] != '']
        data['url'] = data['url_default'].apply(self.parse_url)
        data['category'] = data['type'].apply(self.translate_category)
        data.dropna(subset=['category'], inplace=True)
        tuple_records = data[['url', 'category']].to_records(index=False)
        result = list(tuple_records)
        return result

    def get_base_url(self) -> list:
        result = list(FunnelChannelUrl.objects.values_list('url', 'group'))
        return result

    def update_base(self, remote: list, base: list):
        set_remote = set(tuple(item) for item in remote)
        set_base = set(tuple(item) for item in base)
        result_set = set_remote.difference(set_base)
        difference_list = list(result_set)
        if difference_list:
            funnel_channel_url_objects = [FunnelChannelUrl(url=url, group=group) for url, group in difference_list]
            with transaction.atomic():
                FunnelChannelUrl.objects.bulk_create(funnel_channel_url_objects)
                logger.info("  ↳ Quantity: %(quantity)d" % {"quantity": len(difference_list)})
        else:
            logger.info("No data to add")

    def handle(self, **kwargs):
        logger.info("Update category url start")
        remote_category = self.get_remote_url()
        base_category = self.get_base_url()
        self.update_base(remote_category, base_category)
        logger.info("Update category url finish")
