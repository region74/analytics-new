import html
import pandas

from logging import getLogger
from urllib.parse import urlparse

from django.db import transaction

from apps.traffic.models import LandingPage

from apps.sources.management.commands._base import BaseCommand
from plugins.google.sheets import SheetsAPIClient

logger = getLogger(__name__)


class Command(BaseCommand):
    help = "Обновление списка платных url"

    def parse_url(self, value: str) -> str:
        parse_url = urlparse(html.unescape(value))
        url = parse_url.netloc + parse_url.path
        return url

    def get_remote_url(self) -> pandas.DataFrame:
        sheets_api = SheetsAPIClient()
        worksheet = sheets_api.paid_urls.worksheet("Лендинги платный трафик и база")
        values = worksheet.get_all_values()
        data = pandas.DataFrame(data=values[1:], columns=values[0])
        return data

    def get_base_url(self) -> list:
        result = list(LandingPage.objects.filter(paid=True).values_list("url", flat=True))
        return result

    def update_base(self, remote: pandas.DataFrame, base: list):
        remote.rename(columns={'Посадочная': 'url', 'Тип трафика': 'type'}, inplace=True)
        df = remote[['url', 'type']]
        df = df[df['type'] == 'платный трафик']
        df = df.dropna()
        df['params'] = df['url'].apply(self.parse_url)
        remote_list = list(set(df['params'].tolist()))
        new_urls = [item for item in remote_list if item not in base]
        if new_urls:
            landing_pages = [LandingPage(url=url, paid=True) for url in new_urls]
            with transaction.atomic():
                LandingPage.objects.bulk_create(landing_pages)
                logger.info("  ↳ Quantity: %(quantity)d" % {"quantity": len(landing_pages)})
        else:
            logger.info("No data to add")

    def handle(self, **kwargs):
        logger.info("Update paid url start")
        remote_urls: pandas.DataFrame = self.get_remote_url()
        base_urls = self.get_base_url()
        self.update_base(remote_urls, base_urls)
        logger.info("Update paid url finish")
