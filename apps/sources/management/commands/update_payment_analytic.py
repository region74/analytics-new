import datetime
import html
import re
from typing import Tuple, Union

import pandas
import pytz

from logging import getLogger
from urllib.parse import urlparse, parse_qsl

from django.db import transaction
from django.db.models import Max, QuerySet, Func, F, Q
from django.db.models.functions import Lower
from django.utils import timezone

from apps.sources.models import PaymentAnalytic, AmocrmContact, AmocrmUser, Lead

from apps.sources.management.commands._base import BaseCommand
from apps.traffic.models import LandingPage
from apps.utils import queryset_as_dataframe
from plugins.amocrm.api import AmocrmAPIClient
from plugins.google.sheets import SheetsAPIClient

logger = getLogger(__name__)


class Command(BaseCommand):
    help = "Обновление таблицы оплат"

    def detect_amocrm_id(self, value: str) -> str:
        if not value:
            return ""
        matched = re.match(
            r"^/leads/detail/(\d+).*$", urlparse(value.strip()).path
        )
        return str(matched.group(1)) if matched else ""

    def detect_amocrm_email(self, value: str, contacts: list) -> str:
        if not value:
            return ''
        return next((contact['email'] for contact in contacts if contact.get('amocrm_id') == int(value)), '')

    def check_url_channel(self, value: str, paid_urls: list[str]) -> bool:
        block_list = ['webinar', 'web', 'email', 'bot', 'smm', 'online', 'reality', 'minilesson', 'mail']
        url = urlparse(html.unescape(value))
        params = dict(parse_qsl(url.query))
        if url.netloc + url.path in paid_urls and params.get('utm_source') and params.get(
                'utm_source') not in block_list:
            return True
        return False

    def check_url(self, value: str, paid_urls: list[str]) -> bool:
        url = urlparse(html.unescape(value))
        return True if url.netloc + url.path in paid_urls else False

    def detect_lead(self, item_date: datetime.date, item_email: str, lead_df: pandas.DataFrame, paid_urls: list[str]) -> \
            Union[pandas.Series, None]:
        filtered_df = lead_df[(lead_df['email'] == item_email) & (lead_df['date_created'] <= item_date)]
        if not filtered_df['date_created'].empty:
            filtered_df['date_created'] = pandas.to_datetime(filtered_df['date_created'])
            max_records = len(filtered_df)
            for records_to_select in range(1, max_records + 1):
                current_df = filtered_df.nlargest(max_records, 'date_created')
                current_df = current_df.iloc[[records_to_select - 1]]
                url = current_df['roistat_url'].iloc[0]
                check = self.check_url_channel(url, paid_urls)
                if check:
                    return current_df
            for records_to_select in range(1, max_records + 1):
                current_df = filtered_df.nlargest(max_records, 'date_created')
                current_df = current_df.iloc[[records_to_select - 1]]
                url = current_df['roistat_url'].iloc[0]
                check = self.check_url(url, paid_urls)
                if check:
                    return current_df
            return None
        return None

    def update_lead_and_url(self, row: pandas.Series, lead_df: pandas.DataFrame, paid_urls: list[str]) -> pandas.Series:
        # Если почты нет, смысла обработки нет
        if not row['amo_email']:
            return row
        # TODO: в apps/utils.slugify для транслита
        if row['course'] == 'доп.курсы':
            paid_list = row['paid_date']
            updated_last_paid_lead = []
            updated_target_url = []
            for index, item in enumerate(paid_list):
                closest_lead = self.detect_lead(item, row['amo_email'], lead_df, paid_urls)
                if closest_lead is not None:
                    paid_date = closest_lead['date_created'].item().date().strftime("%d.%m.%Y")
                    roistat_url = closest_lead['roistat_url'].item()
                    updated_last_paid_lead.append(paid_date)
                    updated_target_url.append(roistat_url)
                else:
                    updated_last_paid_lead.append(row.loc['last_paid_lead'][index])
                    updated_target_url.append(
                        row.loc['target_url'][index] if self.check_url_channel(row.loc['target_url'][index],
                                                                               paid_urls) else 'Undefined')
            row['last_paid_lead'] = updated_last_paid_lead
            row['target_url'] = updated_target_url
            return row
        else:
            paid_list = row['paid_date']
            paid_type = row['paid_type']
            updated_last_paid_lead = []
            updated_target_url = []
            for date_item, type_item in zip(paid_list, paid_type):
                if type_item != 'доплата':
                    closest_lead = self.detect_lead(date_item, row['amo_email'], lead_df, paid_urls)
                    if closest_lead is not None:
                        paid_date = closest_lead['date_created'].item().date().strftime("%d.%m.%Y")
                        roistat_url = closest_lead['roistat_url'].item()
                        updated_last_paid_lead.append(paid_date)
                        updated_target_url.append(roistat_url)
                    else:
                        index = paid_list.index(date_item)
                        updated_last_paid_lead.append(row['last_paid_lead'][index])
                        updated_target_url.append(
                            row['target_url'][index] if self.check_url_channel(row['target_url'][index],
                                                                               paid_urls) else 'Undefined')
                else:
                    if not updated_last_paid_lead:
                        index = paid_list.index(date_item)
                        updated_last_paid_lead.append(row['last_paid_lead'][index])
                        updated_target_url.append(
                            row['target_url'][index] if self.check_url_channel(row['target_url'][index],
                                                                               paid_urls) else 'Undefined')
                    else:
                        updated_last_paid_lead.append(updated_last_paid_lead[-1])
                        updated_target_url.append(updated_target_url[-1])

            row['last_paid_lead'] = updated_last_paid_lead
            row['target_url'] = updated_target_url
            return row

    def get_remote_table(self) -> pandas.DataFrame:
        logger.info("  ↳ Getting remote table")
        sheets_api = SheetsAPIClient()
        worksheet = sheets_api.payments_analytic.worksheet("Все оплаты")
        values = worksheet.get_all_values()
        data = pandas.DataFrame(data=values[1:], columns=values[0])
        data.index = range(2, 2 + len(data))
        new_column_names = {
            'Почта': 'email',
            'Ссылка на amocrm ': 'amocrm_url',
            'Дата оплаты': 'paid_date',
            'Месяц / Доплата': 'paid_type',
            'Курс': 'course',
            'Целевая ссылка': 'target_url',
            'Дата последней заявки (платной)': 'last_paid_lead'
        }
        data = data.rename(columns=new_column_names)
        data = data[["email", "amocrm_url", "paid_date", "paid_type", "course", "target_url", 'last_paid_lead']]
        data['paid_date'] = pandas.to_datetime(data['paid_date'], format='%d.%m.%Y').dt.date
        logger.info("  ↳ Remote table getted")
        return data

    def update_table(self, remote: pandas.DataFrame) -> pandas.DataFrame:
        logger.info("  ↳ Updating table")

        # Изъяли amocrm id из amocrm_url
        remote['amocrm_id'] = remote['amocrm_url'].apply(self.detect_amocrm_id)
        logger.info("    ↳ AmoCRM id detected")

        # Получили все контакты
        contacts = list(AmocrmContact.objects.all().values('amocrm_id', 'email'))
        for contact in contacts:
            contact['email'] = contact['email'].lower()
        logger.info("   ↳ Contacts was get, count: %(quantity)d" % {"quantity": len(contacts)})

        # Определили почту amocrm, по amocrm_id
        remote['amo_email'] = remote['amocrm_id'].apply(self.detect_amocrm_email, args=(contacts,))
        logger.info("    ↳ AmoCRM email detected")

        # Сделали уникальный список почт для дальнейшей фильтрации целевых лидов
        unique_emails = list(filter(None, set(remote['amo_email'].tolist())))

        # Получили список платных url
        paid_urls = list(LandingPage.objects.filter(paid=True).values_list("url", flat=True))
        logger.info("   ↳ Paid urls was get, count: %(quantity)d" % {"quantity": len(paid_urls)})

        # Получили все лиды, почта которых имеется в нашем списке
        leads_data = queryset_as_dataframe(Lead.objects.all())
        leads_data['email'] = leads_data['email'].str.lower()
        leads_df = leads_data[leads_data['email'].isin(unique_emails)]
        leads_df.loc[:, 'date_created'] = pandas.to_datetime(leads_df['date_created'], utc=True).dt.tz_convert(
            'Europe/Moscow').dt.date
        logger.info("   ↳ Targeted Leads was get, count: %(quantity)d" % {"quantity": leads_df.shape[0]})

        # Группируем dataframe для обработки, случай -"несколько оплат с почты"
        remote = remote.groupby(['amo_email', 'course']).agg({
            'email': list, 'amocrm_url': list, 'paid_date': list, 'paid_type': list, 'target_url': list,
            'last_paid_lead': list, 'amocrm_id': list}).reset_index()

        # проходимся по каждой строке и определяем последний платный лид и url
        remote = remote.apply(self.update_lead_and_url, args=(leads_df, paid_urls), axis=1)

        # Выворачиваем обратно df
        groupby_columns = ['amo_email', 'course']
        degrouped_remote = remote.set_index(groupby_columns)
        degrouped_remote = degrouped_remote.apply(pandas.Series.explode)
        degrouped_remote = degrouped_remote.reset_index()
        degrouped_remote['target_url'] = degrouped_remote.apply(
            lambda row: 'Undefined' if not self.check_url_channel(row['target_url'], paid_urls) else row['target_url'],
            axis=1)

        logger.info("    ↳ Last_paid_lead and Roistat_url  detected")
        logger.info("  ↳ Table was updated")

        return degrouped_remote

    def update_remote_table(self, df: pandas.DataFrame):
        logger.info("  ↳ Updating remote table")

        # ориг_remote
        original_sheets_api = SheetsAPIClient()
        original_worksheet = original_sheets_api.payments_analytic.worksheet("Все оплаты")
        original_values = original_worksheet.get_all_values()
        original_data = pandas.DataFrame(data=original_values[1:], columns=original_values[0])
        original_data.index = range(2, 2 + len(original_data))
        original_data['Дата оплаты'] = pandas.to_datetime(original_data['Дата оплаты'], format='%d.%m.%Y').dt.strftime(
            '%Y-%m-%d')

        # обновленный_внутр
        df = df.astype(str)
        new_column_names = {
            'email': 'Почта',
            'amocrm_url': 'Ссылка на amocrm ',
            'paid_date': 'Дата оплаты',
            'paid_type': 'Месяц / Доплата',
            'course': 'Курс',
            'target_url': 'Целевая ссылка',
            'last_paid_lead': 'Дата последней заявки (платной)'
        }
        df = df.rename(columns=new_column_names)

        # склейка_remote_и_внутр
        merged_df = pandas.merge(original_data, df[
            ['Почта', 'Ссылка на amocrm ', 'Курс', 'Месяц / Доплата', 'Дата оплаты', 'Целевая ссылка',
             'Дата последней заявки (платной)']],
                                 on=['Почта', 'Ссылка на amocrm ', 'Курс', 'Месяц / Доплата', 'Дата оплаты'],
                                 how='left', suffixes=('', '_new'))
        merged_df['Дата оплаты'] = pandas.to_datetime(merged_df['Дата оплаты'], format='%Y-%m-%d').dt.strftime(
            '%d.%m.%Y')

        # копия_remote
        sheets_api = SheetsAPIClient()
        worksheet = sheets_api.payments_copy.worksheet("Все оплаты")
        worksheet.clear()
        worksheet.update([merged_df.columns.values.tolist()] + merged_df.values.tolist())

        logger.info("  ↳ Remote table was updated")

    def handle(self, **kwargs):
        logger.info("Update payment analytic start")

        remote_table: pandas.DataFrame = self.get_remote_table()
        updated_table: pandas.DataFrame = self.update_table(remote_table)
        self.update_remote_table(updated_table)

        logger.info("Update payment analytic finish")
