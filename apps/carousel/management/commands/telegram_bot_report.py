import datetime
import html
import time
import pandas
import requests

from collections import Counter
from logging import getLogger
from urllib.parse import urlparse

from django.db.models import Avg

from apps.carousel.models import Carousel
from apps.choices import CarouselStatus
from apps.utils import queryset_as_dataframe
from apps.sources.models import TildaLead, Lead
from config import settings

from apps.sources.management.commands._base import BaseCommand

logger = getLogger(__name__)

TELEGRAM_BOT_API_TOKEN = settings.TELEGRAM_BOT_API_TOKEN
TELEGRAM_BOT_GROUP_CHAT_ID = settings.TELEGRAM_BOT_GROUP_CHAT_ID
BOT_URL = f'https://api.telegram.org/bot{TELEGRAM_BOT_API_TOKEN}/sendMessage'


class Command(BaseCommand):
    help = "Отправка отчета в телеграм бота"

    def get_thursday(self, current_date: datetime.date):
        days_to_subtract = (current_date.weekday() - 3) % 7
        thursday = current_date - datetime.timedelta(days=days_to_subtract)
        return thursday

    def get_wednesday(self, current_date: datetime.date):
        days_to_add = (2 - current_date.weekday()) % 7
        wednesday = current_date + datetime.timedelta(days=days_to_add)
        return wednesday

    def parse_url(self, value: str) -> bool:
        parse_url = urlparse(html.unescape(value))
        url = parse_url.netloc + parse_url.path
        return 'baza' in url

    def first_report(self, df: datetime.datetime, dt: datetime.datetime) -> str:
        logger.info("  ↳Create first report")
        leads = queryset_as_dataframe(TildaLead.objects.filter(date_created__range=(df, dt)))
        leads['category'] = leads['roistat_url'].apply(self.parse_url)
        distribution = list(Carousel.objects.filter(created__range=(df, dt), distribution__range=(df, dt),
                                                    status__in=[CarouselStatus.complete.name,
                                                                CarouselStatus.qualified.name,
                                                                CarouselStatus.unqualified.name]).values_list(
            'owner__email', flat=True))
        qualified_count = Carousel.objects.filter(created__range=(df, dt), distribution__range=(df, dt),
                                                  status=CarouselStatus.qualified.name).count()
        unqualified_count = Carousel.objects.filter(created__range=(df, dt), distribution__range=(df, dt),
                                                    status=CarouselStatus.unqualified.name).count()
        avg_score_count = round(
            Carousel.objects.filter(created__range=(df, dt), distribution__range=(df, dt)).aggregate(Avg('score'))[
                'score__avg'])

        full_count = len(leads)
        baza_count = leads['category'].sum()
        paid_count = full_count - baza_count
        distribution_count = len(distribution)
        openers_count = (len(set(distribution)))
        lead_per_opener_count = round(distribution_count / openers_count)
        report_row = f'Отчет №1. Количество распределенных за вчера.\n\nКоличество пришедших лидов общее: {full_count}\nКоличество пришедших лидов база: {baza_count}\nКоличество пришедших лидов платный трафик: {paid_count}\nКоличество опенеров на смене: {openers_count}\nКоличество распределенных лидов: {distribution_count}\nКоличество распределенных на 1 опенера: {lead_per_opener_count}\nКоличество квал.лидов: {qualified_count}\nКоличество неквал.лидов: {unqualified_count}\nСредний балл распределенных лидов: {avg_score_count}\n'
        return report_row

    def second_report(self, df: datetime.datetime, dt: datetime.datetime) -> str:
        logger.info("  ↳Create second report")
        queryset = list(Carousel.objects.filter(created__range=(df, dt), distribution__range=(df, dt),
                                                status__in=[CarouselStatus.complete.name,
                                                            CarouselStatus.qualified.name,
                                                            CarouselStatus.unqualified.name]).values_list(
            'lead__roistat_url', 'status'))
        distribution = pandas.DataFrame(queryset, columns=['url', 'status'])
        distribution['category'] = distribution['url'].apply(self.parse_url)

        baza_count = len(distribution[(distribution['category'] == True)])
        baza_qualified_count = len(
            distribution[(distribution['category'] == True) & (distribution['status'] == 'qualified')])
        baza_unqualified_count = len(
            distribution[(distribution['category'] == True) & (distribution['status'] == 'unqualified')])

        paid_count = len(distribution[(distribution['category'] == False)])
        paid_qualified_count = len(
            distribution[(distribution['category'] == False) & (distribution['status'] == 'qualified')])
        paid_unqualified_count = len(
            distribution[(distribution['category'] == False) & (distribution['status'] == 'unqualified')])

        result_row = f'Отчет №2. Количество квал.лидов по каналам.\nОбщее\Квал\Неквал\n\nAI+GPT каналы: {paid_count}/{paid_qualified_count}/{paid_unqualified_count}\nBaza каналы: {baza_count}/{baza_qualified_count}/{baza_unqualified_count}\n'

        return result_row

    def third_report(self, df: datetime.datetime, dt: datetime.datetime) -> str:
        logger.info("  ↳Create third report")
        score_gte30_yesterday = Carousel.objects.filter(
            created__range=(df, dt), score__gte=30,
            status__in=[CarouselStatus.new.name, CarouselStatus.distributed.name]
        ).values_list('lead__email', flat=True)

        score_lte29_yesterday = Carousel.objects.filter(
            created__range=(df, dt), score__lte=29,
            status__in=[CarouselStatus.new.name, CarouselStatus.distributed.name]
        ).values_list('lead__email', flat=True)

        current_date = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        thursday = self.get_thursday(current_date)
        wednesday = self.get_wednesday(current_date)

        score_gte30_segment = Carousel.objects.filter(
            created__range=(thursday, wednesday), score__gte=30,
            status__in=[CarouselStatus.new.name, CarouselStatus.distributed.name]
        ).values_list('lead__email', flat=True)

        score_lte29_segment = Carousel.objects.filter(
            created__range=(thursday, wednesday), score__lte=29,
            status__in=[CarouselStatus.new.name, CarouselStatus.distributed.name]
        ).values_list('lead__email', flat=True)

        db_email_list = list(Lead.objects.values_list("email", flat=True))
        email_counter = Counter(db_email_list)

        double_gte30_yesterday = sum(email_counter[email] > 1 for email in score_gte30_yesterday)
        double_lte29_yesterday = sum(email_counter[email] > 1 for email in score_lte29_yesterday)

        double_gte30_segment = sum(email_counter[email] > 1 for email in score_gte30_segment)
        double_lte29_segment = sum(email_counter[email] > 1 for email in score_lte29_segment)

        result_row = f'Отчет №3. Состав хвоста.\n\nВчерашний день:\nКоличество лидов с суммой баллов до 29: {score_lte29_yesterday.count()}\nИз них количество дублей: {double_lte29_yesterday}\nКоличество лидов с суммой баллов от 30: {score_gte30_yesterday.count()}\nИз них количество дублей: {double_gte30_yesterday}\n\nПериод с {thursday.date()} по {wednesday.date()}:\nКоличество лидов с суммой баллов до 29: {score_lte29_segment.count()}\nИз них количество дублей: {double_lte29_segment}\nКоличество лидов с суммой баллов от 30: {score_gte30_segment.count()}\nИз них количество дублей: {double_gte30_segment}'
        return result_row

    def send_telegram_message(self, text: str):
        data = {
            'chat_id': TELEGRAM_BOT_GROUP_CHAT_ID,
            'text': text
        }
        try:
            response = requests.post(BOT_URL, data=data)
            response.raise_for_status()
            logger.info("Telegram message sent successfully")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send Telegram message: {e}")

    def handle(self, **kwargs):
        logger.info("Telegram reporting start")
        today = datetime.datetime.now()
        df = (today - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0,
                                                          tzinfo=datetime.timezone.utc)
        dt = today.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)

        reports = [
            {'func': self.first_report, 'error_msg': 'Ошибка отчета №1'},
            {'func': self.second_report, 'error_msg': 'Ошибка отчета №2'},
            {'func': self.third_report, 'error_msg': 'Ошибка отчета №3'},
        ]

        for report_info in reports:
            try:
                report_result = report_info['func'](df, dt)
            except Exception as e:
                report_result = report_info['error_msg']
            self.send_telegram_message(report_result)
            time.sleep(4)

        logger.info("Sending reports")
