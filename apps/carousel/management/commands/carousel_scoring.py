import html
import datetime

from typing import Dict, Any
from logging import getLogger
from urllib.parse import urlparse, parse_qsl

from django.utils import timezone
from django.db.models.query_utils import Q

from apps.choices import CarouselStatus
from apps.carousel.models import Carousel, ScoringUrl, ScoringGroup
from apps.sources.models import TildaLead
from apps.sources.management.commands._base import BaseCommand
from apps.utils import detect_channel_by_querystring


logger = getLogger(__name__)

score_map_days = {
    0: 20,
    1: 15,
    2: 12,
    3: 6,
    7: 1,
    14: 0,
    21: -15,
    28: -25,
    32: -50,
}

score_map_channel = {
    "youtube": 10,
    "tg": 5,
    "direct": 5,
}


class Command(BaseCommand):
    help = "Скоринг лидов"

    def score_value(self, value: str, score: int) -> Dict[str, Any]:
        return {
            "value": value,
            "score": score,
        }

    def score_qa(self, score_map_qa: dict, num: str, value: str) -> int:
        return self.score_value(
            str(value), int(score_map_qa.get(num, {}).get(value, 0))
        )

    def score_date(self, value: datetime.datetime) -> int:
        diff = (timezone.now() - value).days
        value = -60
        for days, score in score_map_days.items():
            if diff <= days:
                value = score
                break
        return self.score_value(str(diff), int(value))

    def score_channel(self, value: str) -> Dict[str, Any]:
        parse_url = urlparse(html.unescape(value))
        params_url = detect_channel_by_querystring(
            dict(parse_qsl(parse_url.query))
        )
        url = "https://" + parse_url.netloc + parse_url.path
        detect_group = ScoringUrl.objects.filter(url=url).values()
        if detect_group:
            score = score_map_channel.get(params_url, 0)
        elif "baza" in url:
            score = -15
        else:
            score = score_map_channel.get(params_url, 0)
        return self.score_value(str(url), int(score))

    def score_map_detect(self, value: str) -> dict:
        parse_url = urlparse(html.unescape(value))
        url = "https://" + parse_url.netloc + parse_url.path
        if "baza" in url:
            scoring_group = ScoringGroup.objects.filter(
                name="База оффер"
            ).first()
        else:
            scoring_group = (
                ScoringGroup.objects.filter(Q(urls__url=url) | Q(default=True))
                .order_by("default")
                .first()
            )
        if scoring_group:
            return scoring_group.scoring_map
        return {}

    def handle(self, **kwargs):
        logger.info("Carousel scoring")

        instances = []
        leads = TildaLead.objects.select_related("carousel").filter(
            carousel__status__in=[
                CarouselStatus.new.name,
                CarouselStatus.distributed.name,
            ]
        )
        for lead in leads:
            if all(getattr(lead, f"qa_{i}", "") == "" for i in range(1, 6)):
                score = {"no_answers": self.score_value("", 100)}
            else:
                score_map_qa = self.score_map_detect(lead.roistat_url)
                score = {
                    "qa_1": self.score_qa(score_map_qa, "1", lead.qa_1),
                    "qa_2": self.score_qa(score_map_qa, "2", lead.qa_2),
                    "qa_3": self.score_qa(score_map_qa, "3", lead.qa_3),
                    "qa_4": self.score_qa(score_map_qa, "4", lead.qa_4),
                    "qa_5": self.score_qa(score_map_qa, "5", lead.qa_5),
                    "date": self.score_date(lead.date_created),
                    "channel": self.score_channel(lead.roistat_url),
                }
            instance = lead.carousel
            instance.status = CarouselStatus.distributed.name
            instance.score = sum(value.get("score") for value in score.values())
            instance.score_info = score
            instance.updated = timezone.now()
            instances.append(instance)

        Carousel.objects.bulk_update(
            instances,
            ["status", "score", "score_info", "updated"],
            batch_size=1000,
        )
