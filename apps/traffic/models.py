from django.db import models

from apps.choices import FunnelChannelUrlType

from . import managers


class FunnelChannelUrl(models.Model):
    url = models.CharField(verbose_name="URL", max_length=2048)
    group = models.CharField(
        verbose_name="Группа",
        max_length=16,
        choices=FunnelChannelUrlType.choices(),
    )

    objects = managers.FunnelChannelUrlManager()

    class Meta:
        verbose_name = "Url канала трафика"
        verbose_name_plural = "Url каналов трафика"
        db_table = "traffic_funnel_channel_url"
        ordering = ("-url",)

    def __str__(self):
        return f"[{self.group}] {self.url}"


class LandingPage(models.Model):
    url = models.CharField(verbose_name="Url", max_length=2048)
    paid = models.BooleanField(verbose_name="Платный трафик", default=False)

    objects = managers.LandingPageManager()

    class Meta:
        verbose_name = "Посадочная страница"
        verbose_name_plural = "Посадочные страницы"
        db_table = "traffic_landing_pages"
        ordering = ("-url",)

    def __str__(self):
        return str(self.url)


class Channel(models.Model):
    key = models.CharField(
        verbose_name="Идентификатор", max_length=256, unique=True, blank=True
    )
    value = models.CharField(verbose_name="Значение", max_length=256)

    objects = managers.ChannelManager()

    class Meta:
        verbose_name = "Канал трафика"
        verbose_name_plural = "Каналы трафика"
        db_table = "traffic_channels"
        ordering = ("-key",)

    def __str__(self):
        return f"[{self.key}] {self.value}"
