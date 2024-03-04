from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
import uuid as _uuid

from apps.sources.managers import GoogleSheetManager


def default_uuid_slice20() -> str:
    return _uuid.uuid4().hex[:20]


class Group(models.Model):
    name = models.CharField(max_length=255, unique=True, verbose_name="Группа")
    probability = models.FloatField(
        verbose_name="Вероятность выпадения",
        validators=[
            MinValueValidator(
                0.0, message="Вероятность не может быть меньше 0"
            ),
            MaxValueValidator(
                1.0, message="Вероятность не может быть больше 1"
            ),
        ],
    )

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Группа"
        verbose_name_plural = "Группы"
        db_table = "roulette_group_data"
        ordering = ("name",)


class Bonus(models.Model):
    name = models.CharField(
        max_length=255,
        verbose_name="Название",
    )
    price = models.PositiveIntegerField(verbose_name="Стоимость")
    group = models.ForeignKey(
        Group, on_delete=models.CASCADE, verbose_name="Группа"
    )
    probability = models.FloatField(
        verbose_name="Вероятность выпадения",
        validators=[
            MinValueValidator(
                0.0, message="Вероятность не может быть меньше 0"
            ),
            MaxValueValidator(
                1.0, message="Вероятность не может быть больше 1"
            ),
        ],
    )

    @property
    def title_short(self):
        return f"{self.name} {self.price} руб."

    def __str__(self):
        return f"{self.title_short} | {self.probability} %"

    class Meta:
        verbose_name = "Бонус"
        verbose_name_plural = "Бонусы"
        db_table = "roulette_bonus_data"
        ordering = ("name",)


class Roulette(models.Model):
    created = models.DateTimeField("Создано", auto_now_add=True, editable=False)
    updated = models.DateTimeField("Обновлено", auto_now=True, editable=False)
    uuid = models.CharField(
        primary_key=True,
        max_length=20,
        editable=False,
        default=default_uuid_slice20,
    )
    name = models.CharField(max_length=255, verbose_name="Название")
    bonus = models.ManyToManyField(Bonus)
    google_table = models.ForeignKey(
        "GoogleSheet",
        on_delete=models.PROTECT,
        verbose_name="google spreadsheet",
    )

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Рулетка"
        verbose_name_plural = "Рулетки"
        db_table = "roulette_data"
        ordering = ("name",)


class GoogleSheet(models.Model):
    title = models.CharField(verbose_name="Заголовок", max_length=128)
    key = models.CharField(verbose_name="Ключ", max_length=64, unique=True)

    objects = GoogleSheetManager()

    class Meta:
        verbose_name = "Google таблица"
        verbose_name_plural = "Google таблицы"
        db_table = "roulette_google_sheets"
        ordering = ("title",)

    def __str__(self):
        return str(self.title)
