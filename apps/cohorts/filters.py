import pytz
import datetime

from typing import Dict
from django.conf import settings

from apps.utils import queryset_as_dataframe, slugify
from apps.cohorts.utils import detect_week
from apps.choices import UserGroup
from apps.filters import no_filter
from apps.traffic.models import Channel
from apps.sources.models import PaymentAnalytic
from apps.datatable.filters import dataframe_filter, DataframeFilterSet

from plugins.data import data_reader


def choice_group():
    df = data_reader.dataframe("groups.pkl")
    data = [(v, f"Группа {v}") for v in sorted(df.group.unique())]
    return [("", "--- Выберите ---")] + data


def choice_manager(*args, **kwargs):
    df = data_reader.dataframe("groups.pkl")
    data = [(v, v) for v in sorted(df.manager.unique())]
    return [("", "--- Выберите ---")] + data


def choice_channel_traffic(*args, **kwargs):
    df = data_reader.dataframe("channels.pkl")
    data = [(v, v) for v in sorted(df.account_title.unique())]
    return [("", "--- Выберите ---")] + data


def expenses_group_choices(*args, **kwargs):
    data = list(UserGroup.choices())
    return [("", "--- Выберите ---"), ("undefined", "Undefined")] + data


def expenses_manager_choices(*args, **kwargs):
    payments = queryset_as_dataframe(PaymentAnalytic.objects.all())
    data = [(slugify(item), item) for item in payments["manager"].unique()]
    return [("", "--- Выберите ---"), ("undefined", "Undefined")] + data


def expenses_channel_choices(*args, **kwargs):
    data = list(
        (item, item) for item in Channel.objects.values_list("value", flat=True)
    )
    return [("", "--- Выберите ---"), ("Undefined", "Undefined")] + data


###
# Filter lookup_expr
###


def expenses_date(dataframe, name, value, exclude):
    dataframe = dataframe[dataframe["date"] >= detect_week(value)[0]]
    return dataframe


###
# Filter fields
###


class CohortsFilter(DataframeFilterSet):
    date_from = dataframe_filter.DateFilter(
        label="С даты", lookup_expr=no_filter
    )
    group = dataframe_filter.TypedChoiceFilter(
        label="Группа", choices=choice_group, lookup_expr=no_filter
    )
    manager = dataframe_filter.TypedChoiceFilter(
        label="Менеджер", choices=choice_manager, lookup_expr=no_filter
    )
    channel_traffic = dataframe_filter.TypedChoiceFilter(
        label="Канал трафика",
        choices=choice_channel_traffic,
        lookup_expr=no_filter,
    )


class ExpensesFilter(DataframeFilterSet):
    date = dataframe_filter.DateFilter(
        label="С даты",
        lookup_expr=no_filter,
    )
    group = dataframe_filter.TypedChoiceFilter(
        label="Группа",
        choices=expenses_group_choices,
        lookup_expr=no_filter,
    )
    manager = dataframe_filter.TypedChoiceFilter(
        label="Менеджер",
        choices=expenses_manager_choices,
        lookup_expr=no_filter,
    )
    channel = dataframe_filter.TypedChoiceFilter(
        label="Канал трафика",
        choices=expenses_channel_choices,
        lookup_expr=no_filter,
    )
    accumulative = dataframe_filter.BooleanFilter(
        label="Накопительно",
        lookup_expr=no_filter,
    )
    profit = dataframe_filter.BooleanFilter(
        label="Показать в процентах",
        lookup_expr=no_filter,
    )

    def __init__(self, *args, **kwargs):
        default = {
            "date": str(
                (
                    datetime.datetime.now(
                        pytz.timezone(settings.ANALYTIC_TIME_ZONE)
                    )
                    - datetime.timedelta(weeks=10)
                ).date()
            )
        }
        super().__init__(default, *args, **kwargs)

    def set_groups_choices(self, choices: Dict[str, str]):
        self.form.fields["group"].choices = [("", "--- Выберите ---")] + list(
            choices.items()
        )

    def set_managers_choices(self, choices: Dict[str, str]):
        self.form.fields["manager"].choices = [("", "--- Выберите ---")] + list(
            choices.items()
        )

    def set_channels_choices(self, choices: Dict[str, str]):
        self.form.fields["channel"].choices = [("", "--- Выберите ---")] + list(
            choices.items()
        )


class TraficOffersFilter(DataframeFilterSet):
    lead_df = dataframe_filter.DateFilter(
        label="Трафик с", lookup_expr=no_filter
    )
    lead_dt = dataframe_filter.DateFilter(
        label="Трафик до", lookup_expr=no_filter
    )
    type_all = dataframe_filter.BooleanFilter(
        label="Все", lookup_expr=no_filter
    )
    type_intensiv2 = dataframe_filter.BooleanFilter(
        label="Интенсив 2 дня", lookup_expr=no_filter
    )
    type_intensiv3 = dataframe_filter.BooleanFilter(
        label="Интенсив 3 дня", lookup_expr=no_filter
    )
    type_gpt_5lesson = dataframe_filter.BooleanFilter(
        label="ChatGPT.Курс 5 уроков", lookup_expr=no_filter
    )
    type_gpt_vebinar = dataframe_filter.BooleanFilter(
        label="ChatGPT.Вебинар", lookup_expr=no_filter
    )
    type_neirostaff = dataframe_filter.BooleanFilter(
        label="Нейростафф", lookup_expr=no_filter
    )
    type_ai_7lesson = dataframe_filter.BooleanFilter(
        label="Курс AI. 7 уроков", lookup_expr=no_filter
    )
    show_romi = dataframe_filter.BooleanFilter(
        label="Показать ROMI", lookup_expr=no_filter
    )
    cumulative = dataframe_filter.BooleanFilter(
        label="Накопительно", lookup_expr=no_filter
    )
