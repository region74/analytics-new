import pytz
import datetime

from typing import Tuple

from django.conf import settings
from django.urls import reverse_lazy

from django_filters import filters

from apps.datatable.forms import FilterFormPost
from apps.datatable.filters import fields as filters_fields
from apps.datatable.filters import widgets as filters_widgets
from apps.filters import no_filter
from apps.choices import LeadLevel
from apps.sources.models import TildaLead
from apps.datatable.filters import (
    dataframe_filter,
    ModelFilterSet,
    DataframeFilterSet,
)

from .forms import IPLReportFilterForm


def filter_mode() -> Tuple[Tuple[str, str], ...]:
    return ("analyze", "Без загрузки"), ("upload", "Загрузить")


def ipl_report_date_from(dataframe, name, value, exclude):
    dataframe = dataframe[dataframe["date"] >= value]
    return dataframe


def ipl_report_date_to(dataframe, name, value, exclude):
    dataframe = dataframe[dataframe["date"] <= value]
    return dataframe


def channels_payment_date_from(dataframe, name, value, exclude):
    dataframe = dataframe[dataframe["date_payment"] >= value]
    return dataframe


def channels_payment_date_to(dataframe, name, value, exclude):
    dataframe = dataframe[dataframe["date_payment"] <= value]
    return dataframe


def channels_expenses_date_from(dataframe, name, value, exclude):
    dataframe = dataframe[dataframe["date"] >= value]
    return dataframe


def channels_expenses_date_to(dataframe, name, value, exclude):
    dataframe = dataframe[dataframe["date"] <= value]
    return dataframe


class ChannelsMonthsWidget(filters_widgets.Html):
    template_name = "traffic/channels/widgets/months.html"

    def get_context(self, name, value, attrs):
        context = super().get_context(name, value, attrs)
        cyr_month = [
            "январь",
            "февраль",
            "март",
            "апрель",
            "май",
            "июнь",
            "июль",
            "август",
            "сентябрь",
            "октябрь",
            "ноябрь",
            "декабрь",
        ]
        date = datetime.datetime.now()
        year = date.year
        months = list(
            map(
                lambda item: ("%i-%02i" % (year, item), cyr_month[item - 1]),
                range(1, date.month + 1),
            )
        )
        context.update({"months": months})
        return context


class ChannelsMonthsField(filters_fields.HtmlField):
    widget = ChannelsMonthsWidget


class ChannelsMonthsFilter(dataframe_filter.HtmlFilter):
    field_class = ChannelsMonthsField


class LeadsFilter(ModelFilterSet):
    tranid = filters.CharFilter(label="Tran ID", lookup_expr="icontains")
    email = filters.CharFilter(label="E-mail", lookup_expr="icontains")
    utm_source = filters.CharFilter(label="UTM source", lookup_expr="icontains")

    class Meta(ModelFilterSet.Meta):
        model = TildaLead
        fields = ["tranid", "email", "utm_source"]


class IPLReportFilter(DataframeFilterSet):
    date_from = dataframe_filter.DateFilter(
        label="С даты", lookup_expr=ipl_report_date_from
    )
    date_to = dataframe_filter.DateFilter(
        label="По дату", lookup_expr=ipl_report_date_to
    )
    account = dataframe_filter.TypedChoiceAjaxFilter(
        label=LeadLevel.account.value,
        coerce=int,
        url=reverse_lazy("api:v1:select_ajax:account"),
    )
    campaign = dataframe_filter.TypedChoiceAjaxFilter(
        label=LeadLevel.campaign.value,
        coerce=int,
        url=reverse_lazy("api:v1:select_ajax:campaign"),
    )
    group = dataframe_filter.TypedChoiceAjaxFilter(
        label=LeadLevel.group.value,
        coerce=int,
        url=reverse_lazy("api:v1:select_ajax:group"),
    )
    groupby = dataframe_filter.TypedChoiceFilter(
        label="Группировать по",
        choices=LeadLevel.choices(),
        lookup_expr=no_filter,
    )
    russia = dataframe_filter.BooleanFilter(
        label="Только Россия", lookup_expr=no_filter
    )

    class Meta(DataframeFilterSet.Meta):
        form = IPLReportFilterForm


class ChannelsFilter(DataframeFilterSet):
    expenses_date_from = dataframe_filter.DateFilter(
        label="Расход с даты", lookup_expr=channels_expenses_date_from
    )
    expenses_date_to = dataframe_filter.DateFilter(
        label="Расход по дату", lookup_expr=channels_expenses_date_to
    )
    payment_date_from = dataframe_filter.DateFilter(
        label="Оплата с даты", lookup_expr=channels_payment_date_from
    )
    payment_date_to = dataframe_filter.DateFilter(
        label="Оплата по дату", lookup_expr=channels_payment_date_to
    )
    months = ChannelsMonthsFilter(label="", lookup_expr=no_filter)

    def __init__(self, *args, **kwargs):
        date_initial = (
            datetime.datetime.now(pytz.timezone(settings.ANALYTIC_TIME_ZONE))
            - datetime.timedelta(weeks=4)
        ).date()
        default = {
            "expenses_date_from": str(date_initial),
            "payment_date_from": str(date_initial),
        }
        super().__init__(default, *args, **kwargs)


class FunnelsFilter(DataframeFilterSet):
    lead_df = dataframe_filter.DateFilter(
        label="Дата заявки с", lookup_expr=no_filter
    )
    lead_dt = dataframe_filter.DateFilter(
        label="Дата заявки до", lookup_expr=no_filter
    )
    payment_df = dataframe_filter.DateFilter(
        label="Дата оплаты с", lookup_expr=no_filter
    )
    payment_dt = dataframe_filter.DateFilter(
        label="Дата оплаты до", lookup_expr=no_filter
    )
    percent = dataframe_filter.BooleanFilter(
        label="Проценты", lookup_expr=no_filter
    )


class DoubleFilter(DataframeFilterSet):
    lead_df = dataframe_filter.DateFilter(
        label="Дата лида с", lookup_expr=no_filter
    )
    lead_dt = dataframe_filter.DateFilter(
        label="Дата лида до", lookup_expr=no_filter
    )
    lead_intensive_2days = dataframe_filter.BooleanFilter(
        label="Интенсив 2 дня", lookup_expr=no_filter
    )
    lead_intensive_3days = dataframe_filter.BooleanFilter(
        label="Интенсив 3 дня", lookup_expr=no_filter
    )
    lead_neirostaff = dataframe_filter.BooleanFilter(
        label="Нейростафф", lookup_expr=no_filter
    )
    lead_baza = dataframe_filter.BooleanFilter(
        label="База: Вебинары", lookup_expr=no_filter
    )
    lead_universe = dataframe_filter.BooleanFilter(
        label="Вселенная AI", lookup_expr=no_filter
    )
    lead_others = dataframe_filter.BooleanFilter(
        label="Прочие", lookup_expr=no_filter
    )


class UploadFilter(DataframeFilterSet):
    mode = dataframe_filter.RadioFilter(
        label="Метод обработки", choices=filter_mode
    )
    file = dataframe_filter.FileFilter(label="Файл", lookup_expr=no_filter)

    class Meta(DataframeFilterSet.Meta):
        form = FilterFormPost


class TelegramFilter(DataframeFilterSet):
    event_df = dataframe_filter.DateFilter(
        label="Мероприятия с", lookup_expr=no_filter
    )
    event_dt = dataframe_filter.DateFilter(
        label="Мероприятия до", lookup_expr=no_filter
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

    type_neirostaff = dataframe_filter.BooleanFilter(
        label="Нейростафф", lookup_expr=no_filter
    )

    type_baza = dataframe_filter.BooleanFilter(
        label="База: Вебинары", lookup_expr=no_filter
    )
