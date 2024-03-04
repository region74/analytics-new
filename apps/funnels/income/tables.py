from django_tables2.tables import columns

from apps.datatable.table import DataframeTable
from apps.datatable.forms import PerPageForm


class FunnelReportTable(DataframeTable):
    payment_all = columns.Column(verbose_name="Все оплаты за период")
    payment_event = columns.Column(verbose_name="Минимум 1 касание с воронкой")

    class Meta(DataframeTable.Meta):
        sequence = (
            "payment_all",
            "payment_event",
        )


class EventsReportTable(DataframeTable):
    date = columns.Column(verbose_name='Дата')
    event = columns.Column(verbose_name='Мероприятие')
    full_profit = columns.Column(verbose_name='Оборот общий')
    reg_profit = columns.Column(verbose_name='Оборот с регистраций')
    peop_profit = columns.Column(verbose_name='Оборот с участников')
    so_profit = columns.Column(verbose_name='Оборот с SO')
    percent_reg = columns.Column(verbose_name='% с регистраций')
    percent_peop = columns.Column(verbose_name='% с участников')
    percent_so = columns.Column(verbose_name='% с предзаказов')

    class Meta(DataframeTable.Meta):
        sequence = (
            "date",
            "event",
            "full_profit",
            "reg_profit",
            "peop_profit",
            "so_profit",
            "percent_reg",
            "percent_peop",
            "percent_so"
        )
