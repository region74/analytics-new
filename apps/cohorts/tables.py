from django_tables2.tables import columns

from apps.datatable.renderer import Renderer
from apps.datatable.table import DataframeTable


class ZoomTable(DataframeTable):
    def __init__(self, *args, **kwargs):
        self._meta.sequence = [col for col in self.base_columns]
        super().__init__(*args, **kwargs)

    class Meta(DataframeTable.Meta):
        pass


class SpecialOffersTable(DataframeTable):
    def __init__(self, *args, **kwargs):
        self._meta.sequence = [col for col in self.base_columns]
        super().__init__(*args, **kwargs)

    class Meta(DataframeTable.Meta):
        pass


class ExpensesTable(DataframeTable):
    date_from = columns.Column(verbose_name="С даты")
    date_to = columns.Column(verbose_name="По дату")
    value = columns.Column(verbose_name="Расход", orderable=False)
    sum = columns.Column(verbose_name="Сумма", orderable=False)

    class Meta(DataframeTable.Meta):
        template_name = "cohorts/expenses/table.html"
        sequence = (
            "date_from",
            "date_to",
            "value",
            "sum",
        )

    def render_date_from(self, value):
        return Renderer.date(value)

    def render_date_to(self, value):
        return Renderer.date(value)

    def render_value(self, value):
        return int(value)

    def render_sum(self, value):
        return int(value)


class TraficOffersTable(DataframeTable):
    channel = columns.Column(verbose_name="Оффер в разбивке по каналам")
    expenses = columns.Column(verbose_name="Расход за период")
    week1 = columns.Column(verbose_name="Оборот 1 неделя")
    week2 = columns.Column(verbose_name="Оборот 2 недели")
    week4 = columns.Column(verbose_name="Оборот 4 недели")
    week8 = columns.Column(verbose_name="Оборот 8 недель")

    class Meta(DataframeTable.Meta):
        sequence = (
            "channel",
            "expenses",
            "week1",
            "week2",
            "week4",
            "week8",
        )

    def render_expenses(self, value):
        return Renderer.int(value)

    def render_week1(self, value):
        return Renderer.int(value)

    def render_week2(self, value):
        return Renderer.int(value)

    def render_week4(self, value):
        return Renderer.int(value)

    def render_week8(self, value):
        return Renderer.int(value)
