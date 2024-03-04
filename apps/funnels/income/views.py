import pandas

from apps.views.mixins import LPRequiredMixin
from .filters import FunnelFilter, EventsFilter

from .functions import get_data, get_report
from .tables import FunnelReportTable, EventsReportTable
from ...datatable.base import DatatableDataframeView


class FunnelView(LPRequiredMixin, DatatableDataframeView):
    template_name = "funnels/income/funnel.html"
    page_title = "Оборот с воронки"
    permission_required = ("core.page_view_funnels_income_funnel",)
    table_pagination = False
    table_class = FunnelReportTable
    filterset_class = FunnelFilter

    def get_data(self) -> pandas.DataFrame:
        tmp: pandas.DataFrame = pandas.DataFrame(
            columns=["payment_all", "payment_event"]
        )
        rows = [0, 0]
        tmp.loc[len(tmp)] = rows
        return tmp

    def update_filters(self):
        if hasattr(self.filterset.form, "cleaned_data"):
            date_from = self.filterset.form.cleaned_data.get("payment_from")
            date_to = self.filterset.form.cleaned_data.get("payment_to")
            event_from = self.filterset.form.cleaned_data.get("event_from")
            event_to = self.filterset.form.cleaned_data.get("event_to")
            if date_from and date_to and event_from and event_to:
                result = get_data(date_from, date_to, event_from, event_to)
                return result

    def prepare_table(self, data: pandas.DataFrame) -> pandas.DataFrame:
        result = self.update_filters()
        if result:
            return pandas.DataFrame.from_dict(result, orient="index").T
        return data


class EventsView(LPRequiredMixin, DatatableDataframeView):
    page_title = "Оборот по мероприятиям"
    template_name = "funnels/income/events.html"
    permission_required = ("core.page_view_funnels_income_events",)
    table_pagination = False
    table_class = EventsReportTable
    filterset_class = EventsFilter

    def get_data(self) -> pandas.DataFrame:
        tmp: pandas.DataFrame = pandas.DataFrame(
            columns=[
                "date",
                "event",
                "full_profit",
                "reg_profit",
                "peop_profit",
                "so_profit",
                "percent_reg",
                "percent_peop",
                "percent_so",
            ]
        )
        rows = ["-", "-", "-", "-", "-", "-", "-", "-", "-"]
        tmp.loc[len(tmp)] = rows
        return tmp

    def update_filters(self):
        if hasattr(self.filterset.form, "cleaned_data"):
            result = get_report(self.filterset.form.cleaned_data)
            return result

    def prepare_table(self, data: pandas.DataFrame) -> pandas.DataFrame:
        new_data: pandas.DataFrame = self.update_filters()
        check = (
            new_data.to_dict(orient="records") if new_data is not None else None
        )
        if check:
            return new_data
        return data
