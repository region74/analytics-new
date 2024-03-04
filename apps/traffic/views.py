import io

import pytz
import pandas
import logging
import datetime
import requests

from typing import List, Dict, Any

from django.db.models import Q
from xlsxwriter import Workbook
from collections import Counter, defaultdict
from urllib.parse import urlparse, parse_qs

from django.conf import settings
from django.urls import reverse_lazy
from django.http import HttpResponseRedirect, FileResponse
from django.core.exceptions import ObjectDoesNotExist

from apps.utils import queryset_as_dataframe
from apps.choices import (
    LeadLevel,
    LeadQuizQuestionSN,
    RoistatDimensionType, FunnelChannelUrlType,
)
from apps.views.mixins import LPRequiredMixin
from apps.sources.models import (
    Lead,
    TildaLead,
    LeadQuizAnswer,
    PaymentAnalytic,
    RoistatDimension,
)
from apps.datatable.base import DatatableModelView, DatatableDataframeView
from apps.datatable.renderer import Renderer

from plugins.data import data_reader

from .models import LandingPage, Channel, FunnelChannelUrl

from .tables import (
    LeadsTable,
    IPLReportTable,
    ChannelsTable,
    FunnelsTable,
    DoubleTable,
    UploadLeadsTable,
    UploadLeadsDetailTable,
    TelegramReportTable,
)
from .filters import (
    LeadsFilter,
    IPLReportFilter,
    ChannelsFilter,
    FunnelsFilter,
    DoubleFilter,
    UploadFilter,
    TelegramFilter,
)
from .utils import (
    translate_channel,
    parse_url_params,
    detect_empty_params,
    detect_channel_from_params,
    parse_url,
    detect_pay_traffic,
    TildaLeadsParseData,
    HttpRequest,
    LeadAPIView,
    get_event,
    get_members_for_cr,
    get_regs_for_cr,
    get_subscriptions_for_cr,
)
from ..api.exceptions import APIException

ANALYTIC_TZ = pytz.timezone(settings.ANALYTIC_TIME_ZONE)
logger = logging.getLogger("django")


class LeadsView(LPRequiredMixin, DatatableModelView):
    template_name = "traffic/leads/index.html"
    page_title = "Tilda лиды"
    permission_required = ("core.page_view_traffic_leads",)
    model = TildaLead
    table_class = LeadsTable
    filterset_class = LeadsFilter


class IPLReportView(LPRequiredMixin, DatatableDataframeView):
    template_name = "traffic/ipl/index.html"
    page_title = "IPL по каналам"
    permission_required = ("core.page_view_traffic_ipl",)
    table_class = IPLReportTable
    filterset_class = IPLReportFilter

    def get_data(self) -> pandas.DataFrame:
        return data_reader.dataframe("ipl_report.pkl")

    def get_levels(self, groupby: str, keys: List[int]) -> Dict[int, str]:
        levels = data_reader.dict(f"ipl_report_level_{groupby}.json")
        result_levels = {}

        for key in keys:
            if str(key) in levels:
                title = levels[str(key)]
            else:
                try:
                    title = RoistatDimension.objects.get(id=key).title
                except ObjectDoesNotExist:
                    title = key
            result_levels[key] = title

        result_levels.update({0: "Undefined"})
        return result_levels

    def update_level(self, name: str, data: pandas.DataFrame):
        field = self.filterset.form.fields.get(name)
        data = {"keys": data[name].unique().tolist()}
        cleaned_data = getattr(self.filterset.form, "cleaned_data", {})
        selected = cleaned_data.get(name)
        data.update({"selected": None if selected == "" else selected})
        field.widget.data = data

    def update_filters(self):
        data = self.filterset.dataframe
        cleaned_data = getattr(self.filterset.form, "cleaned_data", {})

        date_from = cleaned_data.get("date_from")
        if date_from:
            data = data[data["date"] >= date_from]

        date_to = cleaned_data.get("date_to")
        if date_to:
            data = data[data["date"] <= date_to]

        self.update_level("account", data)

        account = cleaned_data.get("account")
        if account:
            data = data[data["account"] == account]

        self.update_level("campaign", data)

        campaign = cleaned_data.get("campaign")
        if campaign:
            data = data[data["campaign"] == campaign]

        self.update_level("group", data)

    def get_groupby(self) -> str:
        cleaned_data = getattr(self.filterset.form, "cleaned_data", {})
        groupby = (
                cleaned_data.get("groupby", LeadLevel.account.name)
                or LeadLevel.account.name
        )
        return groupby

    def get_leads(self) -> pandas.DataFrame:
        data = data_reader.dataframe("leads.pkl")
        cleaned_data = getattr(self.filterset.form, "cleaned_data", {})

        date_from = cleaned_data.get("date_from")
        if date_from:
            date_from = ANALYTIC_TZ.localize(
                datetime.datetime.combine(date_from, datetime.time.min)
            )
            data = data[data["created"] >= date_from]

        date_to = cleaned_data.get("date_to")
        if date_to:
            date_to = ANALYTIC_TZ.localize(
                datetime.datetime.combine(date_to, datetime.time.max)
            )
            data = data[data["created"] <= date_to]

        account = cleaned_data.get("account")
        if account:
            data = data[data["account"] == account]

        campaign = cleaned_data.get("campaign")
        if campaign:
            data = data[data["campaign"] == campaign]

        group = cleaned_data.get("group")
        if group:
            data = data[data["group"] == group]

        russia = cleaned_data.get("russia")
        if russia is True:
            russia_id = (
                LeadQuizAnswer.objects.select_related("question")
                .filter(question__sn=LeadQuizQuestionSN.q_1.name, name="Россия")
                .values_list("pk", flat=True)
                .first()
            )
            if russia_id is not None:
                data = data[data["qa_1"] == russia_id]
        return data

    def get_paid_leads(self) -> pandas.DataFrame:
        leads = self.get_leads()

        leads_db_queryset = Lead.objects.filter(pk__in=leads.index).values("id", "roistat_url")
        landings = list(LandingPage.objects.filter(paid=True).values_list("url", flat=True))

        leads_db_df = pandas.DataFrame.from_records(leads_db_queryset, index="id")
        leads_db_df["url"] = leads_db_df["roistat_url"].apply(parse_url)
        leads_db_df["url"] = leads_db_df["url"].apply(detect_pay_traffic, args=(landings,))

        paid_leads_ids = leads_db_df[leads_db_df["url"]].index

        leads = leads[leads.index.isin(paid_leads_ids)]
        return leads

    def prepare_table(self, data: pandas.DataFrame) -> pandas.DataFrame:
        self.update_filters()
        groupby = self.get_groupby()
        leads = self.get_paid_leads()

        leads_group_ids = set(leads[groupby].fillna(0))
        data_group_ids = set(data[groupby].fillna(0))
        all_group_ids = list(leads_group_ids | data_group_ids)

        levels = self.get_levels(groupby, all_group_ids)

        items = []
        total_income = 0
        total_quantity = 0
        total_expenses = 0

        for id_ in all_group_ids:

            if id_ == 0:
                leads_group = leads[leads[groupby].isna()]
                data_group = data[data[groupby].isna()]
            else:
                leads_group = leads[leads[groupby] == id_]
                data_group = data[data[groupby] == id_]

            quantity = len(leads_group)
            income = leads_group["ipl"].sum()
            ipl = income / quantity if quantity else 0
            expenses = data_group["expenses"].sum()
            romi = (income - expenses) / expenses if expenses else 0
            cpl = expenses / quantity if quantity else 0

            total_income += income
            total_quantity += quantity
            total_expenses += expenses

            items.append(
                {
                    "id": id_,
                    "title": levels.get(id_),
                    "leads": quantity,
                    "ipl": ipl,
                    "expenses": expenses,
                    "romi": romi,
                    "cpl": cpl,
                }
            )

        total_source = pandas.Series(
            {
                "actions": "",
                "title": "Итого",
                "leads": total_quantity,
                "ipl": total_income / total_quantity if total_quantity else 0,
                "expenses": total_expenses,
                "romi": (total_income - total_expenses) / total_expenses,
                "cpl": total_expenses / total_quantity if total_quantity else 0,
            }
        )
        self.extra_context = {
            "total_source": total_source,
            "total": pandas.Series(
                {
                    "actions": total_source.actions,
                    "title": total_source.title,
                    "leads": Renderer.int(total_source.leads),
                    "ipl": Renderer.money(total_source.ipl),
                    "expenses": Renderer.money(total_source.expenses),
                    "romi": Renderer.percent(total_source.romi),
                    "cpl": Renderer.money(total_source.cpl),
                }
            ),
        }
        return pandas.DataFrame(data=items)

    def download_ipl(self, workbook: Workbook):
        data = self.object_list[["title", "leads", "ipl", "expenses", "romi", "cpl"]]
        total = self.extra_context.get("total_source")
        total["ipl"] = round(total["ipl"])
        total["expenses"] = round(total["expenses"])
        total["cpl"] = round(total["cpl"])

        worksheet_titles = [
            self.table_class.base_columns.get(column_name).verbose_name
            for column_name in data.columns
        ]
        worksheet_total = [
            getattr(total, column_name) for column_name in data.columns
        ]

        worksheet = workbook.add_worksheet("Статистика")
        worksheet.write_row(0, 0, worksheet_titles)
        worksheet.write_row(1, 0, worksheet_total)

        for index, row in enumerate(data.values, 2):
            row = list(row)
            row[2] = round(row[2])
            row[3] = round(row[3])
            row[5] = round(row[5])
            worksheet.write_row(index, 0, row)
        worksheet.autofilter("A1:E1")

    def download_extra(self, workbook: Workbook, data: Dict[str, Any]):
        worksheet = workbook.add_worksheet(data.get("title"))
        for index, column in enumerate(data.get("columns0")):
            worksheet.merge_range(0, index * 3, 0, index * 3 + 2, column)
        worksheet.write_row(1, 0, data.get("columns1"))
        for index, row in enumerate(data.get("data"), 2):
            worksheet.write_row(index, 0, row)

    def download_leads(self, workbook: Workbook, data: Dict[str, Any]):
        worksheet = workbook.add_worksheet(
            f'{data.get("title")[:28]}{"..." if len(data.get("title")) > 31 else ""}'
        )
        worksheet.write_row(0, 0, data.get("columns"))
        for index, row in enumerate(data.get("data"), 1):
            row[0] = datetime.datetime.strptime(
                row[0], "%Y-%m-%dT%H:%M:%SZ"
            ).strftime("%d-%m-%Y %H:%M:%S")
            worksheet.write_row(index, 0, row)
        worksheet.autofilter("A1:J1")

    def download(self):
        output = io.BytesIO()
        workbook = Workbook(output)
        self.download_ipl(workbook)
        detail = self.request.GET.get("detail", None)

        scheme = self.request.scheme
        host = self.request.get_host()
        request_url = f"{scheme}://{host}/api/v1/ipl/detail/"
        if detail is not None:
            request_url += f"{detail}/"

        response = requests.post(request_url, json=dict(self.request.GET.items())).json()
        self.download_extra(workbook, response[0])
        self.download_leads(workbook, response[1])
        workbook.close()
        output.seek(0)
        return output

    def get(self, request, *args, **kwargs):
        response = super().get(request, *args, **kwargs)
        if "download" in self.request.GET.keys():
            response = FileResponse(
                self.download(),
                as_attachment=True,
                filename="statistics.xlsx",
            )
        return response


class ChannelsView(LPRequiredMixin, DatatableDataframeView):
    template_name = "traffic/channels/index.html"
    page_title = "Оплаты по каналам"
    permission_required = ("core.page_view_traffic_channels",)
    table_pagination = False
    table_class = ChannelsTable
    filterset_class = ChannelsFilter

    def get_channels(self) -> Dict[int, str]:
        return dict(
            RoistatDimension.objects.filter(
                type=RoistatDimensionType.marker_level_1.name,
            )
            .order_by("title")
            .values_list("pk", "title")
        )

    def get_payments(self) -> pandas.DataFrame:
        payments = queryset_as_dataframe(PaymentAnalytic.objects.all())
        payments.drop(
            columns=[
                "id",
                "manager",
                "user_id",
                "date_created",
                "date_zoom",
                "email",
                "amocrm_id",
                "type",
                "roistat_url",
                "params",
            ],
            inplace=True,
        )
        payments.rename(
            columns={
                "date_last_paid": "date",
                "roistat_marker_level_1_id": "channel",
            },
            inplace=True,
        )
        payments["channel"] = payments["channel"].fillna(0).astype(int)
        payments["channel"] = payments["channel"].apply(
            lambda item: self.channels.get(item, "Undefined") or "Undefined"
        )
        payments = payments.groupby(['channel', 'date_payment', 'date', 'manager_group', 'group'], as_index=False)[
            'profit'].sum()
        return payments

    def get_expenses(self) -> pandas.DataFrame:
        date_from = self.filterset.form.cleaned_data.get("expenses_date_from")
        date_to = self.filterset.form.cleaned_data.get("expenses_date_to")
        expenses = data_reader.dataframe("ipl_report.pkl")
        expenses = expenses[["date", "expenses", "account"]]
        expenses.rename(columns={"account": "channel"}, inplace=True)
        if date_from:
            expenses = expenses[expenses["date"] >= date_from]
        if date_to:
            expenses = expenses[expenses["date"] <= date_to]
        expenses["channel"] = expenses["channel"].apply(
            lambda item: self.channels.get(item, "Undefined") or "Undefined"
        )
        return expenses

    def get_leads(self) -> pandas.DataFrame:
        data = data_reader.dataframe("leads.pkl")
        cleaned_data = getattr(self.filterset.form, "cleaned_data", {})

        date_from = cleaned_data.get("expenses_date_from")
        if date_from:
            date_from = ANALYTIC_TZ.localize(
                datetime.datetime.combine(date_from, datetime.time.min)
            )
            data = data[data["created"] >= date_from]

        date_to = cleaned_data.get("expenses_date_to")
        if date_to:
            date_to = ANALYTIC_TZ.localize(
                datetime.datetime.combine(date_to, datetime.time.max)
            )
            data = data[data["created"] <= date_to]

        data = data[["created", "account", "ipl"]]
        data.rename(
            columns={"created": "date", "account": "channel"}, inplace=True
        )
        data["channel"] = data["channel"].apply(
            lambda item: self.channels.get(item, "Undefined") or "Undefined"
        )
        data["date"] = data["date"].apply(
            lambda item: item.astimezone(ANALYTIC_TZ).date()
        )

        return data.reset_index(drop=True)

    def get_data(self) -> pandas.DataFrame:
        self.channels = self.get_channels()
        payments = self.get_payments()
        return payments

    def prepare_table(self, payments: pandas.DataFrame) -> pandas.DataFrame:
        expenses = self.get_expenses()
        expenses_rows = []
        for (date, channel), row in expenses.groupby(by=["date", "channel"]):
            expenses_rows.append(
                {
                    "date": date,
                    "channel": channel,
                    "expenses": row["expenses"].sum(),
                }
            )
        expenses = pandas.DataFrame(data=expenses_rows, columns=["date", "channel", "expenses"])

        leads = self.get_leads()

        data = payments.merge(expenses, how="outer", on=["date", "channel"])
        data["profit"].fillna(0, inplace=True)
        data["expenses"].fillna(0, inplace=True)
        data.drop(columns=["date", "date_payment"], inplace=True)

        output = []
        total_expenses = 0
        total_profit = 0
        total_leads_quantity = 0
        total_payments_quantity = 0

        for channel_name, channel in data.groupby(by="channel"):
            leads_channel = leads[leads["channel"] == channel_name]
            expenses = channel["expenses"].sum()
            profit = channel["profit"].sum()
            percent = profit / expenses if expenses else 0
            leads_quantity = leads_channel.shape[0]
            payments_quantity = payments[
                payments["channel"] == channel_name
                ].shape[0]
            conversion = (
                payments_quantity / leads_quantity if leads_quantity else 0
            )
            average_payment = (
                profit / payments_quantity if payments_quantity else 0
            )
            lead_price = expenses / leads_quantity if leads_quantity else 0
            profit_on_lead = profit / leads_quantity if leads_quantity else 0
            ipl_available = leads_channel[leads_channel["ipl"] > 0]
            ipl = (
                ipl_available["ipl"].sum() / ipl_available.shape[0]
                if ipl_available.shape[0]
                else 0
            )

            total_expenses += expenses
            total_profit += profit
            total_leads_quantity += leads_quantity
            total_payments_quantity += payments_quantity

            row = {
                "channel": channel_name,
                "expenses": expenses,
                "profit": profit,
                "percent": float(percent),
                "leads_quantity": leads_quantity,
                "payments_quantity": payments_quantity,
                "conversion": float(conversion),
                "average_payment": float(average_payment),
                "lead_price": float(lead_price),
                "profit_on_lead": float(profit_on_lead),
                "ipl": float(ipl),
            }

            output.append(row)

        data = pandas.DataFrame(data=output, columns=["channel", "expenses", "profit", "percent", "leads_quantity",
                                                      "payments_quantity", "conversion", "average_payment",
                                                      "lead_price", "profit_on_lead", "ipl"]).sort_values(by="channel")

        total_ipl_available = leads[leads["ipl"] > 0]
        total_ipl = (
            total_ipl_available["ipl"].sum() / total_ipl_available.shape[0]
            if total_ipl_available.shape[0]
            else 0
        )
        self.extra_context = {
            "total": pandas.Series(
                {
                    "channel": "Всего",
                    "expenses": Renderer.money(total_expenses),
                    "profit": Renderer.money(total_profit),
                    "percent": Renderer.percent(
                        float(
                            total_profit / total_expenses
                            if total_expenses
                            else 0
                        )
                    ),
                    "leads_quantity": Renderer.int(total_leads_quantity),
                    "payments_quantity": Renderer.int(total_payments_quantity),
                    "conversion": Renderer.percent(
                        float(
                            total_payments_quantity / total_leads_quantity
                            if total_leads_quantity
                            else 0
                        ),
                        2,
                    ),
                    "average_payment": Renderer.money(
                        float(
                            total_profit / total_payments_quantity
                            if total_payments_quantity
                            else 0
                        )
                    ),
                    "lead_price": Renderer.money(
                        float(
                            total_expenses / total_leads_quantity
                            if total_leads_quantity
                            else 0
                        )
                    ),
                    "profit_on_lead": Renderer.money(
                        float(
                            total_profit / total_leads_quantity
                            if total_leads_quantity
                            else 0
                        )
                    ),
                    "ipl": Renderer.money(float(total_ipl)),
                }
            ),
        }

        return data


class FunnelsView(LPRequiredMixin, DatatableDataframeView):
    template_name = "traffic/funnels/index.html"
    page_title = "Оборот по воронке и каналу трафика"
    permission_required = ("core.page_view_traffic_funnels",)
    table_pagination = False
    table_class = FunnelsTable
    filterset_class = FunnelsFilter

    def get_data(self) -> pandas.DataFrame:
        default_df: pandas.DataFrame = pandas.DataFrame(
            columns=[
                "channel",
                "expenses_intensiv3",
                "profit_intensiv3",
                "expenses_intensiv2",
                "profit_intensiv2",
                "expenses_gpt",
                "profit_gpt",
                "expenses_neirostaff",
                "profit_neirostaff",
                "expenses_7lesson",
                "profit_7lesson",
                "expenses_gptveb",
                "profit_gptveb",
            ]
        )
        rows = ["-", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        default_df.loc[len(default_df)] = rows
        return default_df

    def update_filters(self):
        cleaned_data = getattr(self.filterset.form, "cleaned_data", {})
        return cleaned_data

    def prepare_table(self, data: pandas.DataFrame) -> pandas.DataFrame:
        channel_data = list(Channel.objects.values("key", "value"))
        channels = {item["key"]: item["value"] for item in channel_data}
        filter_selected = self.update_filters()
        if filter_selected:
            lead_df = filter_selected.get("lead_df")
            lead_dt = filter_selected.get("lead_dt")
            paid_df = filter_selected.get("payment_df")
            paid_dt = filter_selected.get("payment_dt")
            if lead_df and lead_dt and paid_df and paid_dt:
                """БЛОК ПРИХОДОВ"""
                pkl_data = data_reader.dataframe("funnel_channel_profit.pkl")
                profit = pkl_data[
                    (pkl_data["payment_date"] >= paid_df)
                    & (pkl_data["payment_date"] <= paid_dt)
                    & (pkl_data["lead_date"] >= lead_df)
                    & (pkl_data["lead_date"] <= lead_dt)
                    ]

                pivot_profit = pandas.pivot_table(
                    profit,
                    values="profit",
                    index=["payment_date", "lead_date", "channel"],
                    columns="url",
                    fill_value=0,
                    aggfunc="sum",
                )
                pivot_table = pivot_profit.reset_index()
                pivot_table.drop(
                    columns=["lead_date", "payment_date"], inplace=True
                )
                pivot_table["channel"] = pivot_table["channel"].apply(
                    translate_channel, args=(channels,)
                )
                result_profit = (
                    pivot_table.groupby("channel").sum().reset_index()
                )
                rename_mapping = {
                    "channel": "channel",
                    "ChatGPT. Курс 5 уроков": "profit_gpt",
                    "Интенсив 2 дня": "profit_intensiv2",
                    "Интенсив 3 дня": "profit_intensiv3",
                    "Нейростафф": "profit_neirostaff",
                    "Курс AI. 7 уроков": "profit_7lesson",
                    "ChatGPT. Вебинар": "profit_gptveb",
                }
                for old_column, new_column in rename_mapping.items():
                    if old_column in result_profit.columns:
                        result_profit.rename(
                            columns={old_column: new_column}, inplace=True
                        )

                """БЛОК РАСХОДОВ"""
                pkl_data = data_reader.dataframe("funnel_channel_expenses.pkl")
                expenses = pkl_data[
                    (pkl_data["lead_date"] >= lead_df)
                    & (pkl_data["lead_date"] <= lead_dt)
                    ]
                pivot_expenses = pandas.pivot_table(
                    expenses,
                    values="expenses",
                    index=["lead_date", "channel"],
                    columns="url",
                    aggfunc="sum",
                    fill_value=0,
                )
                pivot_table = pivot_expenses.reset_index()
                pivot_table.drop(columns=["lead_date"], inplace=True)
                pivot_table["channel"] = pivot_table["channel"].apply(
                    translate_channel, args=(channels,)
                )
                result_expenses = (
                    pivot_table.groupby("channel").sum().reset_index()
                )
                rename_mapping = {
                    "channel": "channel",
                    "ChatGPT. Курс 5 уроков": "expenses_gpt",
                    "Интенсив 2 дня": "expenses_intensiv2",
                    "Интенсив 3 дня": "expenses_intensiv3",
                    "Нейростафф": "expenses_neirostaff",
                    "Курс AI. 7 уроков": "expenses_7lesson",
                    "ChatGPT. Вебинар": "expenses_gptveb",
                }
                for old_column, new_column in rename_mapping.items():
                    if old_column in result_expenses.columns:
                        result_expenses.rename(
                            columns={old_column: new_column}, inplace=True
                        )
                # Склейка части приходной и расходной
                result_funnel_channel: pandas.DataFrame = pandas.merge(
                    result_profit, result_expenses, on="channel", how="outer"
                )
                result_funnel_channel = result_funnel_channel.fillna(0)

                # Склейка стоковой таблицы и полученной (обход недостатка
                # колонок)
                default_df: pandas.DataFrame = pandas.DataFrame(
                    columns=[
                        "channel",
                        "expenses_intensiv3",
                        "profit_intensiv3",
                        "expenses_intensiv2",
                        "profit_intensiv2",
                        "expenses_gpt",
                        "profit_gpt",
                        "expenses_neirostaff",
                        "profit_neirostaff",
                        "expenses_7lesson",
                        "profit_7lesson",
                        "expenses_gptveb",
                        "profit_gptveb",
                    ]
                )
                result_df: pandas.DataFrame = pandas.merge(
                    result_funnel_channel, default_df, how="outer"
                )
                result_df = result_df.fillna(0)

                result_df["expenses_intensiv3"] = result_df[
                    "expenses_intensiv3"
                ].astype(int)
                result_df["profit_intensiv3"] = result_df[
                    "profit_intensiv3"
                ].astype(int)

                result_df["expenses_intensiv2"] = result_df[
                    "expenses_intensiv2"
                ].astype(int)
                result_df["profit_intensiv2"] = result_df[
                    "profit_intensiv2"
                ].astype(int)

                result_df["expenses_gpt"] = result_df["expenses_gpt"].astype(
                    int
                )
                result_df["profit_gpt"] = result_df["profit_gpt"].astype(int)

                result_df["expenses_neirostaff"] = result_df[
                    "expenses_neirostaff"
                ].astype(int)
                result_df["profit_neirostaff"] = result_df[
                    "profit_neirostaff"
                ].astype(int)

                result_df["expenses_7lesson"] = result_df[
                    "expenses_7lesson"
                ].astype(int)
                result_df["profit_7lesson"] = result_df[
                    "profit_7lesson"
                ].astype(int)

                result_df["expenses_gptveb"] = result_df[
                    "expenses_gptveb"
                ].astype(int)
                result_df["profit_gptveb"] = result_df["profit_gptveb"].astype(
                    int
                )

                row_sum = (
                    result_df.select_dtypes(include="number").sum().astype(int)
                )
                result_row = pandas.DataFrame(row_sum).T
                result_row["channel"] = "Итого"
                result_df = pandas.concat(
                    [result_row, result_df], ignore_index=True
                )
                return result_df
        return data


class DoubleView(LPRequiredMixin, DatatableDataframeView):
    template_name = "traffic/double/index.html"
    page_title = "Количество дублей по каналам"
    permission_required = ("core.page_view_traffic_double",)
    table_pagination = False
    table_class = DoubleTable
    filterset_class = DoubleFilter

    def get_data(self) -> pandas.DataFrame:
        default: pandas.DataFrame = pandas.DataFrame(
            columns=["channel", "count_lead", "count_double", "percent_double", "event"]
        )
        rows = ["Нет данных", 0, 0, "0%", None]
        default.loc[len(default)] = rows
        return default

    def update_filters(self):
        if hasattr(self.filterset.form, "cleaned_data"):
            set_filter = Q()
            lead_df = self.filterset.form.cleaned_data.get("lead_df")
            lead_dt = self.filterset.form.cleaned_data.get("lead_dt")
            if lead_df and lead_dt:
                set_filter &= Q(
                    date_created__date__gte=lead_df,
                    date_created__date__lte=lead_dt,
                )
                return Lead.objects.filter(set_filter).values("date_created", "email", "roistat_url")

    def prepare_table(self, data: pandas.DataFrame) -> pandas.DataFrame:
        landings = list(
            LandingPage.objects.filter(paid=True).values_list("url", flat=True)
        )
        channel_events = list(FunnelChannelUrl.objects.values("url", "group"))
        channel_data = list(Channel.objects.values("key", "value"))
        channels = {item["key"]: item["value"] for item in channel_data}
        queryset = self.update_filters()
        if queryset:
            db_email_list = list(Lead.objects.values_list("email", flat=True))
            email_counter = Counter(db_email_list)
            df = pandas.DataFrame.from_records(queryset)
            df["params"] = df["roistat_url"].apply(parse_url_params)
            df["params"] = df["params"].apply(detect_empty_params)
            df["channel"] = df["params"].apply(detect_channel_from_params)
            df["url"] = df["roistat_url"].apply(parse_url)
            df['event'] = df['url'].apply(get_event, args=(channel_events,))
            df = self.update_dataframe_by_event(df)
            if df.empty:
                return self.get_data()
            df = df.dropna(subset=["channel", "url", "email"])
            df["url"] = df["url"].apply(detect_pay_traffic, args=(landings,))
            df = df[df["url"]]
            df["count_double"] = df["email"].apply(
                lambda x: 1 if email_counter[x] > 1 else 0
            )
            df["channel"] = df["channel"].apply(
                translate_channel, args=(channels,)
            )
            df.drop(columns=["roistat_url", "params", "url"], inplace=True)
            result = (
                df.groupby(["event", "channel"])
                .agg(
                    email=("email", "count"),
                    count_double=("count_double", "sum"),
                )
                .reset_index()
            )
            result["count_lead"] = result["email"]
            result["percent_double"] = (
                                               (result["count_double"] / result["count_lead"]) * 100
                                       ).round(1).astype(str) + "%"
            result.drop(columns=["email"], inplace=True)
            row_sum = result.select_dtypes(include="number").sum().astype(int)
            result_row = pandas.DataFrame(row_sum).T
            result_row["channel"] = "Итого"
            result_row["percent_double"] = (
                                                   (result_row["count_double"] / result_row["count_lead"]) * 100
                                           ).round(1).astype(str) + "%"
            result_df = pandas.concat([result_row, result], ignore_index=True)

            value_to_insert_before = FunnelChannelUrlType.choices()
            agg_columns = ["count_lead", "count_double"]
            result_df.sort_values(by=['event'], inplace=True, ignore_index=True)

            for key, value in value_to_insert_before:
                index_to_insert = result_df.index[result_df["event"] == key].min()

                if pandas.notna(index_to_insert):
                    total_row = {"channel": value, 'event': key}
                    category_sum = result_df[result_df["event"] == key][agg_columns].sum()
                    total_row.update(category_sum.to_dict())
                    total_row["percent_double"] = str(
                        round(total_row["count_double"] / total_row["count_lead"] * 100, 1)
                    ) + "%"

                    result_df = pandas.concat(
                        [
                            result_df.loc[:index_to_insert - 1],
                            pandas.DataFrame([total_row]),
                            result_df.loc[index_to_insert:],
                        ]
                    ).reset_index(drop=True)
            last_row = result_df.iloc[-1]
            last_row['event'] = 'all'
            result_df = result_df.iloc[:-1]
            result_df = pandas.concat([last_row.to_frame().T, result_df], ignore_index=True)
            data = result_df
        return data

    def generate_csv_response(self, dataframe: pandas.DataFrame, filename: str):
        if dataframe.empty:
            return
        csv_data = io.BytesIO()
        dataframe.to_csv(csv_data, sep=";", encoding="utf-8-sig", index=False)
        csv_content = csv_data.getvalue()
        return FileResponse(
            io.BytesIO(csv_content),
            as_attachment=True,
            filename=f"{filename}.csv",
        )

    def get_dataframe(
            self,
            request,
            report: str,
            channel: str,
            event: str
    ) -> pandas.DataFrame:
        landings = list(
            LandingPage.objects.filter(paid=True).values_list("url", flat=True)
        )
        channel_events = list(FunnelChannelUrl.objects.values("url", "group"))
        channel_data = list(Channel.objects.values("key", "value"))
        channels = {item["key"]: item["value"] for item in channel_data}

        lead_df = request.GET.get("lead_df")
        lead_dt = request.GET.get("lead_dt")

        if lead_df and lead_dt:
            queryset = Lead.objects.filter(
                date_created__date__gte=lead_df,
                date_created__date__lte=lead_dt,
            ).values("date_created", "name", "phone", "email", "roistat_url")

            db_email_list = list(Lead.objects.values_list("email", flat=True))
            email_counter = Counter(db_email_list)

            df = pandas.DataFrame.from_records(queryset)

            df["params"] = df["roistat_url"].apply(parse_url_params)
            df["params"] = df["params"].apply(detect_empty_params)
            df["channel"] = df["params"].apply(detect_channel_from_params)
            df["url"] = df["roistat_url"].apply(parse_url)
            df['event'] = df['url'].apply(get_event, args=(channel_events,))
            df = self.update_dataframe_by_event(df)
            if df.empty:
                return pandas.DataFrame(columns=['date_created', 'name', 'phone', 'email', 'roistat_url', 'event'])
            if event and event != 'all':
                df = df[df["event"] == event]
            df = df.dropna(subset=["channel", "url", "email", "event"])
            df["url"] = df["url"].apply(detect_pay_traffic, args=(landings,))
            df = df[df["url"]]
            df["count_double"] = df["email"].apply(
                lambda x: 1 if email_counter[x] > 1 else 0
            )
            df["channel"] = df["channel"].apply(
                translate_channel, args=(channels,)
            )
            if report == "count_double":
                df = df[df["count_double"] == 1]

            channel_reports = set([value for _, value in FunnelChannelUrlType.choices()])
            channel_reports.add('Итого')
            if channel not in channel_reports:
                df = df[df["channel"] == channel]
            df.drop(
                columns=["params", "url", "count_double", "event"],
                inplace=True,
            )
            return df

    def get(self, request, *args, **kwargs):
        if "report" and "lead_df" and "lead_dt" in request.GET:
            report = request.GET.get("report")
            channel = request.GET.get("channel")
            event = request.GET.get("event")
            response_csv = self.generate_csv_response(
                self.get_dataframe(request, report=report, channel=channel, event=event),
                filename=report,
            )
            if response_csv:
                return response_csv
        return super().get(request, *args, **kwargs)

    def update_dataframe_by_event(self, df: pandas.DataFrame) -> pandas.DataFrame:
        cleaned_data = self.request.GET
        if cleaned_data:
            lead_intensive_2days = cleaned_data.get("lead_intensive_2days")
            lead_intensive_3days = cleaned_data.get("lead_intensive_3days")
            lead_neirostaff = cleaned_data.get("lead_neirostaff")
            lead_baza = cleaned_data.get("lead_baza")
            lead_universe = cleaned_data.get("lead_universe")
            lead_others = cleaned_data.get("lead_others")

            channel_event = []
            if lead_intensive_2days:
                channel_event.extend(
                    [
                        FunnelChannelUrlType.intensive2day.name,
                    ]
                )
            if lead_intensive_3days:
                channel_event.extend(
                    [
                        FunnelChannelUrlType.intensive3day.name,
                    ]
                )
            if lead_neirostaff:
                channel_event.extend(
                    [
                        FunnelChannelUrlType.neirostaff.name,
                    ]
                )
            if lead_baza:
                channel_event.extend(
                    [
                        FunnelChannelUrlType.chatgptveb.name,
                    ]
                )
            if lead_universe:
                channel_event.extend(
                    [
                        FunnelChannelUrlType.universe.name,
                    ]
                )
            if lead_others:
                channel_event.extend(
                    [
                        FunnelChannelUrlType.chatgpt.name,
                        FunnelChannelUrlType.course7lesson.name,
                        'Undefined',
                    ]
                )
            if channel_event:
                return df[df['event'].isin(channel_event)]
            return df


class UploadLeadsView(LPRequiredMixin, DatatableDataframeView):
    template_name = "traffic/leads/upload.html"
    page_title = "Загрузка tilda лидов"
    permission_required = ("core.page_view_traffic_upload_leads",)
    table_class = UploadLeadsTable
    table_detail_class = UploadLeadsDetailTable
    filterset_class = UploadFilter
    table_pagination = False

    def get_data(self):
        if "table" in self.request.session:
            table_data = self.request.session["table"]
            return pandas.DataFrame(table_data)
        return pandas.DataFrame({})

    @staticmethod
    def create_tildaleads(dataframe: pandas.DataFrame) -> None:
        """
        Метод добавляет в БД тильда лиды, создает историю и добавляет в
        карусельку
        """
        for _, data in dataframe.iterrows():
            try:
                lead_data = TildaLeadsParseData(data=data)()
                request = HttpRequest(data=lead_data)
                request.method = "POST"
                tildaLeads_view = LeadAPIView()
                tildaLeads_view.post(request)
            except APIException as err:
                logger.info(f"ERROR: {data.email}, {data.phone} | {err}")

    @staticmethod
    def read_file(request) -> pandas.DataFrame | None:
        if (
                request.user.has_perm("core.page_view_traffic_upload_leads")
                and "file" in request.FILES
        ):
            return pandas.read_csv(request.FILES["file"])
        return pandas.DataFrame({})

    @staticmethod
    def get_columns() -> dict[str, str]:
        return {
            "created": "created",
            "name": "name",
            "email": "email",
            "phone": "phone",
            "sp_book_id": "sp_book_id",
            "roistat_url": "roistat_url",
            "formid": "formid",
            "country": "qa_1",
            "сколько_вам_лет": "qa_2",
            "в_какой_сфере_сейчас_работаете": "qa_3",
            "ваш_средний_доход_в_месяц": "qa_4",
            "рассматриваете_ли_в_перспективе_платное_обучение_профессии_разработчик_искусственного_интеллекта": "qa_5",
            "сколько_времени_готовы_выделить_на_обучение_в_неделю": "qa_6",
        }

    @staticmethod
    def parse_roistat_url(url) -> dict[str, str]:
        parsed_url = urlparse(url)
        qs = parse_qs(parsed_url.query)
        data = defaultdict(str)

        data["utm_source"] = qs.get("utm_source", [""])[0]
        data["utm_campaign"] = qs.get("utm_campaign", [""])[0]
        data["utm_content"] = qs.get("utm_content", [""])[0]
        data["utm_medium"] = qs.get("utm_medium", [""])[0]
        data["utm_term"] = qs.get("utm_term", [""])[0]
        data["roistat_id"] = qs.get("roistat", [""])[0]
        data["yclid"] = qs.get("yclid", [""])[0]
        data["url"] = (
                parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path
        )
        return data

    @staticmethod
    def get_landing_page(url: str) -> str:
        parsed_url = urlparse(url)
        return parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path

    def check_roistat_url(self, row_roistat: str, lead_roistat: str) -> bool:
        row_roistat = self.parse_roistat_url(row_roistat)
        lead_roistat = self.parse_roistat_url(lead_roistat)
        if row_roistat["url"] != lead_roistat["url"]:
            return True
        if (
                row_roistat["utm_source"] != lead_roistat["utm_source"]
                or row_roistat["utm_campaign"] != lead_roistat["utm_campaign"]
                or row_roistat["utm_content"] != lead_roistat["utm_content"]
                or row_roistat["utm_medium"] != lead_roistat["utm_medium"]
                or row_roistat["utm_term"] != lead_roistat["utm_term"]
                or row_roistat["roistat_id"] != lead_roistat["roistat_id"]
        ):
            return True
        return False

    @staticmethod
    def check_qa(
            row_data: tuple[str],
            lead: pandas.Series,
    ) -> bool:
        if (
                row_data.qa_1 != lead.qa_1
                or row_data.qa_2 != lead.qa_2
                or row_data.qa_3 != lead.qa_3
                or row_data.qa_4 != lead.qa_4
                or row_data.qa_5 != lead.qa_5
                or row_data.qa_6 != lead.qa_6
        ):
            return True
        return False

    @staticmethod
    def get_numbers_from_phone(phone: str):
        return "".join(filter(str.isdigit, phone))

    def get_leads_by(
            self, tildaleds: pandas.DataFrame, **kwargs
    ) -> pandas.DataFrame:
        condition = None
        for key, value in kwargs.items():
            if condition is None:
                condition = tildaleds[key] == value
            else:
                if key == "phone":
                    condition &= tildaleds[key].apply(
                        self.get_numbers_from_phone
                    ) == self.get_numbers_from_phone(value)
                elif key == "created":
                    # отбираются лиды с +\- 16 часов
                    date = pandas.to_datetime(value, utc=True)
                    hours_delta = pandas.to_timedelta(16, unit="h")
                    condition &= (
                                         tildaleds["date_created"] >= date - hours_delta
                                 ) & (tildaleds["date_created"] <= date + hours_delta)
                else:
                    condition &= tildaleds[key] == value

        if condition is None:
            return tildaleds

        return tildaleds[condition]

    def check_one_lead(self, row_data: tuple, lead: pandas.Series) -> bool:
        roistat_url = str(row_data.roistat_url)
        if roistat_url.startswith("http") and lead.roistat_url.startswith(
                "http"
        ):
            if self.check_roistat_url(
                    row_roistat=row_data.roistat_url,
                    lead_roistat=lead.roistat_url,
            ):
                return True
        if self.check_qa(row_data, lead):
            return True
        return False

    def get_undefined_leads(
            self,
            csv_dataframe: pandas.DataFrame,
            tildaleads_dataframe: pandas.DataFrame,
            log: bool = False,
    ) -> pandas.DataFrame:
        logger.info("Getting undefined leads to upload into database...")
        undefined_leads = list()
        for row_data in csv_dataframe.itertuples(index=False):
            # phone + email
            leads_by_email_phone = self.get_leads_by(
                tildaleds=tildaleads_dataframe,
                email=row_data.email,
                phone=row_data.phone,
                created=row_data.created,
            )

            if leads_by_email_phone.empty or leads_by_email_phone.shape[0] == 1:
                lead = (
                    leads_by_email_phone.iloc[0]
                    if not leads_by_email_phone.empty
                    else None
                )
                if lead is None or self.check_one_lead(row_data, lead):
                    # logger.info(f'{row_data.name}, {row_data.email} NOT exists')
                    undefined_leads.append(row_data)
                    continue

            # phone + email + roistat url
            roistat_url = str(row_data.roistat_url)
            leads_by_roistat = (
                leads_by_email_phone[
                    leads_by_email_phone.roistat_url.str.startswith(
                        self.parse_roistat_url(roistat_url)["url"]
                    )
                ]
                if roistat_url.startswith("http")
                else leads_by_email_phone[
                    leads_by_email_phone.roistat_url.str.startswith(roistat_url)
                ]
            )

            if leads_by_roistat.empty or leads_by_roistat.shape[0] == 1:
                lead = (
                    leads_by_roistat.iloc[0]
                    if not leads_by_roistat.empty
                    else None
                )
                if lead is None or self.check_one_lead(row_data, lead):
                    # logger.info(f'{row_data.name}, {row_data.email} NOT exists')
                    undefined_leads.append(row_data)
                    continue

            # phone + email + roistat url + qa
            leads_by_qa = leads_by_roistat[
                (leads_by_roistat.qa_1 == row_data.qa_1)
                & (leads_by_roistat.qa_2 == row_data.qa_2)
                & (leads_by_roistat.qa_3 == row_data.qa_3)
                & (leads_by_roistat.qa_4 == row_data.qa_4)
                & (leads_by_roistat.qa_5 == row_data.qa_5)
                & (leads_by_roistat.qa_6 == row_data.qa_6)
                ]

            if leads_by_qa.empty or leads_by_qa.shape[0] == 1:
                lead = leads_by_qa.iloc[0] if not leads_by_qa.empty else None
                if lead is None or self.check_one_lead(row_data, lead):
                    # logger.info(f'{row_data.name}, {row_data.email} NOT exists')
                    undefined_leads.append(row_data)
                    continue

            if log:
                logger.info(
                    f"Lead {row_data.name, row_data.email, row_data.phone} exists"
                )

        df = pandas.DataFrame(
            data=undefined_leads, columns=self.get_columns().values()
        )
        df.reset_index(drop=True, inplace=True)
        df["index"] = list(range(1, len(df) + 1))
        logger.info("getting undefined leads end ...")
        return df

    def get_tildaleads(self, days: int = 14) -> pandas.DataFrame:
        date_start = datetime.datetime.now() - datetime.timedelta(days=days)
        return pandas.DataFrame(
            TildaLead.objects.filter(
                date_created__date__gte=date_start.date()
            ).values()
        )

    @staticmethod
    def normalize_df(df: pandas.DataFrame) -> pandas.DataFrame:
        for column in df.columns:
            df[column] = df[column].astype(str).str.strip()
        return df

    def post(self, request, *args, **kwargs):
        dataframe = self.read_file(request)
        tildaleads_df = self.get_tildaleads(days=28)
        mode = request.POST.get("mode")

        if not dataframe.empty:
            dataframe.dropna(how="all", inplace=True)
            dataframe.fillna("", inplace=True)
            dataframe = dataframe[self.get_columns().keys()]
            dataframe = self.normalize_df(dataframe)
            dataframe.rename(columns=self.get_columns(), inplace=True)
            dataframe = self.get_undefined_leads(
                csv_dataframe=dataframe, tildaleads_dataframe=tildaleads_df
            )
            landing_pages = set(
                self.get_landing_page(url)
                for url in dataframe["roistat_url"]
                if len(str(url)) > 0 and str(url).startswith("http")
            )
            table_data = pandas.DataFrame(
                data=landing_pages, columns=["roistat_url"]
            )
            self.table_data = table_data
            context = self.get_context_data(**kwargs)
            context["table_detail"] = self.table_detail_class(
                dataframe, request=request
            )
            context["filter"] = self.filterset_class(request=self.request)
            self.request.session["table_detail"] = dataframe.to_dict("records")
            self.request.session["table"] = table_data.to_dict("records")

            if mode == "upload":
                self.create_tildaleads(dataframe)

            return self.render_to_response(context)

        return HttpResponseRedirect(reverse_lazy("traffic:upload_leads"))

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        if "table_detail" in self.request.session:
            table_detail = self.table_detail_class(
                pandas.DataFrame(self.request.session["table_detail"]),
                request=self.request,
            )
            context.update({"table_detail": table_detail})
        return context


class TelegramView(LPRequiredMixin, DatatableDataframeView):
    template_name = "traffic/telegram/index.html"
    page_title = "CR в Telegram"
    permission_required = ("core.page_view_traffic_telegram",)
    table_pagination = False
    table_class = TelegramReportTable
    filterset_class = TelegramFilter

    def get_data(self) -> pandas.DataFrame:
        default: pandas.DataFrame = pandas.DataFrame(
            data=[
                ["Нет данных", "Нет данных", 0, 0, 0, 0, 0, 0],
            ],
            columns=[
                "date_event",
                "channel",
                "count_reg",
                "count_reg_duplicates",
                "count_member",
                "percent_from_reg",
                "tg_visit",
                "percent_to_tg",
            ],
        )
        return default

    def update_filters(self):
        if hasattr(self.filterset.form, "cleaned_data"):
            event_df = self.filterset.form.cleaned_data.get("event_df")
            event_dt = self.filterset.form.cleaned_data.get("event_dt")
            true_keys = [
                key
                for key, value in self.filterset.form.cleaned_data.items()
                if value is True
            ]
            if event_df and event_dt and true_keys:
                landings = list(FunnelChannelUrl.objects.values("url", "group"))

                members_df = get_members_for_cr(event_df, event_dt, true_keys)
                date_of_events = members_df[['date', 'course']].drop_duplicates()
                regs_df = get_regs_for_cr(date_of_events, landings)
                subscriptions_df = get_subscriptions_for_cr(date_of_events, landings)
                return regs_df, members_df, subscriptions_df

        return None, None, None

    def prepare_table(self, data: pandas.DataFrame) -> pandas.DataFrame:
        regs, members, subscriptions = self.update_filters()

        if regs is not None and subscriptions is not None:
            # join members
            regs["count_member"] = (
                regs.apply(
                    lambda row: row["email"] in members[(members["course"] == row["category"]) &
                                                        (members["date"] == row["date_event"])]["email"].tolist()
                    , axis=1
                )
            )

            # join subs
            merge_on_columns = ['email', 'channel', 'date_event', 'category']
            regs_with_subs = regs.merge(subscriptions, on=merge_on_columns, how='left', indicator=True)
            regs['tg_visit'] = regs_with_subs['_merge'].apply(lambda x: 1 if x == 'both' else 0)

            # rename channels
            channels = dict(Channel.objects.values_list("key", "value"))
            regs["channel"] = regs["channel"].apply(translate_channel, args=(channels,))

            # result df
            result_df = (
                regs.groupby(["date_event", "category", "channel"])
                .agg(
                    count_reg=("email", "count"),
                    count_reg_duplicates=("is_duplicated", "sum"),
                    count_member=("count_member", "sum"),
                    tg_visit=("tg_visit", "sum"),
                )
                .reset_index()
            )

            value_to_insert_before = {
                "type_intensiv3": "ИНТЕНСИВ 3 ДНЯ",
                "type_intensiv2": "ИНТЕНСИВ 2 ДНЯ",
                "type_neirostaff": "НЕЙРОСТАФФ",
            }

            agg_columns = ["count_reg", "count_reg_duplicates", "count_member", "tg_visit"]
            result_df.sort_values(by=['category', 'date_event'], inplace=True, ignore_index=True)

            for key, value in value_to_insert_before.items():
                index_to_insert = result_df.index[result_df["category"] == key].min()

                if pandas.notna(index_to_insert):
                    total_row = {"date_event": "", "category": key, "channel": value}
                    category_sum = result_df[result_df["category"] == key][agg_columns].sum()
                    total_row.update(category_sum.to_dict())
                    result_df = pandas.concat(
                        [
                            result_df.loc[:index_to_insert - 1],
                            pandas.DataFrame([total_row]),
                            result_df.loc[index_to_insert:],
                        ]
                    ).reset_index(drop=True)

            result_df["percent_from_reg"] = (result_df["count_member"] / result_df["count_reg"])
            result_df["percent_to_tg"] = result_df["tg_visit"] / result_df["count_reg"]
            data = result_df
        return data