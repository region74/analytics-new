import pytz
import numpy
import pandas
import datetime

from typing import List, Dict
from functools import reduce
from transliterate import slugify

from django.conf import settings

from apps.traffic.utils import detect_channel_from_params, translate_channel
from apps.utils import queryset_as_dataframe, slugify
from apps.cohorts.filters import (
    CohortsFilter,
    ExpensesFilter,
    TraficOffersFilter,
)
from apps.cohorts.tables import (
    ZoomTable,
    SpecialOffersTable,
    ExpensesTable,
    TraficOffersTable,
)
from apps.cohorts.utils import (
    detect_week,
    detect_category_url,
    detect_channel_url,
    detect_expenses_channel,
    convert_to_romi,
)
from apps.choices import RoistatDimensionType, UserGroup
from apps.datatable.base import DatatableDataframeView
from apps.sources.models import (
    Lead,
    RoistatAnalytic,
    PaymentAnalytic,
    RoistatDimension,
)
from apps.traffic.models import FunnelChannelUrl, Channel
from apps.views.mixins import LPRequiredMixin
from plugins.data import data_reader


def render_week_money(value):
    return int(value)


class BaseCohortsView(DatatableDataframeView):
    value_column_name: str = None
    filename: list = None
    table_pagination = False

    @staticmethod
    def parse_slug(value: str) -> str:
        if str(value) == "" or pandas.isna(value):
            return pandas.NA
        return slugify(str(value), "ru").replace("-", "_")

    def get_values_week(
            self,
            date_from: datetime.date,
            date_end: datetime.date,
            date_to: datetime.date,
            weeks: int,
    ) -> List[int]:
        values = self.values[
            (self.values["date"] >= date_from)
            & (self.values["date"] <= date_to)
            ].reset_index(drop=True)

        output = []
        while date_from <= date_end:
            date_to = date_from + datetime.timedelta(days=6)
            output.append(
                values[
                    (values["profit_date"] >= date_from)
                    & (values["profit_date"] <= date_to)
                    ]["profit"].sum()
            )
            date_from += datetime.timedelta(weeks=1)

        output += [pandas.NA] * (weeks - len(output))

        return output

    def _read_pkl_files(self):
        return [data_reader.dataframe(file) for file in self.filename]

    def get_data(self) -> pandas.DataFrame:
        return pandas.DataFrame({})

    @staticmethod
    def rename_week_columns(column_name):
        if "week_" in column_name:
            return column_name.split("week_")[-1]
        return column_name

    def data_groupby(self, date_from: datetime.date):
        date_end = detect_week(datetime.datetime.now().date())[
                       0
                   ] - datetime.timedelta(weeks=1)
        weeks = ((date_end - date_from) / 7 + datetime.timedelta(days=1)).days

        values_from = [date_from]
        values_to = [date_end + datetime.timedelta(days=6)]
        values_weeks = []
        counts_weeks = []

        while date_from <= date_end:
            date_to = date_from + datetime.timedelta(days=6)
            values_week = self.get_values_week(
                date_from, date_end, date_to, weeks
            )
            values_weeks.append(values_week)
            values_from.append(date_from)
            values_to.append(date_to)
            counts_weeks.append(
                self.counts[
                    (self.counts["date"] >= date_from)
                    & (self.counts["date"] <= date_to)
                    ]["count"].sum()
            )
            date_from += datetime.timedelta(weeks=1)

        data = pandas.DataFrame(
            columns=[str(x) for x in range(1, weeks + 1)],
            data=values_weeks,
        )
        numeric_values = [
            [
                value
                for value in row
                if isinstance(value, (numpy.int64, numpy.float64))
            ]
            for row in values_weeks
        ]

        data.insert(0, self.value_column_name, counts_weeks)
        data.insert(1, "sum", [numpy.sum(v) for v in numeric_values])

        data = pandas.concat(
            [
                pandas.DataFrame(
                    data=[[pandas.NA] * (weeks + 2)], columns=data.columns
                ),
                data,
            ],
            ignore_index=True,
        )
        data.insert(0, "date_from", values_from)
        data.insert(1, "date_to", values_to)
        data = data.iloc[1:].reset_index(drop=True)
        data.fillna("-", inplace=True)
        data.rename(
            columns={
                "date_from": "С даты",
                "date_to": "По дату",
                "sum": "Сумма",
            },
            inplace=True,
        )
        return data

    def update_data(
            self,
            group: str = None,
            manager: str = None,
            channel_traffic: str = None,
    ):
        if group:
            self.values = self.values[self.values.group_id == group]
        if manager:
            self.values = self.values[self.values.manager == manager]
        if channel_traffic:
            self.values = self.values[self.values.channel == channel_traffic]

    def _get_data(self) -> None:
        self.values, self.counts, groups, channels = self._read_pkl_files()

        self.values = self.values[
            self.values["profit_date"] >= self.values["date"]
            ]

        self.values = self.values.merge(
            groups, how="left", on=["manager_id"]
        ).rename(columns={"group": "group_id"})
        self.values["group"] = self.values["group_id"].apply(
            lambda item: f'Группа "{item}"'
        )

        if "manager_id" in self.counts.columns:
            self.counts = self.counts.merge(
                groups, how="left", on=["manager_id"]
            ).rename(columns={"group": "group_id"})

        channels["channel_id"] = channels["account_title"].apply(
            self.parse_slug
        )
        channels.rename(columns={"account_title": "channel"}, inplace=True)
        channels.drop_duplicates(subset=["channel_id"], inplace=True)
        self.values = self.values.merge(channels, how="left", on=["channel_id"])
        return None

    def prepare_table(self, data: pandas.DataFrame) -> pandas.DataFrame:
        self.table_class.base_columns.clear()
        cleaned_data = getattr(self.filterset.form, "cleaned_data", {})
        date_from_field = self.filterset.form.fields.get("date_from")

        group = cleaned_data.get("group")
        manager = cleaned_data.get("manager")
        channel_traffic = cleaned_data.get("channel_traffic")

        if cleaned_data.get("date_from") is None:
            date_from = datetime.datetime.now() - datetime.timedelta(weeks=10)
            date_from = date_from.date()
            date_from_field.initial = date_from.strftime("%Y-%m-%d")
        else:
            date_from = cleaned_data.get("date_from")

        self._get_data()
        self.update_data(
            group=group, manager=manager, channel_traffic=channel_traffic
        )

        data = self.data_groupby(date_from=date_from)
        self.table_class.add_dynamic_columns(
            dict(
                map(
                    lambda item: (item, {"verbose_name": item}),
                    data.columns.tolist(),
                )
            )
        )
        return data


class ZoomView(LPRequiredMixin, BaseCohortsView):
    template_name = "cohorts/zoom.html"
    page_title = 'Когорты "Zoom"'
    permission_required = ("core.page_view_cohorts_zoom",)
    table_class = ZoomTable
    filterset_class = CohortsFilter
    value_column_name: str = "Zoom"
    filename = ["zoom.pkl", "zoom_count.pkl", "groups.pkl", "channels.pkl"]


class SpecialOffersView(LPRequiredMixin, BaseCohortsView):
    template_name = "cohorts/so.html"
    page_title = 'Когорты "Special Offers"'
    permission_required = ("core.page_view_cohorts_so",)
    table_class = SpecialOffersTable
    filterset_class = CohortsFilter
    value_column_name: str = "SO"
    filename = ["so.pkl", "so_count.pkl", "groups.pkl", "channels.pkl"]


class ExpensesView(LPRequiredMixin, DatatableDataframeView):
    template_name = "cohorts/expenses/index.html"
    page_title = 'Когорты "Расход"'
    permission_required = ("core.page_view_cohorts_expenses",)
    table_pagination = False
    table_class = ExpensesTable
    filterset_class = ExpensesFilter

    def update_group(self, data: pandas.DataFrame):
        data["group"] = data["group"].apply(
            lambda item: item if item else "undefined"
        )
        user_groups = dict(UserGroup.choices())
        groups = dict(
            (item, user_groups.get(item, "Undefined") or "Undefined")
            for item in list(data["group"].unique())
        )
        self.groups_available = dict(
            sorted(groups.items(), key=lambda item: item[1])
        )

    def update_manager(self, data: pandas.DataFrame):
        data["manager"].fillna("Undefined", inplace=True)
        managers_list = sorted(list(filter(None, data["manager"].unique())))
        self.managers_available = dict(
            zip(
                list(map(lambda item: slugify(item), managers_list)),
                managers_list,
            )
        )
        managers_swapped = {
            value: key for key, value in self.managers_available.items()
        }
        data["manager"] = data["manager"].apply(
            lambda item: managers_swapped.get(item)
        )

    def update_channel(self, data: pandas.DataFrame):
        data["channel"] = data["channel"].fillna(0).astype(int)
        channels = dict(
            RoistatDimension.objects.filter(
                pk__in=list(data["channel"].unique()),
                type=RoistatDimensionType.marker_level_1.name,
            ).values_list("pk", "name")
        )
        data["channel"] = data["channel"].apply(
            lambda item: channels.get(item, "undefined") or "undefined"
        )
        unavailable = list(
            set(data["channel"].unique()) - set(self.channels.keys())
        )
        if unavailable:
            self.channels.update({"undefined": "Undefined"})
            data.loc[data["channel"].isin(unavailable), "channel"] = "undefined"
        data["channel"] = data["channel"].apply(
            lambda item: self.channels.get(item, "Undefined") or "Undefined"
        )
        data_tmp = data.copy()
        if 'params' in data_tmp.columns:
            channel_data = list(Channel.objects.values("key", "value"))
            channels = {item["key"]: item["value"] for item in channel_data}
            data_tmp['new_channel'] = data_tmp['params'].apply(detect_channel_from_params)
            data_tmp['new_channel_translate'] = data_tmp['new_channel'].apply(translate_channel, args=(channels,))
            data['channel'] = data_tmp['new_channel_translate']
        self.channels_available += list(data["channel"].unique())
        self.channels_available = list(set(self.channels_available))

    def parse_group(self, value: str):
        try:
            return UserGroup[value].name
        except KeyError:
            return ""

    def get_payments(self) -> pandas.DataFrame:
        payments = queryset_as_dataframe(PaymentAnalytic.objects.all())
        payments.drop(
            columns=[
                "id",
                "user_id",
                "date_created",
                "date_zoom",
                "manager_group",
                "email",
                "amocrm_id",
                "type",
                "roistat_url",
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
        payments["group"] = payments["group"].apply(self.parse_group)
        return payments

    def get_expenses(self) -> pandas.DataFrame:
        expenses = data_reader.dataframe("roistat_channel_expenses.pkl")
        return expenses

    def get_data(self) -> pandas.DataFrame:
        self.channels = dict(Channel.objects.values_list("key", "value"))
        self.groups_available = []
        self.managers_available = []
        self.channels_available = []
        return self.get_payments()

    def filter_payments(self, payments: pandas.DataFrame) -> pandas.DataFrame:
        date = self.filterset.form.cleaned_data.get("date")
        group = self.filterset.form.cleaned_data.get("group")
        manager = self.filterset.form.cleaned_data.get("manager")
        channel = self.filterset.form.cleaned_data.get("channel")

        if date:
            payments = payments[payments["date"] >= date]

        self.update_group(payments)
        self.update_manager(payments)
        self.update_channel(payments)

        if group:
            payments = payments[payments["group"] == group]

        if manager:
            payments = payments[payments["manager"] == manager]

        if channel:
            payments = payments[payments["channel"] == channel]

        return payments

    def filter_expenses(self, expenses: pandas.DataFrame) -> pandas.DataFrame:
        date = self.filterset.form.cleaned_data.get("date")
        channel = self.filterset.form.cleaned_data.get("channel")

        if date:
            expenses = expenses[expenses["date"] >= date]

        self.update_channel(expenses)

        if channel:
            expenses = expenses[expenses["channel"] == channel]

        return expenses

    def get_groups(self) -> Dict[str, str]:
        return self.groups_available

    def get_managers(self) -> Dict[str, str]:
        return self.managers_available

    def get_channels(self) -> Dict[str, str]:
        return dict((item, item) for item in sorted(self.channels_available))

    def get_payments_weeks(
            self,
            data: pandas.DataFrame,
            date_from: datetime.date,
            date_end: datetime.date,
            date_to: datetime.date,
            weeks: int,
    ) -> List[int]:
        values = data[
            (data["date"] >= date_from) & (data["date"] <= date_to)
            ].reset_index(drop=True)

        output = []
        while date_from <= date_end:
            date_to = date_from + datetime.timedelta(days=6)
            output.append(
                values[
                    (values["date_payment"] >= date_from)
                    & (values["date_payment"] <= date_to)
                    ]["profit"].sum()
            )
            date_from += datetime.timedelta(weeks=1)

        output += [pandas.NA] * (weeks - len(output))

        return output

    def prepare_table(self, payments: pandas.DataFrame) -> pandas.DataFrame:
        expenses = self.get_expenses()

        payments = self.filter_payments(payments)
        expenses = self.filter_expenses(expenses)
        groups = self.get_groups()
        managers = self.get_managers()
        channels = self.get_channels()

        self.filterset.set_groups_choices(groups)
        self.filterset.set_managers_choices(managers)
        self.filterset.set_channels_choices(channels)

        date_from = detect_week(self.filterset.form.cleaned_data.get("date"))[0]
        date_end = detect_week(
            datetime.datetime.now(
                pytz.timezone(settings.ANALYTIC_TIME_ZONE)
            ).date()
        )[0] - datetime.timedelta(weeks=1)
        weeks = ((date_end - date_from) / 7 + datetime.timedelta(days=1)).days
        if weeks < 0:
            weeks = 0

        payments_from = [date_from]
        payments_to = [
            date_end + datetime.timedelta(weeks=1) - datetime.timedelta(days=1)
        ]
        payments_weeks = []
        values_weeks = []

        while date_from <= date_end:
            date_to = date_from + datetime.timedelta(days=6)
            payments_week = self.get_payments_weeks(
                payments, date_from, date_end, date_to, weeks
            )
            payments_weeks.append(payments_week)
            payments_from.append(date_from)
            payments_to.append(date_to)
            values_weeks.append(
                expenses[
                    (expenses["date"] >= date_from)
                    & (expenses["date"] <= date_to)
                    ]["expenses"].sum()
            )
            date_from += datetime.timedelta(weeks=1)

        weeks_columns = [str(x) for x in range(1, weeks + 1)]
        output = pandas.DataFrame(data=payments_weeks, columns=weeks_columns)
        numeric_values = [
            [
                value
                for value in row
                if isinstance(value, (numpy.int64, numpy.float64))
            ]
            for row in payments_weeks
        ]

        output.insert(0, "value", values_weeks)
        output.insert(1, "sum", ["" for v in numeric_values])

        output = pandas.concat(
            [
                pandas.DataFrame(
                    data=[[pandas.NA] * (weeks + 2)], columns=output.columns
                ),
                output,
            ],
            ignore_index=True,
        )
        output.insert(0, "date_from", payments_from)
        output.insert(1, "date_to", payments_to)
        output.fillna("", inplace=True)

        self.table_class.add_dynamic_columns(
            dict(
                map(
                    lambda item: (
                        item,
                        {
                            "verbose_name": item,
                            "render_method": render_week_money,
                            "orderable": False,
                        },
                    ),
                    weeks_columns,
                )
            )
        )

        return output


class TraficOffersView(LPRequiredMixin, DatatableDataframeView):
    template_name = "cohorts/offers.html"
    page_title = "Когорты по офферам"
    permission_required = ("core.page_view_cohorts_offers",)
    table_pagination = False
    table_class = TraficOffersTable
    filterset_class = TraficOffersFilter

    def get_data(self) -> pandas.DataFrame:
        default: pandas.DataFrame = pandas.DataFrame(
            data=[["Нет данных", 0, 0, 0, 0, 0]],
            columns=["channel", "expenses", "week1", "week2", "week4", "week8"],
        )
        return default

    def update_filters(self):
        if hasattr(self.filterset.form, "cleaned_data"):
            lead_df = self.filterset.form.cleaned_data.get("lead_df")
            lead_dt = self.filterset.form.cleaned_data.get("lead_dt")
            true_keys = [
                key
                for key, value in self.filterset.form.cleaned_data.items()
                if value is True
            ]

            if lead_df and lead_dt and true_keys:
                category_list = list(
                    FunnelChannelUrl.objects.values("url", "group")
                )
                category = {
                    item["url"]: item["group"] for item in category_list
                }

                channel_list = list(Channel.objects.values("key", "value"))
                channel = {item["key"]: item["value"] for item in channel_list}

                lead_df_datetime = (
                    datetime.datetime.combine(
                        lead_df, datetime.datetime.min.time()
                    )
                ).replace(tzinfo=datetime.timezone.utc)
                lead_dt_datetime = (
                    datetime.datetime.combine(
                        lead_dt, datetime.datetime.max.time()
                    )
                ).replace(tzinfo=datetime.timezone.utc)
                leads: pandas.DataFrame = pandas.DataFrame(
                    list(
                        Lead.objects.filter(
                            date_created__gte=lead_df_datetime,
                            date_created__lte=lead_dt_datetime,
                        ).values("date_created", "roistat_url")
                    )
                )
                if not leads.empty:
                    leads["category"] = leads["roistat_url"].apply(
                        detect_category_url, args=(category,)
                    )
                    leads["channel"] = leads["roistat_url"].apply(
                        detect_channel_url, args=(channel,)
                    )
                    leads["date_created"] = leads["date_created"].dt.date
                    leads = leads.rename(columns={"date_created": "date"})
                    leads.dropna(inplace=True)
                    leads.drop(columns=["roistat_url", "date"], inplace=True)
                    leads = leads.drop_duplicates(
                        ["category", "channel"]
                    ).reset_index(drop=True)

                expenses: pandas.DataFrame = pandas.DataFrame(
                    list(
                        RoistatAnalytic.objects.filter(
                            date__gte=lead_df, date__lte=lead_dt
                        ).values(
                            "date",
                            "expenses",
                            "dimension_landing_page__name",
                            "dimension_marker_level_1__name",
                        )
                    )
                )
                if not expenses.empty:
                    expenses["category"] = expenses[
                        "dimension_landing_page__name"
                    ].apply(detect_category_url, args=(category,))
                    expenses["channel"] = expenses[
                        "dimension_marker_level_1__name"
                    ].apply(detect_expenses_channel, args=(channel,))
                    expenses.dropna(inplace=True)
                    expenses.drop(
                        columns=[
                            "dimension_marker_level_1__name",
                            "dimension_landing_page__name",
                            "date",
                        ],
                        inplace=True,
                    )
                    expenses = (
                        expenses.groupby(["category", "channel"])
                        .agg({"expenses": "sum"})
                        .reset_index()
                    )

                profit: pandas.DataFrame = pandas.DataFrame(
                    list(
                        (
                            PaymentAnalytic.objects.filter(
                                date_payment__gte=lead_df,
                                date_payment__lte=lead_df
                                                  + datetime.timedelta(days=56),
                                date_last_paid__gte=lead_df,
                                date_last_paid__lte=lead_dt,
                            ).values("date_payment", "profit", "roistat_url")
                        )
                    )
                )
                if not profit.empty:
                    profit["category"] = profit["roistat_url"].apply(
                        detect_category_url, args=(category,)
                    )
                    profit["channel"] = profit["roistat_url"].apply(
                        detect_channel_url, args=(channel,)
                    )
                    profit = profit.rename(columns={"date_payment": "date"})
                    profit.dropna(inplace=True)
                    profit.drop(columns="roistat_url", inplace=True)

                    week1: pandas.DataFrame = profit[
                        (profit["date"] >= lead_df)
                        & (
                                profit["date"]
                                < lead_df + datetime.timedelta(days=7)
                        )
                        ].copy()
                    week1.drop(columns="date", inplace=True)
                    week1 = (
                        week1.groupby(["category", "channel"])
                        .agg({"profit": "sum"})
                        .reset_index()
                    )
                    week1.rename(columns={"profit": "week1"}, inplace=True)

                    week2: pandas.DataFrame = profit[
                        (profit["date"] >= lead_df + datetime.timedelta(days=7))
                        & (
                                profit["date"]
                                < lead_df + datetime.timedelta(days=14)
                        )
                        ].copy()
                    week2.drop(columns="date", inplace=True)
                    week2 = (
                        week2.groupby(["category", "channel"])
                        .agg({"profit": "sum"})
                        .reset_index()
                    )
                    week2.rename(columns={"profit": "week2"}, inplace=True)

                    week3: pandas.DataFrame = profit[
                        (
                                profit["date"]
                                >= lead_df + datetime.timedelta(days=14)
                        )
                        & (
                                profit["date"]
                                < lead_df + datetime.timedelta(days=21)
                        )
                        ].copy()
                    week3.drop(columns="date", inplace=True)
                    week3 = (
                        week3.groupby(["category", "channel"])
                        .agg({"profit": "sum"})
                        .reset_index()
                    )
                    week3.rename(columns={"profit": "week3"}, inplace=True)

                    week4: pandas.DataFrame = profit[
                        (
                                profit["date"]
                                >= lead_df + datetime.timedelta(days=21)
                        )
                        & (
                                profit["date"]
                                < lead_df + datetime.timedelta(days=28)
                        )
                        ].copy()
                    week4.drop(columns="date", inplace=True)
                    week4 = (
                        week4.groupby(["category", "channel"])
                        .agg({"profit": "sum"})
                        .reset_index()
                    )
                    week4.rename(columns={"profit": "week4"}, inplace=True)

                    week5: pandas.DataFrame = profit[
                        (
                                profit["date"]
                                >= lead_df + datetime.timedelta(days=28)
                        )
                        & (
                                profit["date"]
                                < lead_df + datetime.timedelta(days=35)
                        )
                        ].copy()
                    week5.drop(columns="date", inplace=True)
                    week5 = (
                        week5.groupby(["category", "channel"])
                        .agg({"profit": "sum"})
                        .reset_index()
                    )
                    week5.rename(columns={"profit": "week5"}, inplace=True)

                    week6: pandas.DataFrame = profit[
                        (
                                profit["date"]
                                >= lead_df + datetime.timedelta(days=35)
                        )
                        & (
                                profit["date"]
                                < lead_df + datetime.timedelta(days=42)
                        )
                        ].copy()
                    week6.drop(columns="date", inplace=True)
                    week6 = (
                        week6.groupby(["category", "channel"])
                        .agg({"profit": "sum"})
                        .reset_index()
                    )
                    week6.rename(columns={"profit": "week6"}, inplace=True)

                    week7: pandas.DataFrame = profit[
                        (
                                profit["date"]
                                >= lead_df + datetime.timedelta(days=42)
                        )
                        & (
                                profit["date"]
                                < lead_df + datetime.timedelta(days=49)
                        )
                        ].copy()
                    week7.drop(columns="date", inplace=True)
                    week7 = (
                        week7.groupby(["category", "channel"])
                        .agg({"profit": "sum"})
                        .reset_index()
                    )
                    week7.rename(columns={"profit": "week7"}, inplace=True)

                    week8: pandas.DataFrame = profit[
                        (
                                profit["date"]
                                >= lead_df + datetime.timedelta(days=49)
                        )
                        & (
                                profit["date"]
                                <= lead_df + datetime.timedelta(days=56)
                        )
                        ].copy()
                    week8.drop(columns="date", inplace=True)
                    week8 = (
                        week8.groupby(["category", "channel"])
                        .agg({"profit": "sum"})
                        .reset_index()
                    )
                    week8.rename(columns={"profit": "week8"}, inplace=True)

                    return [
                        leads,
                        expenses,
                        week1,
                        week2,
                        week3,
                        week4,
                        week5,
                        week6,
                        week7,
                        week8,
                        true_keys,
                    ]
            return None

    def prepare_table(self, data: pandas.DataFrame) -> pandas.DataFrame:
        data_list = self.update_filters()
        if (
                data_list is not None
                and len(data_list) >= 2
                and all(df is not None and not df.empty for df in data_list[:2])
        ):
            (
                leads,
                expenses,
                week1,
                week2,
                week3,
                week4,
                week5,
                week6,
                week7,
                week8,
                true_keys,
            ) = data_list
            dfs_to_merge = [
                leads,
                expenses,
                week1,
                week2,
                week3,
                week4,
                week5,
                week6,
                week7,
                week8,
            ]
            result_df = reduce(
                lambda left, right: pandas.merge(
                    left, right, on=["category", "channel"], how="outer"
                ),
                dfs_to_merge,
            )
            agg_columns = [
                "expenses",
                "week1",
                "week2",
                "week3",
                "week4",
                "week5",
                "week6",
                "week7",
                "week8",
            ]
            result_df = (
                result_df.groupby(["category", "channel"])[agg_columns]
                .sum()
                .reset_index()
            )

            value_to_insert_before = {
                "type_intensiv3": "ИНТЕНСИВ 3 ДНЯ",
                "type_intensiv2": "ИНТЕНСИВ 2 ДНЯ",
                "type_gpt_5lesson": "ChatGPT. КУРС 5 УРОКОВ",
                "type_ai_7lesson": "КУРС AI. 7 УРОКОВ",
                "type_neirostaff": "НЕЙРОСТАФФ",
                "type_gpt_vebinar": "ChatGPT. ВЕБИНАР",
            }

            for key, value in value_to_insert_before.items():
                index_to_insert = result_df.index[result_df["category"] == key].min()
                if pandas.notna(index_to_insert):
                    total_row = {"category": key, "channel": value}
                    category_sum = result_df[result_df["category"] == key][agg_columns].sum()
                    total_row.update(category_sum.to_dict())
                    result_df = pandas.concat(
                        [
                            result_df.loc[:index_to_insert - 1],
                            pandas.DataFrame([total_row]),
                            result_df.loc[index_to_insert:],
                        ]
                    ).reset_index(drop=True)

            if "type_all" not in true_keys:
                result_df = result_df[result_df["category"].isin(true_keys)]

            if "cumulative" in true_keys:
                result_df.iloc[:, 3:] = result_df.iloc[:, 3:].cumsum(axis=1)

            if "show_romi" in true_keys:
                result_df = convert_to_romi(result_df)

            result_df = result_df[
                (
                        result_df[["expenses", "week1", "week2", "week4", "week8"]]
                        != 0
                ).any(axis=1)
            ]
            data = result_df
        return data
