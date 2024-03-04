import pandas
import datetime

from django.utils import timezone

from scheduler.base import DjangoOperator


class CollectLeadsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("collect_leads")


class IPLReportOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command(
            "ipl_report",
            date_from=timezone.now().date() - datetime.timedelta(days=40),
        )


class CollectPaymentChannelOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("collect_payment_channel")


class FunnelChannelReportOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("funnel_channel_report")


class RoistatChannelExpensesOperator(DjangoOperator):
    def execute(self, context=None):
        from plugins.data import data_writer
        from apps.sources.models import RoistatAnalytic

        data = (
            pandas.DataFrame(
                data=list(
                    RoistatAnalytic.objects.values_list(
                        "date", "expenses", "dimension_marker_level_1__id"
                    )
                ),
                columns=["date", "expenses", "channel"],
            )
            .groupby(by=["date", "channel"])
            .sum()
            .reset_index()
        )
        data_writer.dataframe(data, "roistat_channel_expenses.pkl")
