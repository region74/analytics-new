import datetime

from scheduler.base import DjangoOperator


class MigrateRoistatLeadsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.utils import timezone
        from django.core.management import call_command

        call_command(
            "migrate_roistat_leads",
            step=7,
            date_from=str(timezone.now().date() - datetime.timedelta(days=6)),
        )


class RoistatExpensesOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command(
            "migrate_roistat_expenses",
            date_from=str(
                datetime.datetime.utcnow().date() - datetime.timedelta(days=6)
            ),
        )


class MigrateAmocrmLeadsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.utils import timezone
        from django.core.management import call_command

        call_command(
            "migrate_amocrm_leads",
            step=4,
            date_from=str(timezone.now().date() - datetime.timedelta(days=6)),
        )


class MergeTildaLeadsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("merge_tilda_leads")


class MergeRoistatLeadsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("merge_roistat_leads")


class MergeAmocrmLeadsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("merge_amocrm_leads")


class MergeRelatedLeadsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("merge_related_leads")


class ProcessSourceLeadsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("process_source_leads")


class RoistatAnalyticOperator(DjangoOperator):
    def execute(self, context=None):
        from django.utils import timezone
        from django.core.management import call_command

        call_command(
            "migrate_roistat_analytic",
            date_from=str(timezone.now().date() - datetime.timedelta(days=40)),
        )


class UpdateQuizIPLOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("update_quiz_ipl")


class IntensivesEmailsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("intensives_emails")


class MigrateSipuniCallsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.utils import timezone
        from django.core.management import call_command

        call_command(
            "migrate_sipuni_calls",
            date_from=str(timezone.now().date() - datetime.timedelta(days=6)),
        )


class MigratePaymentAnalyticOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("migrate_payment_analytic")


class ManagerCalendarOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("manager_calendar")


class SpecialOffersOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("special_offers")


class SendWebhooksOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("send_webhooks")


class UpdatePaidUrlOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("update_paid_url")


class UpdateCategoryUrlOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("update_category_url")


class UpdateTrafficChannelsOperator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("update_traffic_channels")


class UpdateAmocrmContacts(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("update_amocrm_contacts")


class UpdatePaymentAnalytic(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("update_payment_analytic")
