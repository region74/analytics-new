from scheduler.base import DjangoOperator


class Operator(DjangoOperator):
    def execute(self, context=None):
        from django.core.management import call_command

        call_command("telegram_bot_report")
