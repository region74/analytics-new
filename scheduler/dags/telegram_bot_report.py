import pytz
import datetime

from airflow import DAG

from scheduler.operators import telegram_bot_report as operators


dag = DAG(
    dag_id="TelegramBotReport",
    description="Отправка отчета в ТГ бота",
    schedule_interval="0 6 * * *",
    catchup=False,
    start_date=datetime.datetime.combine(
        datetime.datetime.now().astimezone(pytz.UTC).date()
        - datetime.timedelta(days=1),
        datetime.time.min,
    ),
)


bot_op = operators.Operator(
    task_id="TelegramBotReport",
    dag=dag,
)
