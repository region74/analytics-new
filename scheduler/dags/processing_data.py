import pytz
import datetime

from airflow import DAG

from scheduler.operators import processing_data as operators


dag = DAG(
    dag_id="ProcessingData",
    description="Обработка данных, полученных из удаленных источников",
    schedule_interval="0 1/2 * * *",
    catchup=False,
    start_date=datetime.datetime.combine(
        datetime.datetime.now().astimezone(pytz.UTC).date(), datetime.time.min
    ),
)


collect_leads_op = operators.CollectLeadsOperator(
    task_id="CollectLeads",
    dag=dag,
)
ipl_report_op = operators.IPLReportOperator(
    task_id="IPLReport",
    dag=dag,
)
collect_payment_channel_op = operators.CollectPaymentChannelOperator(
    task_id="CollectPaymentChannel",
    dag=dag,
)
funnel_channel_report_op = operators.FunnelChannelReportOperator(
    task_id="FunnelChannelReport",
    dag=dag,
)
roistat_channel_expenses_op = operators.RoistatChannelExpensesOperator(
    task_id="RoistatChannelExpenses",
    dag=dag,
)


collect_payment_channel_op >> funnel_channel_report_op
