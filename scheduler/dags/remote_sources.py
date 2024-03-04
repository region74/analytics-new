import pytz
import datetime

from airflow import DAG

from scheduler.operators import remote_sources as operators

dag = DAG(
    dag_id="RemoteSources",
    description="Получение данных из удаленных источников",
    schedule_interval="0 0/2 * * *",
    catchup=False,
    start_date=datetime.datetime.combine(
        datetime.datetime.now().astimezone(pytz.UTC).date(), datetime.time.min
    ),
)

migrate_roistat_leads_op = operators.MigrateRoistatLeadsOperator(
    task_id="MigrateRoistatLeads",
    dag=dag,
)
migrate_amocrm_leads_op = operators.MigrateAmocrmLeadsOperator(
    task_id="MigrateAmocrmLeads",
    dag=dag,
)
merge_tilda_leads_op = operators.MergeTildaLeadsOperator(
    task_id="MergeTildaLeads",
    dag=dag,
)
merge_roistat_leads_op = operators.MergeRoistatLeadsOperator(
    task_id="MergeRoistatLeads",
    dag=dag,
)
merge_amocrm_leads_op = operators.MergeAmocrmLeadsOperator(
    task_id="MergeAmocrmLeads",
    dag=dag,
)
merge_related_leads_op = operators.MergeRelatedLeadsOperator(
    task_id="MergeRelatedLeads",
    dag=dag,
)
process_source_leads_op = operators.ProcessSourceLeadsOperator(
    task_id="ProcessSourceLeads",
    dag=dag,
)
roistat_analytic_op = operators.RoistatAnalyticOperator(
    task_id="RoistatAnalytic",
    dag=dag,
)
update_quiz_ipl_op = operators.UpdateQuizIPLOperator(
    task_id="UpdateQuizIPL",
    dag=dag,
)
intensives_emails_op = operators.IntensivesEmailsOperator(
    task_id="IntensivesEmails",
    dag=dag,
)
# migrate_sipuni_calls_op = operators.MigrateSipuniCallsOperator(
#     task_id="MigrateSipuniCalls",
#     dag=dag,
# )
migrate_payment_analytic_op = operators.MigratePaymentAnalyticOperator(
    task_id="MigratePaymentAnalytic",
    dag=dag,
)
manager_calendar_op = operators.ManagerCalendarOperator(
    task_id="ManagerCalendar",
    dag=dag,
)
special_offers_op = operators.SpecialOffersOperator(
    task_id="SpecialOffers",
    dag=dag,
)
# send_webhooks_op = operators.SendWebhooksOperator(
#     task_id="SendWebhooks",
#     dag=dag,
# )
update_paid_url_op = operators.UpdatePaidUrlOperator(
    task_id="UpdatePaidUrl",
    dag=dag,
)
update_category_url_op = operators.UpdateCategoryUrlOperator(
    task_id="UpdateCategoryUrl",
    dag=dag,
)
update_traffic_channels_op = operators.UpdateTrafficChannelsOperator(
    task_id="UpdateTrafficChannels",
    dag=dag,
)

# update_amocrm_contacts_op = operators.UpdateAmocrmContacts(
#     task_id="UpdateAmocrmContacts",
#     dag=dag,
# )
#
# update_payment_analytic_op = operators.UpdatePaymentAnalytic(
#     task_id="UpdatePaymentAnalytic",
#     dag=dag,
# )

migrate_roistat_leads_op >> roistat_analytic_op

migrate_roistat_leads_op >> merge_tilda_leads_op
migrate_amocrm_leads_op >> merge_tilda_leads_op

merge_tilda_leads_op >> merge_roistat_leads_op
merge_tilda_leads_op >> merge_amocrm_leads_op

merge_roistat_leads_op >> merge_related_leads_op
merge_amocrm_leads_op >> merge_related_leads_op

merge_related_leads_op >> process_source_leads_op

process_source_leads_op >> intensives_emails_op

intensives_emails_op >> update_paid_url_op
update_paid_url_op >> update_category_url_op

roistat_analytic_op >> update_traffic_channels_op

# intensives_emails_op >> migrate_sipuni_calls_op
