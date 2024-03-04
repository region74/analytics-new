import datetime
import pytz

from logging import getLogger

from django.db import transaction

from apps.sources.models import PaymentAnalytic, AmocrmContact, AmocrmUser
from apps.sources.management.commands._base import BaseCommand
from plugins.amocrm.api import AmocrmAPIClient
from plugins.amocrm.api.exceptions import AmocrmAPIException

logger = getLogger(__name__)


class Command(BaseCommand):
    help = "Обновление контактов AmoCRM"

    def get_difference_ids(self) -> list:
        logger.info("  ↳ Get difference ids started")
        pyment_ids = list(set(PaymentAnalytic.objects.values_list('amocrm_id', flat=True)))
        contacts_ids = list(set(AmocrmContact.objects.values_list('amocrm_id', flat=True)))
        difference_ids = [ids for ids in pyment_ids if ids not in contacts_ids]
        logger.info("    ↳ New contacts: %(quantity)d" % {"quantity": len(difference_ids)})
        return difference_ids

    def update_amocrm_contact(self, ids: list):
        logger.info("  ↳ Get new amoCRM contact started")
        contacts_objects = []
        for id_lead in ids:
            amocrm_api_lead_links = AmocrmAPIClient(to_entity_id=id_lead)
            response = amocrm_api_lead_links.lead_links.get()
            links = response.get('_embedded', {}).get('links', [])
            if links:
                contact_id = links[0].get('to_entity_id')
                if contact_id is not None:
                    amocrm_api_contact = AmocrmAPIClient(contact_id=contact_id)
                    try:
                        response_data = amocrm_api_contact.contact.get()
                    except AmocrmAPIException as error:
                        logger.warning(f"    ↳ Invalid response. Contact id: {contact_id}, Lead id {id_lead} - Ignored...")
                        continue
                    if response_data:
                        name = response_data.get('name', '')
                        responsible_user_id = response_data.get('responsible_user_id', '')
                        is_deleted = response_data.get('is_deleted', '')
                        created_at_timestamp = int(response_data.get('created_at', 0))
                        updated_at_timestamp = int(response_data.get('updated_at', 0))
                        moscow_timezone = pytz.timezone('Europe/Moscow')
                        created_at = datetime.datetime.fromtimestamp(created_at_timestamp, tz=moscow_timezone)
                        updated_at = datetime.datetime.fromtimestamp(updated_at_timestamp, tz=moscow_timezone)
                        phone_entry = next((entry for entry in response_data.get('custom_fields_values', []) if
                                            entry.get('field_code') == 'PHONE'), {})
                        phone_value = phone_entry.get('values', [{}])[0].get('value', '')
                        email_entry = next((entry for entry in response_data.get('custom_fields_values', []) if
                                            entry.get('field_code') == 'EMAIL'), {})
                        email_value = email_entry.get('values', [{}])[0].get('value', '')

                        amocrm_contact = AmocrmContact(
                            date_created=created_at,
                            date_updated=updated_at,
                            name=name,
                            responsible_user=AmocrmUser.objects.filter(amocrm_id=id_lead).first(),
                            is_deleted=is_deleted,
                            phone=phone_value,
                            email=email_value,
                            amocrm_id=id_lead,
                        )

                        contacts_objects.append(amocrm_contact)
        logger.info("    ↳ New contacts objects: %(quantity)d" % {"quantity": len(contacts_objects)})
        with transaction.atomic():
            AmocrmContact.objects.bulk_create(contacts_objects, batch_size=1000)
            logger.info("  ↳ DB has been replenished")

    def handle(self, **kwargs):
        logger.info("Update amocrm_contacts start")
        new_ids = self.get_difference_ids()
        self.update_amocrm_contact(new_ids)
        logger.info("Update amocrm_contacts finish")
