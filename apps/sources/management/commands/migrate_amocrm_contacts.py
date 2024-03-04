import datetime

from time import sleep
from typing import List, Dict, Any
from logging import getLogger

from django.db import transaction

from plugins.amocrm.api import AmocrmAPIClient

from apps.sources.models import AmocrmContact

from ._base import BaseCommand


logger = getLogger(__name__)

CUSTOM_FIELDS = {
    345809: "email",
    345807: "phone",
}


class Command(BaseCommand):
    help = "Сбор контактов из AmoCRM"

    def prepare_data(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        contacts = []
        for item in items:
            custom_fields_values = [
                (
                    field.get("field_id"),
                    field.get("values", [{"value": ""}])[0].get("value", "")
                    or "",
                )
                for field in item.get("custom_fields_values", []) or []
            ]
            custom_fields_values = list(
                filter(
                    lambda field: field[0] in CUSTOM_FIELDS.keys(),
                    custom_fields_values,
                )
            )
            custom_fields_values = dict(
                [
                    (CUSTOM_FIELDS.get(field[0]), field[1])
                    for field in custom_fields_values
                ]
            )
            contacts.append(
                {
                    "amocrm_id": int(item.get("id")),
                    "date_created": datetime.datetime.utcfromtimestamp(
                        item.get("created_at")
                    ).replace(tzinfo=datetime.timezone.utc)
                    if item.get("created_at")
                    else None,
                    "date_updated": datetime.datetime.utcfromtimestamp(
                        item.get("updated_at")
                    ).replace(tzinfo=datetime.timezone.utc)
                    if item.get("updated_at")
                    else None,
                    "name": item.get("name", "") or "",
                    "responsible_user": int(item.get("responsible_user_id")),
                    "is_deleted": item.get("is_deleted", False) or False,
                    **custom_fields_values,
                }
            )
        return contacts

    def get_contacts(self, page: int = 1) -> List[Dict[str, Any]]:
        logger.info("  ↳ Request API page: %(page)d" % {"page": page})
        amocrm_api = AmocrmAPIClient()
        sleep(1)
        response = amocrm_api.contacts.get(page=page)
        output = response.get("_embedded", {}).get("contacts", [])

        next_link = response.get("_links", {}).get("next", {}).get("href")
        if next_link:
            next_page = int(response.get("_page")) + 1
            output += self.get_contacts(next_page)

        return output

    def handle(self, **kwargs):
        logger.info("Update all contacts")

        response = self.get_contacts()
        items = self.prepare_data(response)
        logger.info(
            "    ↳ Contacts quantity: %(quantity)d" % {"quantity": len(items)}
        )

        items = dict([(item.get("amocrm_id"), item) for item in items])

        contacts_update_instances = AmocrmContact.objects.filter(
            amocrm_id__in=items.keys()
        )
        amocrm_id_created = []
        for instance in contacts_update_instances:
            amocrm_id = instance.amocrm_id
            amocrm_id_created.append(amocrm_id)
            instance.date_created = items.get(amocrm_id).get("date_created")
            instance.date_updated = items.get(amocrm_id).get("date_updated")
            instance.responsible_user = items.get(amocrm_id).get(
                "responsible_user"
            )
            instance.is_deleted = (
                items.get(amocrm_id).get("is_deleted", False) or False
            )
            instance.email = items.get(amocrm_id).get("email", "") or ""
            instance.phone = items.get(amocrm_id).get("phone", "") or ""

        contacts_create_instances = [
            AmocrmContact(**items.get(amocrm_id))
            for amocrm_id in set(items.keys()) - set(amocrm_id_created)
        ]

        with transaction.atomic():
            AmocrmContact.objects.bulk_create(contacts_create_instances)
            AmocrmContact.objects.bulk_update(
                contacts_update_instances,
                [
                    "date_created",
                    "date_updated",
                    "name",
                    "responsible_user",
                    "is_deleted",
                    "email",
                    "phone",
                ],
            )
