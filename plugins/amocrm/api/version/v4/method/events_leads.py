from enum import Enum
from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, NonNegativeInt, conint, validator

from ..base import (
    Base,
    NestedBaseModel as BaseNestedBaseModel,
    DateRange)


class DataGetWithEnum(Enum):
    """
    https://www.amocrm.ru/developers/content/crm_platform/events-and-notes#with-e2e4c901-fcb2-463f-b8bf-fcdf1643963d-params
    """
    contact_name = "contact_name"
    lead_name = "lead_name"
    company_name = "company_name"
    catalog_element_name = "catalog_element_name"
    customer_name = "customer_name"
    catalog_name = "catalog_name"


class DataGetTypeEnum(Enum):
    """
    https://www.amocrm.ru/developers/content/crm_platform/events-and-notes#events-types
    """
    lead_added = "lead_added"
    lead_deleted = "lead_deleted"
    lead_restored = "lead_restored"
    lead_status_changed = "lead_status_changed"
    lead_linked = "lead_linked"
    lead_unlinked = "lead_unlinked"
    contact_added = "contact_added"
    contact_deleted = "contact_deleted"
    contact_restored = "contact_restored"
    contact_linked = "contact_linked"
    contact_unlinked = "contact_unlinked"
    company_added = "company_added"
    company_deleted = "company_deleted"
    company_restored = "company_restored"
    company_linked = "company_linked"
    company_unlinked = "company_unlinked"
    customer_added = "customer_added"
    customer_deleted = "customer_deleted"
    customer_status_changed = "customer_status_changed"
    customer_linked = "customer_linked"
    customer_unlinked = "customer_unlinked"
    task_added = "task_added"
    task_deleted = "task_deleted"
    task_completed = "task_completed"
    task_type_changed = "task_type_changed"
    task_text_changed = "task_text_changed"
    task_deadline_changed = "task_deadline_changed"
    task_result_added = "task_result_added"
    incoming_call = "incoming_call"
    outgoing_call = "outgoing_call"
    incoming_chat_message = "incoming_chat_message"
    outgoing_chat_message = "outgoing_chat_message"
    entity_direct_message = "entity_direct_message"
    incoming_sms = "incoming_sms"
    outgoing_sms = "outgoing_sms"
    entity_tag_added = "entity_tag_added"
    entity_tag_deleted = "entity_tag_deleted"
    entity_linked = "entity_linked"
    entity_unlinked = "entity_unlinked"
    sale_field_changed = "sale_field_changed"
    name_field_changed = "name_field_changed"
    ltv_field_changed = "ltv_field_changed"
    custom_field_value_changed = "custom_field_value_changed"
    entity_responsible_changed = "entity_responsible_changed"
    robot_replied = "robot_replied"
    intent_identified = "intent_identified"
    nps_rate_added = "nps_rate_added"
    link_followed = "link_followed"
    transaction_added = "transaction_added"
    common_note_added = "common_note_added"
    common_note_deleted = "common_note_deleted"
    attachment_note_added = "attachment_note_added"
    targeting_in_note_added = "targeting_in_note_added"
    targeting_out_note_added = "targeting_out_note_added"
    geo_note_added = "geo_note_added"
    service_note_added = "service_note_added"
    site_visit_note_added = "site_visit_note_added"
    message_to_cashier_note_added = "message_to_cashier_note_added"
    key_action_completed = "key_action_completed"
    entity_merged = "entity_merged"
    # custom_field_{FIELD_ID}_value_changed


class NestedBaseModel(BaseNestedBaseModel):

    def _get_nested(self, key: str, value: Any) -> Dict[str, Any]:
        if key == 'type_':
            key = key.replace('_', '')
        return super(NestedBaseModel, self)._get_nested(key, value)


class DataGetFilter(NestedBaseModel):
    id: Optional[Union[NonNegativeInt, List[NonNegativeInt]]]
    created_at: Optional[DateRange]
    created_by: Optional[List[NonNegativeInt]]
    entity: Optional[Union[str, List[str]]]
    entity_id: Optional[Union[NonNegativeInt, List[NonNegativeInt]]]
    type_: Optional[Union[DataGetTypeEnum, List[DataGetTypeEnum]]]
    # value_before: Optional[List[str]]
    # value_after: Optional[List[str]]

    @validator('created_by', pre=True, always=True)
    def limit_created_by(cls, v):
        return v[:10] if v else v

    @validator('entity_id', pre=True, always=True)
    def limit_entity_id(cls, v):
        return v[:10] if v else v


class DataGet(BaseModel):
    with_: Optional[List[DataGetWithEnum]]
    page: Optional[NonNegativeInt]
    limit: Optional[conint(ge=1, le=100)]
    filter: Optional[DataGetFilter]

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        data = super().dict(*args, **kwargs)
        with_ = data.pop("with_", None)
        if with_ is not None:
            data["with"] = ",".join([item.value for item in with_])
        filter_ = data.pop("filter", None)
        if filter_ is not None:
            data.update(
                **dict(
                    [(f"filter{key}", value) for key, value in filter_.items()]
                )
            )
            print(data)
        return data


class Method(Base):
    path = "/events"
