from enum import Enum
from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, NonNegativeInt, conint

from ..base import Base, NestedBaseModel, OrderDirectionEnum, DateRange


class DataGetFilterNoteTypeEnum(Enum):
    common = "common"
    call_in = "call_in"
    call_out = "call_out"
    service_message = "service_message"
    message_cashier = "message_cashier"
    geolocation = "geolocation"
    sms_in = "sms_in"
    sms_out = "sms_out"
    extended_service_message = "extended_service_message"
    attachment = "attachment"


class DataGetFilter(NestedBaseModel):
    id: Optional[Union[NonNegativeInt, List[NonNegativeInt]]]
    entity_id: Optional[List[NonNegativeInt]]
    note_type: Optional[
        Union[DataGetFilterNoteTypeEnum, List[DataGetFilterNoteTypeEnum]]
    ]
    updated_at: Optional[DateRange]


class DataGetOrder(NestedBaseModel):
    id: Optional[OrderDirectionEnum]
    updated_at: Optional[OrderDirectionEnum]


class DataGet(BaseModel):
    page: Optional[NonNegativeInt]
    limit: Optional[conint(ge=1, le=250)]
    filter: Optional[DataGetFilter]
    order: Optional[DataGetOrder]

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        data = super().dict(*args, **kwargs)
        filter_ = data.pop("filter", None)
        if filter_ is not None:
            data.update(
                **dict(
                    [(f"filter{key}", value) for key, value in filter_.items()]
                )
            )
        order = data.pop("order", None)
        if order is not None:
            data.update(
                **dict([(f"order{key}", value) for key, value in order.items()])
            )
        return data


class Method(Base):
    path = "/leads/notes"
