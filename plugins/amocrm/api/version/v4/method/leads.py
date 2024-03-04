from enum import Enum
from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, PositiveInt, NonNegativeInt, conint

from ..base import (
    Base,
    NestedBaseModel,
    PriceRange,
    DateRange,
    OrderDirectionEnum,
)


class DataGetWithEnum(Enum):
    catalog_elements = "catalog_elements"
    is_price_modified_by_robot = "is_price_modified_by_robot"
    loss_reason = "loss_reason"
    contacts = "contacts"
    only_deleted = "only_deleted"
    source_id = "source_id"


class DataGetFilterStatuses(BaseModel):
    pipeline_id: NonNegativeInt
    status_id: NonNegativeInt


class DataGetFilter(NestedBaseModel):
    id: Optional[Union[NonNegativeInt, List[NonNegativeInt]]]
    name: Optional[Union[str, List[str]]]
    price: Optional[PriceRange]
    statuses: Optional[List[DataGetFilterStatuses]]
    pipeline_id: Optional[Union[NonNegativeInt, List[NonNegativeInt]]]
    created_by: Optional[Union[PositiveInt, List[PositiveInt]]]
    updated_by: Optional[Union[PositiveInt, List[PositiveInt]]]
    responsible_user_id: Optional[Union[NonNegativeInt, List[NonNegativeInt]]]
    created_at: Optional[DateRange]
    updated_at: Optional[DateRange]
    closed_at: Optional[DateRange]
    closest_task_at: Optional[DateRange]


class DataGetOrder(NestedBaseModel):
    id: Optional[OrderDirectionEnum]
    created_at: Optional[OrderDirectionEnum]
    updated_at: Optional[OrderDirectionEnum]


class DataGet(BaseModel):
    with_: Optional[List[DataGetWithEnum]]
    page: Optional[NonNegativeInt]
    limit: Optional[conint(ge=1, le=250)]
    query: Optional[str]
    filter: Optional[DataGetFilter]
    order: Optional[DataGetOrder]

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
        order = data.pop("order", None)
        if order is not None:
            data.update(
                **dict([(f"order{key}", value) for key, value in order.items()])
            )
        return data


class Method(Base):
    path = "/leads"
