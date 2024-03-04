from enum import Enum
from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, PositiveInt, NonNegativeInt, conint

from ..base import (
    Base,
)


class DataGetWithEnum(Enum):
    catalog_elements = "catalog_elements"
    is_price_modified_by_robot = "is_price_modified_by_robot"
    loss_reason = "loss_reason"
    contacts = "contacts"
    only_deleted = "only_deleted"
    source_id = "source_id"


class DataGet(BaseModel):
    with_: Optional[List[DataGetWithEnum]]

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


class DataPatch(BaseModel):
    update_data: Optional[Dict[str, Any]]

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        data = super().dict(*args, **kwargs)
        update_data = data.pop("update_data", None)
        if update_data is not None:
            return update_data
        return data


class Method(Base):
    path = "/leads"
    __lead_id: PositiveInt

    @property
    def lead_id(self) -> PositiveInt:
        return self.__lead_id

    @lead_id.setter
    def lead_id(self, lead_id: PositiveInt):
        self.__lead_id = lead_id

    def get_path(self) -> str:
        return f'{super(Method, self).get_path()}/{self.lead_id}'
