from enum import Enum
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

from ..base import (
    Base,
)


class DataGetWithEnum(Enum):
    catalog_elements = "catalog_elements"
    leads = "leads"
    customers = "customers"


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


class Method(Base):
    path = "/contacts"

    def get_path(self):
        return f'{super().get_path()}/{self.client.kwargs.get("contact_id")}'
