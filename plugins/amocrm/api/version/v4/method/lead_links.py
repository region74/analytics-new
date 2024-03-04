from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, NonNegativeInt

from ..base import (
    Base, NestedBaseModel,
)


class DataGetFilter(NestedBaseModel):
    to_entity_id: Optional[Union[NonNegativeInt, List[NonNegativeInt]]]
    to_entity_type: Optional[Union[str, List[str]]]
    to_catalog_id: Optional[Union[NonNegativeInt, List[NonNegativeInt]]]


class DataGet(BaseModel):
    filter: Optional[DataGetFilter]

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        data = super().dict(*args, **kwargs)
        filter_ = data.pop("filter", None)
        if filter_ is not None:
            data.update(
                **dict(
                    [(f"filter{key}", value) for key, value in filter_.items()]
                )
            )
        return data


class Method(Base):
    path = "/leads"

    def get_path(self):
        return f'{super().get_path()}/{self.client.kwargs.get("to_entity_id")}/links'
