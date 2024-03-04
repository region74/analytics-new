from typing import List, Dict, Any, Optional, Union
from pydantic import BaseModel, NonNegativeInt, conint

from ..base import Base, NestedBaseModel


class DataGetFilter(NestedBaseModel):
    id: Optional[Union[NonNegativeInt, List[NonNegativeInt]]]
    name: Optional[Union[str, List[str]]]


class DataGet(BaseModel):
    page: Optional[NonNegativeInt]
    limit: Optional[conint(ge=1, le=250)]
    query: Optional[str]
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
    path = "/leads/tags"
