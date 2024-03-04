from enum import Enum
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, NonNegativeInt, conint

from ..base import Base


class DataGetWithEnum(Enum):
    role = "role"
    group = "group"
    uuid = "uuid"
    amojo_id = "amojo_id"
    user_rank = "user_rank"


class DataGet(BaseModel):
    with_: Optional[List[DataGetWithEnum]]
    page: Optional[NonNegativeInt]
    limit: Optional[conint(ge=1, le=250)]

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        data = super().dict(*args, **kwargs)
        with_ = data.pop("with_", None)
        if with_ is not None:
            data["with"] = ",".join([item.value for item in with_])
        return data


class Method(Base):
    path = "/users"
