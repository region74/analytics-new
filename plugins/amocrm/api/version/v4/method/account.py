from enum import Enum
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

from ..base import Base


class DataGetWithEnum(Enum):
    amojo_id = "amojo_id"
    amojo_rights = "amojo_rights"
    users_groups = "users_groups"
    task_types = "task_types"
    version = "version"
    entity_names = "entity_names"
    datetime_settings = "datetime_settings"
    drive_url = "drive_url"
    is_api_filter_enabled = "is_api_filter_enabled"


class DataGet(BaseModel):
    with_: Optional[List[DataGetWithEnum]]

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        data = super().dict(*args, **kwargs)
        with_ = data.pop("with_", None)
        if with_ is not None:
            data["with"] = ",".join([item.value for item in with_])
        return data


class Method(Base):
    path = "/account"
