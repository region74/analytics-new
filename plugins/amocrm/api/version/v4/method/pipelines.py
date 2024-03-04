from pydantic import BaseModel

from ..base import Base


class DataGet(BaseModel):
    pass


class Method(Base):
    path = "/leads/pipelines"
