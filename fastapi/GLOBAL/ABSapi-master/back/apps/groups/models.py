from pydantic import BaseModel, Field

from models import PyObjectId


class GroupResponseSchema(BaseModel):
    id: PyObjectId = Field(alias='_id')
    card_ids: list[str]


class GroupListResponseSchema(BaseModel):
    status: str
    groups: list[GroupResponseSchema]
    pages: str
