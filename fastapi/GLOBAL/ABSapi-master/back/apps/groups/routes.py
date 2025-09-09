import math
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Query, status

from apps.groups.models import GroupListResponseSchema
from models import CommaSeparatedList, PyObjectId
from user.oauth2 import require_user

group_router = APIRouter()


@group_router.get('', response_model=GroupListResponseSchema)
async def get_groups(
    request: Request,
    limit: int = 40,
    page: int = 1,
    group_ids: Annotated[CommaSeparatedList[PyObjectId] | None, Query()] = None,
    user_id: str = Depends(require_user),
):
    filter_query = []
    if group_ids:
        filter_query.append({'_id': {'$in': group_ids}})
    groups_collection = request.app.database.central_groups
    groups = await (groups_collection.find(*filter_query).skip(limit * (page - 1)).limit(limit)).to_list()
    if groups:
        groups_count = await (groups_collection.estimated_document_count())
        pages = math.ceil(groups_count / limit)
        return {'status': 'success', 'groups': groups, 'pages': str(pages)}
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Not found',
        )
