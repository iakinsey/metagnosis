from datetime import datetime
from pydantic import BaseModel


class PDF(BaseModel):
    id: str
    path: str
    processed: bool
    created: datetime
    updated: datetime