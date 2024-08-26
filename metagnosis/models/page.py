from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class Page(BaseModel):
    id: str
    title: str
    url: str
    score: int
    path: str
    error: Optional[str]
    processed: bool
    created: datetime
    updated: datetime