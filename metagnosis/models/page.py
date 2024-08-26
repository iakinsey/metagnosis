from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class Page(BaseModel):
    id: str
    title: str
    url: str
    score: int
    text: str
    error: Optional[str]
    processed: bool
    created: datetime
    updated: datetime