from typing import List
from pydantic import BaseModel

class Metadata(BaseModel):
    title: str
    tags: List[str]