from datetime import datetime
from typing import List
from pydantic import BaseModel
from .pdf import PDF

class Document(BaseModel):
    id: str
    path: str
    text: str
    title: str
    categories: List[str]
    vector: List[float]
    processed: bool
    created: datetime
    updated: datetime

    @classmethod
    def from_pdf(cls, pdf: PDF):
        now = datetime.now()

        return cls.model_construct(
            id=pdf.id,
            path=pdf.path,
            processed=False,
            created=now(),
            updated=now()
        )