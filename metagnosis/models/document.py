from datetime import datetime
from typing import List
from pydantic import BaseModel
from .pdf import PDF

class Document(BaseModel):
    id: str
    path: str
    origin: str
    data_type: str
    path: str
    score: int
    categories: List[str]
    vector: List[float]
    processed: bool
    created: datetime
    updated: datetime
    text: str

    @classmethod
    def from_pdf(cls, pdf: PDF):
        now = datetime.now()

        return cls.model_construct(
            id=pdf.id,
            path=pdf.path,
            data_type="pdf",
            origin=pdf.origin,
            score=pdf.score or 0,
            processed=False,
            text=pdf.text,
            created=now,
            updated=now
        )