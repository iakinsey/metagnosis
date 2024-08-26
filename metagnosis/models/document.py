from datetime import datetime
from typing import List
from pydantic import BaseModel
from .page import Page
from .pdf import PDF

class Document(BaseModel):
    id: str
    path: str
    data_type: str
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
            data_type="pdf",
            processed=False,
            created=now(),
            updated=now()
        )
    
    @classmethod
    def from_page(cls, page: Page):
        now = datetime.now()

        return cls.model_construct(
            id=page.id,
            path=page.path, # TODO
            data_type="page",
            processed=False,
            created=now(),
            updated=now()
        )