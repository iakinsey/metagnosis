from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from pymupdf import open as open_pdf


class PDF(BaseModel):
    id: str
    path: str
    url: str
    origin: str
    title: Optional[str] = None
    score: Optional[int] = None
    error: Optional[str] = None
    created: datetime
    updated: datetime
    processed: bool
    text: Optional[str] = None

    def hydrate_text(self):
        text = ""

        with open_pdf(self.path) as f:
            for page_num in range(len(f)):
                page = f[page_num]
                text += page.get_text()

        self.text = text
