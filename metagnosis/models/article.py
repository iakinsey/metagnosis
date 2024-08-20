from datetime import datetime
from typing import List


class Article:
    title: str
    origin: str
    vector: List[float]
    created: datetime
    category: str