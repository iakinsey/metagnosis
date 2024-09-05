from enum import Enum
from typing import Any, Coroutine, Optional
from pydantic import BaseModel


class TaskCategory:
    VECTORIZE_TEXT = "vectorize_text"
    EXTRACT_METADATA = "extract_metadata"


class Task:
    id: Optional[str]
    result: Any
    category: TaskCategory
    success: bool
    error: Optional[Exception]

    @classmethod
    async def wrap(
        cls, coro: Coroutine[Any, Any, Any], category: TaskCategory, id: str = None
    ):
        params = {
            "id": id,
            "category": category,
        }
        try:
            params["result"] = await coro
            params["success"] = True
        except Exception as e:
            params["success"] = False
            params["error"] = e

        return cls(**params)
