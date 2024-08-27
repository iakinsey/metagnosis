from asyncio import get_running_loop
from concurrent.futures import ThreadPoolExecutor
from json import loads
from typing import Any
from transformers import pipeline, Pipeline
from ..models.metadata import Metadata

METADATA_PROMPT = '''
Given a body of text, extract the title and tags related to it. Try to keep tag
values down to a single word. Use high level categories as much as possible and
limit the tag list to a maximum of 5 tags.

Return the data in json in the following format:
{
    "title": "title",
    "tags": ["tag1", "tag2"]
}
'''

class LLMGateway:
    model_name: str
    model_parameters: dict[str, Any] 
    pipe: Pipeline
    pool: ThreadPoolExecutor

    def __init__(self):
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.model_name = "microsoft/Phi-3.5-mini-instruct"
        self.model_parameters = {
            'max_new_tokens': 150,
            'return_full_text': True,
            'temperature': 0.0,
            'do_sample': False
        }
        self.pipe = pipeline(
            "text-generation",
            model=self.model_name,
            trust_remote_code=True
        )

    async def extract_metadata(self, text: str) -> Metadata:
        loop = get_running_loop()
        messages = [
            {"role": "system", "content": METADATA_PROMPT},
            {"role": "user", "content": text}
        ]
        result = loop.run_in_executor(
            self.pool,
            self.pipe,
            messages,
            **self.model_parameters
        )
        message = result[-1]

        if message['role'] != "assistant":
            raise TypeError("Response was not from assistant")
        
        contents = message['content'].replace("```", "").strip()
        message = loads(contents)

        return Metadata(**message)
