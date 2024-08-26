from asyncio import get_running_loop
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
from sentence_transformers import SentenceTransformer
from ..log import log


class EncoderGateway:
    def __init__(self):
        self.model_name = "dunzhang/stella_en_1.5B_v5"
        self.model = SentenceTransformer(self.model_name)
        self.pool = ThreadPoolExecutor(max_workers=4)

    async def encode(self, values: List[Tuple[str, str]]) -> List[Tuple[str, List[float]]]:
        log.info(f"Encoding {len(values)} entities")

        loop = get_running_loop()
        ids = [i[0] for i in values]
        strings = [i[1] for i in values]

        result = await loop.run_in_executor(
            self.pool,
            self.model.encode,
            strings 
        )

        return list(zip(ids, result.tolist()))
