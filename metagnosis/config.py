from pydantic import BaseModel
from os import getcwd, makedirs
from os.path import join


class Config(BaseModel):
    storage_path: str
    queue_path: str
    user_agent: str


def get_config():
    data_path = join(getcwd(), "data")

    makedirs(data_path, exist_ok=True)

    return Config(
        storage_path=join(data_path, "storage"),
        queue_path=join(data_path, "mg.db"),
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )