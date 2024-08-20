from pydantic import BaseModel
from os import getcwd, makedirs
from os.path import join


class Config(BaseModel):
    storage_path: str
    queue_path: str


def get_config():
    data_path = join(getcwd(), "data")

    makedirs(data_path, exist_ok=True)

    return Config(
        storage_path=join(data_path, "storage"),
        queue_path=join(data_path, "queue")
    )