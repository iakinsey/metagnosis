from pydantic import BaseModel
from os import getcwd, makedirs
from os.path import join


class PublishCredentials(BaseModel):
    shipping_level: str
    email: str
    package_id: str
    name: str
    street1: str
    street2: str
    city: str
    state_code: str
    country_code: str
    postcode: str
    phone_number: str


class Config(BaseModel):
    storage_path: str
    db_path: str
    job_path: str
    user_agent: str
    publish_creds: PublishCredentials


def get_config():
    data_path = join(getcwd(), "data")

    makedirs(data_path, exist_ok=True)

    return Config(
        storage_path=join(data_path, "storage"),
        db_path=join(data_path, "mg.db"),
        job_path=join(data_path, "jobs.db"),
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    )

