from pydantic import BaseModel
from os import getcwd, makedirs
from os.path import join

data_path = join(getcwd(), "data")

class PublishCredentials(BaseModel):
    email: str
    package_id: str = "0850X1100FCSTDPB080CW444GXX"
    name: str
    street1: str
    street2: str
    city: str
    state_code: str
    country_code: str
    postcode: str
    phone_number: str


class Config(BaseModel):
    storage_path: str = data_path
    db_path: str = join(data_path, "mg.db")
    job_path: str = join(data_path, "jobs.db")
    user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    aws_access_key_id: str
    aws_secret_access_key: str
    s3_bucket: str
    publish_creds: PublishCredentials


def get_config() -> Config:
    config_json = open(join(getcwd(), "config.json"), 'r').read().strip()
    config = Config.model_validate_json(config_json)

    makedirs(config.storage_path, exist_ok=True)

    return config