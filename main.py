import logging
import os
import random
import string

import boto3
import botocore.client
import botocore.exceptions
import fastapi
import fastapi.responses
import pydantic
import toml


class Settings(pydantic.BaseSettings):
    access_key: str
    secret_key: str
    endpoint_url: str
    bucket: str


class S3Helper():
    def __init__(self, access_key, secret_key, endpoint_url):
        self.access_key = settings.access_key
        self.secret_key = settings.secret_key
        self.endpoint_url = settings.endpoint_url

        self.client = self.get_client()

    def get_client(self):
        session = boto3.session.Session()
        return session.client(
            service_name="s3",
            aws_access_key_id=settings.access_key,
            aws_secret_access_key=settings.secret_key,
            endpoint_url=settings.endpoint_url,
        )

    def ping(self):
        # Simple connectivity test
        try:
            self.client.head_bucket(Bucket=settings.bucket)
            return True
        except botocore.exceptions.ClientError:
            return False


def generate_padding(count):
    alphabeth = "ABCDEF1234567890"
    return ("".join([random.choice(alphabeth) for _ in range(35)]) + ":0" for _ in range(count))


def is_valid_hash(hash):
    return len(hash) == 5 and all(c in string.hexdigits for c in hash)


logger = logging.getLogger("uvicorn")
config_path = os.getenv("PWNED_PASSWORDS_CONFIG")
config = toml.load(config_path)
settings = Settings(**config)

s3 = S3Helper(
    access_key=settings.access_key,
    secret_key=settings.secret_key,
    endpoint_url=settings.endpoint_url,
)

if not s3.ping():
    raise Exception("Unable to communicate with S3")

app = fastapi.FastAPI()


@app.get("/range/{hash}")
def get_range(hash: str):
    if not is_valid_hash(hash):
        raise fastapi.HTTPException(status_code=400, detail="Invalid hash")

    hash = hash.upper()

    try:
        obj = s3.client.get_object(Bucket=settings.bucket, Key=hash)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            # This is not supposed to happen, but technically it could
            # https://haveibeenpwned.com/API/v3#SearchingPwnedPasswordsByRange
            raise fastapi.HTTPException(status_code=404) from None
        else:
            raise

    content = obj["Body"].read().decode("ascii").strip().split("\n")

    # Always pad the result
    if len(content) <= 800:
        # Fewer than 800 should pad to between 800-1000
        count = random.randint(800 - len(content), 1000 - len(content))
        padding = generate_padding(count)
    elif len(content) > 800:
        # More than 800 should supposedly never occur?
        # If there are more than 800 we're supposed to not pad, or?
        # We're adding some padding
        # https://haveibeenpwned.com/API/v3#PwnedPasswordsPadding
        logger.warning(f"Dataset contains more than 800 entries: {hash}")
        padding = generate_padding(random.randint(0, 200))

    content.extend(padding)

    plain_text = "\n".join(content)

    return fastapi.responses.PlainTextResponse(plain_text)


@app.get("/health")
def get_health():
    return {"healthy": s3.ping()}
