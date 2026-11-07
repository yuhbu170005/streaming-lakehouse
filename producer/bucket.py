import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError


def main():
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        config=Config(connect_timeout=2, read_timeout=3, retries={"max_attempts": 0}),
    )
    for attempt in range(60):
        try:
            client.create_bucket(Bucket="lakehouse")
            return
        except ClientError as error:
            if error.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                return
            raise
        except EndpointConnectionError:
            if attempt == 59:
                raise
            time.sleep(2)


if __name__ == "__main__":
    main()
