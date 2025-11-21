import pickle
from contextlib import contextmanager
from typing import Any, Iterator, Optional

import boto3
from dagster import InputContext, IOManager, OutputContext


class S3IOManager(IOManager):
    def __init__(self, s3_bucket: str, s3_prefix: Optional[str] = None):
        self.bucket = s3_bucket
        self.prefix = s3_prefix or "dagster"
        self.s3 = boto3.client("s3")

    def _get_path(self, context: Any) -> str:
        parts = context.asset_key.path
        return "/".join([self.prefix] + parts)

    def handle_output(self, context: OutputContext, obj: Any):
        key = self._get_path(context)
        context.log.info(f"Writing output to s3://{self.bucket}/{key}")
        
        # Serialize object to bytes
        try:
            obj_data = pickle.dumps(obj)
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=obj_data)
        except Exception as e:
            raise RuntimeError(f"Failed to write to S3: {e}")

    def load_input(self, context: InputContext) -> Any:
        key = self._get_path(context)
        context.log.info(f"Loading input from s3://{self.bucket}/{key}")

        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            return pickle.loads(obj["Body"].read())
        except Exception as e:
            raise RuntimeError(f"Failed to load from S3: {e}")
