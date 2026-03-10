import base64
import os
import re
from datetime import datetime
from typing import List
from urllib.parse import quote

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from fastapi import HTTPException, status

load_dotenv()


class S3StorageService:
    DEFAULT_SECTION_PATHS = {
        "listening": ["audio", "images"],
        "reading": ["passages", "images"],
        "writing": ["prompts", "images"],
        "speaking": ["audio", "prompts"],
    }

    def __init__(self):
        access_key = os.getenv("AWS_ACCESS_KEY")
        secret_key = os.getenv("AWS_SECRET")
        region = os.getenv("AWS_REGION", "eu-north-1")
        bucket = os.getenv("AWS_BUCKET")

        if not access_key or not secret_key or not bucket:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="S3 is not configured. Set AWS_ACCESS_KEY, AWS_SECRET, AWS_BUCKET",
            )

        self.region = region
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

    @staticmethod
    def _safe_part(value: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(value).strip()).strip("-")
        return cleaned or "default"

    def _build_base_prefix(self, base_prefix: str, module_type: str, test_id: str) -> str:
        root = self._safe_part(base_prefix)
        module = self._safe_part(module_type).lower()
        test = self._safe_part(test_id)
        return f"{root}/ielts/{module}/{test}".strip("/")

    @staticmethod
    def _decode_base64(file_content_base64: str) -> bytes:
        raw = file_content_base64.strip()
        if "," in raw and "base64" in raw[:64].lower():
            raw = raw.split(",", 1)[1]
        try:
            return base64.b64decode(raw, validate=True)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file_content_base64: {exc}",
            )

    def _object_url(self, key: str) -> str:
        encoded_key = quote(key, safe="/-_.~")
        return f"https://{self.bucket}.s3.{self.region}.amazonaws.com/{encoded_key}"

    def create_question_bucket_structure(
        self,
        module_type: str,
        test_id: str,
        base_prefix: str = "questions",
        sections: List[str] | None = None,
    ) -> dict:
        root_prefix = self._build_base_prefix(base_prefix, module_type, test_id)
        requested_sections = sections or list(self.DEFAULT_SECTION_PATHS.keys())

        created_keys: List[str] = []
        try:
            self.client.put_object(Bucket=self.bucket, Key=f"{root_prefix}/", Body=b"")
            created_keys.append(f"{root_prefix}/")

            for section in requested_sections:
                section_name = self._safe_part(section).lower()
                sub_paths = self.DEFAULT_SECTION_PATHS.get(section_name, ["files"])
                for sub in sub_paths:
                    key = f"{root_prefix}/{section_name}/{self._safe_part(sub).lower()}/"
                    self.client.put_object(Bucket=self.bucket, Key=key, Body=b"")
                    created_keys.append(key)
        except ClientError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to create S3 structure: {exc}",
            )

        return {
            "bucket": self.bucket,
            "base_prefix": root_prefix,
            "created_keys": created_keys,
        }

    def upload_question_file(
        self,
        module_type: str,
        test_id: str,
        section: str,
        file_name: str,
        file_content_base64: str,
        content_type: str | None = None,
        base_prefix: str = "questions",
        sub_path: str | None = None,
    ) -> dict:
        if not file_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="file_name is required")

        file_bytes = self._decode_base64(file_content_base64)
        if len(file_bytes) == 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File content is empty")

        root_prefix = self._build_base_prefix(base_prefix, module_type, test_id)
        section_name = self._safe_part(section).lower()
        safe_name = self._safe_part(file_name)

        sub_dir = self._safe_part(sub_path).lower() if sub_path else "files"
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        key = f"{root_prefix}/{section_name}/{sub_dir}/{timestamp}-{safe_name}"

        put_args = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": file_bytes,
        }
        if content_type:
            put_args["ContentType"] = content_type

        try:
            self.client.put_object(**put_args)
        except ClientError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to upload file to S3: {exc}",
            )

        return {
            "bucket": self.bucket,
            "key": key,
            "url": self._object_url(key),
            "size": len(file_bytes),
            "content_type": content_type,
        }
