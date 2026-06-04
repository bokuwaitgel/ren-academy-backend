import base64
import os
import re
from datetime import datetime, timezone
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

    # File extensions classified for the reusable media library.
    AUDIO_EXTS = {".mp3", ".mpeg", ".mpga", ".wav", ".m4a", ".aac", ".ogg", ".oga", ".webm", ".flac"}
    IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".avif"}
    _CONTENT_TYPES = {
        ".mp3": "audio/mpeg", ".mpeg": "audio/mpeg", ".mpga": "audio/mpeg",
        ".wav": "audio/wav", ".m4a": "audio/mp4", ".aac": "audio/aac",
        ".ogg": "audio/ogg", ".oga": "audio/ogg", ".webm": "audio/webm", ".flac": "audio/flac",
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
        ".gif": "image/gif", ".webp": "image/webp", ".svg": "image/svg+xml",
        ".bmp": "image/bmp", ".avif": "image/avif",
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

    def _safe_prefix(self, prefix: str) -> str:
        """Sanitize a multi-segment S3 prefix, preserving '/' separators."""
        segments = [self._safe_part(seg) for seg in str(prefix).split("/") if seg.strip()]
        return "/".join(segments)

    @classmethod
    def _classify(cls, ext: str) -> str | None:
        if ext in cls.AUDIO_EXTS:
            return "audio"
        if ext in cls.IMAGE_EXTS:
            return "images"
        return None

    def list_media(
        self,
        kind: str | None = None,
        search: str | None = None,
        prefix: str = "questions",
        limit: int = 200,
    ) -> dict:
        """List previously uploaded media objects under a prefix, for reuse.

        Classifies each object as 'audio' or 'images' by file extension. Supports
        an optional `kind` filter and a case-insensitive `search` over the file
        name/key. Returns the most recently modified items first.
        """
        base = self._safe_prefix(prefix) or "questions"
        list_prefix = f"{base}/"
        kind_filter = (kind or "").strip().lower() or None
        needle = (search or "").strip().lower() or None
        try:
            limit = max(1, min(int(limit), 1000))
        except (TypeError, ValueError):
            limit = 200

        items: List[dict] = []
        token: str | None = None
        scanned = 0
        try:
            while True:
                kwargs: dict = {"Bucket": self.bucket, "Prefix": list_prefix, "MaxKeys": 1000}
                if token:
                    kwargs["ContinuationToken"] = token
                resp = self.client.list_objects_v2(**kwargs)

                for obj in resp.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith("/"):
                        continue  # folder placeholder
                    size = int(obj.get("Size", 0))
                    if size == 0:
                        continue
                    filename = key.rsplit("/", 1)[-1]
                    ext = ("." + filename.rsplit(".", 1)[-1].lower()) if "." in filename else ""
                    item_kind = self._classify(ext)
                    if item_kind is None:
                        continue
                    if kind_filter and item_kind != kind_filter:
                        continue
                    if needle and needle not in filename.lower() and needle not in key.lower():
                        continue
                    last_modified = obj.get("LastModified")
                    items.append({
                        "key": key,
                        "url": self._object_url(key),
                        "filename": filename,
                        "size": size,
                        "kind": item_kind,
                        "content_type": self._CONTENT_TYPES.get(ext),
                        "last_modified": last_modified.isoformat() if last_modified else None,
                    })

                scanned += len(resp.get("Contents", []))
                token = resp.get("NextContinuationToken")
                if not token or scanned >= 5000:
                    break
        except ClientError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to list S3 media: {exc}",
            )

        items.sort(key=lambda it: it["last_modified"] or "", reverse=True)
        return {"items": items[:limit], "count": len(items)}

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

    def upload_bytes(
        self,
        module_type: str,
        test_id: str,
        section: str,
        file_name: str,
        file_bytes: bytes,
        content_type: str | None = None,
        base_prefix: str = "questions",
        sub_path: str | None = None,
    ) -> dict:
        if not file_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="file_name is required")
        if len(file_bytes) == 0:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="File content is empty")

        root_prefix = self._build_base_prefix(base_prefix, module_type, test_id)
        section_name = self._safe_part(section).lower()
        safe_name = self._safe_part(file_name)

        sub_dir = self._safe_part(sub_path).lower() if sub_path else "files"
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        key = f"{root_prefix}/{section_name}/{sub_dir}/{timestamp}-{safe_name}"

        put_args: dict = {"Bucket": self.bucket, "Key": key, "Body": file_bytes}
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

    def upload_speaking_audio(
        self,
        mode: str,
        session_id: str,
        user_id: str,
        question_id: str,
        index: int,
        file_bytes: bytes,
        ext: str = "webm",
        content_type: str | None = None,
        test_id: str | None = None,
    ) -> dict:
        """Upload speaking audio with standardised path.

        Test session: speaking/{mode}/{session_id}_{test_id}_{user_id}_{question_id}_{index}.{ext}
        Practice:     speaking/practice/{session_id}_{user_id}_{question_id}_{index}.{ext}
        """
        safe = self._safe_part
        safe_mode = safe(mode).lower()
        safe_ext = safe(ext).lower() or "webm"

        if test_id:
            file_name = f"{safe(session_id)}_{safe(test_id)}_{safe(user_id)}_{safe(question_id)}_{index}.{safe_ext}"
        else:
            file_name = f"{safe(session_id)}_{safe(user_id)}_{safe(question_id)}_{index}.{safe_ext}"

        key = f"speaking/{safe_mode}/{file_name}"

        put_args: dict = {"Bucket": self.bucket, "Key": key, "Body": file_bytes}
        if content_type:
            put_args["ContentType"] = content_type

        try:
            self.client.put_object(**put_args)
        except ClientError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to upload speaking audio to S3: {exc}",
            )

        return {
            "bucket": self.bucket,
            "key": key,
            "url": self._object_url(key),
            "size": len(file_bytes),
            "content_type": content_type,
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
        file_bytes = self._decode_base64(file_content_base64)
        return self.upload_bytes(
            module_type=module_type,
            test_id=test_id,
            section=section,
            file_name=file_name,
            file_bytes=file_bytes,
            content_type=content_type,
            base_prefix=base_prefix,
            sub_path=sub_path,
        )
