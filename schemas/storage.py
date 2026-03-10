from pydantic import BaseModel, Field
from typing import List, Optional


class S3StructureCreateRequest(BaseModel):
    module_type: str = Field(default="academic")
    test_id: str
    sections: Optional[List[str]] = None
    base_prefix: str = Field(default="questions")


class S3QuestionFileUploadRequest(BaseModel):
    module_type: str = Field(default="academic")
    test_id: str
    section: str
    file_name: str
    file_content_base64: str
    content_type: Optional[str] = None
    base_prefix: str = Field(default="questions")
    sub_path: Optional[str] = None
