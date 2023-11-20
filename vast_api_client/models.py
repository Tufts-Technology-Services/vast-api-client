from pathlib import Path

from pydantic import (BaseModel, PositiveInt, InstanceOf, model_validator,
                      field_validator, field_serializer, ConfigDict)
from typing import Set
from enum import Enum, IntEnum
import re


def validate_path(path: Path):
    # Regular expression pattern for Unix paths
    pattern = r'^/([A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+$'

    # Use re.match to check if the path matches the pattern
    if re.match(pattern, path.as_posix()):
        return path
    else:
        raise ValueError("invalid path")


class ProtocolEnum(str, Enum):
    SMB = 'SMB'
    NFS = 'NFS'


class PolicyEnum(IntEnum):
    SMBDefault = 5


class QuotaCreate(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    name: str
    path: Path
    soft_limit: PositiveInt = None
    hard_limit: PositiveInt
    create_dir: bool = False

    @field_serializer('path')
    def serialize_path(self, path: Path, _info):
        return path.as_posix()

    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)

    @model_validator(mode="after")
    def soft_limit_below_hard_limit(self) -> 'QuotaCreate':
        if self.soft_limit is None:
            self.soft_limit = self.hard_limit
        if self.hard_limit < self.soft_limit:
            raise ValueError("'soft_limit' cannot be larger than 'hard_limit'")
        return self


class ViewCreate(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True, frozen=True)
    share: str
    path: Path
    policy_id: InstanceOf[PolicyEnum] = PolicyEnum.SMBDefault
    protocols: Set[InstanceOf[ProtocolEnum]] = {ProtocolEnum.SMB}
    create_dir: bool = True

    @field_serializer('path')
    def serialize_path(self, path: Path, _info):
        return path.as_posix()

    @field_serializer('policy_id')
    def serialize_policy_id(self, policy_id: PolicyEnum, _info):
        return policy_id.value

    @field_serializer('protocols')
    def serialize_protocols(self, protocols: Set[ProtocolEnum], _info):
        return [i.value for i in protocols]

    @field_validator("share")
    @classmethod
    def is_valid_share_name(cls, share: str):
        if not share.endswith("$"):
            raise ValueError("share_name must end with '$'")
        else:
            return share

    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)
