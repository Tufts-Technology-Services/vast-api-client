from pathlib import Path
from typing import Set, Optional
from enum import Enum, IntEnum
import re
from pydantic import (BaseModel, ConfigDict, Field, PositiveInt, field_validator,
                      model_validator, field_serializer, InstanceOf)


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
    SMBMigration = 6


class CloneTypeEnum(str, Enum):
    LOCAL = 'LOCAL'
    REMOTE = 'REMOTE'


class QuotaCreate(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    name: str
    path: Path
    soft_limit: Optional[PositiveInt] = None
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
    share: str = None # share name must end with '$'
    path: Path
    policy_id: InstanceOf[PolicyEnum] = None
    protocols: Set[InstanceOf[ProtocolEnum]] = {}
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
    
    @field_validator("policy_id")
    @classmethod
    def is_valid_policy_id(cls, policy_id: PolicyEnum) -> PolicyEnum:
        if policy_id is None:
            raise ValueError("policy_id must be set")
        return policy_id
    
    @field_validator("protocols")
    @classmethod
    def is_valid_protocols(cls, protocols: Set[ProtocolEnum]) -> Set[ProtocolEnum]:
        if len(protocols) == 0:
            raise ValueError("protocols must not be empty")
        else:
            return protocols

    @field_validator("share")
    @classmethod
    def is_valid_share_name(cls, share: str):
        if share is not None and not share.endswith("$"):
            raise ValueError("share_name must end with '$'")
        else:
            return share

    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)
    
    @model_validator(mode="after")
    def share_if_smb(self) -> 'ViewCreate':
        if ProtocolEnum.SMB in self.protocols and self.share is None:
            raise ValueError("SMB views require a share name")
        return self

class ProtectionPolicyFrame(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True, frozen=True)
    every: str
    start_at: str
    keep_local: str
    keep_remote: str = '0s'

    """
    {'every': '1D', 'start-at': '2024-01-22 17:00:00', 'keep-local': '2M', 'keep-remote': '0s'}
    """
    @field_validator("every")
    @classmethod
    def is_valid_every(cls, every: str) -> str:
        if every not in ['1D', '1W', '1M', '1Y']:
            raise ValueError("Invalid value for 'every'")
        return every
    
    @field_validator("start_at")
    @classmethod
    def is_valid_start_at(cls, start_at: str) -> str:
        return start_at
    
    @field_validator("keep_local")
    @classmethod
    def is_valid_keep_local(cls, keep_local: str) -> str:
        return keep_local
    
    @field_validator("keep_remote")
    @classmethod
    def is_valid_keep_remote(cls, keep_remote: str) -> str:
        return keep_remote
    

class ProtectionPolicyCreate(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True, frozen=True)
    name: str
    frames: list[ProtectionPolicyFrame]
    prefix: str
    clone_type: CloneTypeEnum = CloneTypeEnum.LOCAL
    indestructible: bool = False

    @field_serializer('frames')
    def serialize_frames(self, frames: list[ProtectionPolicyFrame], _info):
        return [frame.dict() for frame in frames]
    
    @field_serializer('clone_type')
    def serialize_clone_type(self, clone_type: CloneTypeEnum, _info):
        return clone_type.value
                
    @field_validator("name")
    @classmethod
    def is_valid_name(cls, name: str) -> str:
        return name
    
    @field_validator("frames")
    @classmethod
    def is_valid_frames(cls, frames: list[ProtectionPolicyFrame]) -> list[ProtectionPolicyFrame]:
        return frames
    
    @field_validator("prefix")
    @classmethod
    def is_valid_prefix(cls, prefix: str) -> str:
        return prefix
    
    @field_validator("clone_type")
    @classmethod
    def is_valid_clone_type(cls, clone_type: str) -> str:
        if clone_type not in ['LOCAL', 'REMOTE']:
            raise ValueError("Invalid value for 'clone_type'")
        return clone_type


class ProtectedPathCreate(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True, frozen=True)
    name: str
    source_dir: Path
    enabled: bool = True
    protection_policy_id: int
    tenant_id: int

    @field_serializer('source_dir')
    def serialize_source_dir(self, source_dir: Path, _info):
        return source_dir.as_posix()
    
    @field_validator("source_dir")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)
    

class PathBody(BaseModel):
    model_config = ConfigDict(extra='allow', str_strip_whitespace=True)
    path: Path

    @field_serializer('path')
    def serialize_path(self, path: Path, _info):
        return path.as_posix()

    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)


class FolderCreateOrUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True, frozen=True)
    path: Path
    owner_is_group: bool = False
    user: str = None
    group: str = None

    @field_serializer('path')
    def serialize_path(self, path: Path, _info):
        return path.as_posix()
    
    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)
    

class QuotaUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    soft_limit: Optional[PositiveInt] = None
    hard_limit: PositiveInt

    @model_validator(mode="after")
    def soft_limit_below_hard_limit(self) -> 'QuotaUpdate':
        if self.soft_limit is None:
            self.soft_limit = self.hard_limit
        if self.hard_limit < self.soft_limit:
            raise ValueError("'soft_limit' cannot be larger than 'hard_limit'")
        return self

