from pydantic import BaseModel, PositiveInt, DirectoryPath
from typing import Set
from enum import Enum, IntEnum


class ProtocolEnum(str, Enum):
    SMB = 'SMB'
    NFS = 'NFS'


class PolicyEnum(IntEnum):
    SMBDefault = 5


class QuotaCreate(BaseModel):
    name: str
    path: DirectoryPath
    soft_limit: PositiveInt
    hard_limit: PositiveInt


class ViewCreate(BaseModel):
    share: str
    path: DirectoryPath
    policy_id: PolicyEnum = PolicyEnum.SMBDefault
    protocols: Set[ProtocolEnum] = ProtocolEnum.SMB

