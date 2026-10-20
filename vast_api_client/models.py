import re
from enum import UNIQUE, StrEnum, verify
from pathlib import Path
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_serializer,
    field_validator,
    model_serializer,
    model_validator,
)


def validate_path(path: Path):
    # Regular expression pattern for Unix paths
    pattern = r"^/([A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+$"

    # Use re.match to check if the path matches the pattern
    if re.match(pattern, path.as_posix()):
        return path
    else:
        raise ValueError("invalid path")


@verify(UNIQUE)
class ProtocolEnum(StrEnum):
    SMB = "SMB"
    NFS = "NFS"


@verify(UNIQUE)
class ACLPerm(StrEnum):
    READ = "READ"
    CHANGE = "CHANGE"
    FULL = "FULL"


@verify(UNIQUE)
class ACLGrantee(StrEnum):
    USERS = "users"
    GROUPS = "groups"


@verify(UNIQUE)
class CloneTypeEnum(StrEnum):
    LOCAL = "LOCAL"
    REMOTE = "REMOTE"


# Operators bigcatalogconfig/query_data accepts in a filter condition. eq and startswith are verified
# against VAST 5.4.4 and 5.5 in the VAST KB; gt, ge and lt appear in its catalog examples, and le is
# assumed alongside them. contains and gte are documented as rejected (400), so they are left out.
CATALOG_FILTER_OPERATORS = frozenset({"eq", "startswith", "gt", "ge", "lt", "le"})


class QuotaCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    name: str
    path: Path
    soft_limit: PositiveInt | None = None
    hard_limit: PositiveInt
    create_dir: bool = False

    @field_serializer("path")
    def serialize_path(self, path: Path, _info):
        return path.as_posix()

    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)

    @model_validator(mode="after")
    def soft_limit_below_hard_limit(self) -> "QuotaCreate":
        if self.soft_limit is None:
            self.soft_limit = self.hard_limit
        if self.hard_limit < self.soft_limit:
            raise ValueError("'soft_limit' cannot be larger than 'hard_limit'")
        return self


class ACL(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    perm: ACLPerm
    grantee: ACLGrantee
    sid_str: str
    # None for SMB-only groups, which have no POSIX gid; left out of the body rather than sent as null
    uid_or_gid: int | None = None
    group_type: ProtocolEnum = ProtocolEnum.SMB

    @field_validator("uid_or_gid")
    @classmethod
    def is_valid_uid_or_gid(cls, uid_or_gid: int | None) -> int | None:
        if uid_or_gid is not None and uid_or_gid < 0:
            raise ValueError("uid_or_gid must be non-negative")
        return uid_or_gid

    @model_validator(mode="after")
    def all_fields_present(self) -> "ACL":
        if not all([self.perm, self.grantee, self.sid_str]):
            raise ValueError("perm, grantee and sid_str must be present and non-empty")
        return self

    @model_serializer(mode="wrap")
    def omit_missing_uid_or_gid(self, handler):
        data = handler(self)
        if self.uid_or_gid is None:
            data.pop("uid_or_gid", None)
        return data

    def sort_key(self) -> tuple:
        return (
            self.grantee,
            self.sid_str,
            self.perm,
            self.group_type,
            -1 if self.uid_or_gid is None else self.uid_or_gid,
        )


class ShareCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    share: str | None = None  # share name must end with '$'
    path: Path
    policy_id: int
    # validate_default, so leaving protocols out fails like passing an empty set
    protocols: frozenset[ProtocolEnum] = Field(default=frozenset(), validate_default=True)
    create_dir: bool = True
    inherit_acl: bool = True
    create_dir_acls: frozenset[ACL] = frozenset()

    @field_serializer("path")
    def serialize_path(self, path: Path, _info):
        return path.as_posix()

    # sorted, so the same view always produces the same body
    @field_serializer("protocols")
    def serialize_protocols(self, protocols: frozenset[ProtocolEnum], _info):
        return sorted(protocol.value for protocol in protocols)

    @field_serializer("create_dir_acls")
    def serialize_create_dir_acls(self, create_dir_acls: frozenset[ACL], _info):
        return [acl.model_dump(mode="json") for acl in sorted(create_dir_acls, key=ACL.sort_key)]

    @field_validator("protocols")
    @classmethod
    def is_valid_protocols(cls, protocols: frozenset[ProtocolEnum]) -> frozenset[ProtocolEnum]:
        if len(protocols) == 0:
            raise ValueError("protocols must not be empty")
        else:
            return protocols

    @field_validator("create_dir_acls", mode="before")
    @classmethod
    def no_acls_if_none(cls, create_dir_acls):
        return frozenset() if create_dir_acls is None else create_dir_acls

    @field_validator("share")
    @classmethod
    def is_valid_share_name(cls, share: str | None):
        if share is not None and not share.endswith("$"):
            raise ValueError("share_name must end with '$'")
        else:
            return share

    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)

    @model_validator(mode="after")
    def share_if_smb(self) -> "ShareCreate":
        if ProtocolEnum.SMB in self.protocols and self.share is None:
            raise ValueError("SMB views require a share name")
        return self


class ViewCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    path: Path
    policy_id: int
    # validate_default, so leaving protocols out fails like passing an empty set
    protocols: frozenset[ProtocolEnum] = Field(default=frozenset(), validate_default=True)
    create_dir: bool = True

    @field_serializer("path")
    def serialize_path(self, path: Path, _info):
        return path.as_posix()

    # sorted, so the same view always produces the same body
    @field_serializer("protocols")
    def serialize_protocols(self, protocols: frozenset[ProtocolEnum], _info):
        return sorted(protocol.value for protocol in protocols)

    @field_validator("protocols")
    @classmethod
    def is_valid_protocols(cls, protocols: frozenset[ProtocolEnum]) -> frozenset[ProtocolEnum]:
        if len(protocols) == 0:
            raise ValueError("protocols must not be empty")
        else:
            return protocols

    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)


class ProtectionPolicyFrame(BaseModel):
    """
    VAST names the fields with hyphens; build with underscores, dump with by_alias=True to send:
    {'every': '1D', 'start-at': '2024-01-22 17:00:00', 'keep-local': '2M', 'keep-remote': '0s'}
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True, populate_by_name=True)
    every: str
    start_at: str = Field(alias="start-at")
    keep_local: str = Field(alias="keep-local")
    keep_remote: str = Field(default="0s", alias="keep-remote")

    @field_validator("every")
    @classmethod
    def is_valid_every(cls, every: str) -> str:
        if every not in ["1D", "1W", "1M", "1Y"]:
            raise ValueError("Invalid value for 'every'")
        return every


class ProtectionPolicyCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    name: str
    frames: list[ProtectionPolicyFrame]
    prefix: str
    clone_type: CloneTypeEnum = CloneTypeEnum.LOCAL
    indestructible: bool = False

    @field_serializer("frames")
    def serialize_frames(self, frames: list[ProtectionPolicyFrame], _info):
        return [frame.model_dump(mode="json", by_alias=True) for frame in frames]


class ProtectedPathCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    name: str
    source_dir: Path
    enabled: bool = True
    protection_policy_id: int
    tenant_id: int

    @field_serializer("source_dir")
    def serialize_source_dir(self, source_dir: Path, _info):
        return source_dir.as_posix()

    @field_validator("source_dir")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)


class PathBody(BaseModel):
    model_config = ConfigDict(extra="allow", str_strip_whitespace=True)
    path: Path

    @field_serializer("path")
    def serialize_path(self, path: Path, _info):
        return path.as_posix()

    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)


class FolderCreateOrUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    path: Path
    owner_is_group: bool = False
    user: str | None = None
    group: str | None = None

    @field_serializer("path")
    def serialize_path(self, path: Path, _info):
        return path.as_posix()

    @field_validator("path")
    @classmethod
    def is_valid_unix_path(cls, path: Path) -> Path:
        return validate_path(path)


class QuotaUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    soft_limit: PositiveInt | None = None
    hard_limit: PositiveInt

    @model_validator(mode="after")
    def soft_limit_below_hard_limit(self) -> "QuotaUpdate":
        if self.soft_limit is None:
            self.soft_limit = self.hard_limit
        if self.hard_limit < self.soft_limit:
            raise ValueError("'soft_limit' cannot be larger than 'hard_limit'")
        return self


class CatalogQuery(BaseModel):
    """
    body for POST bigcatalogconfig/query_data. filters maps a catalog column to conditions,
    each a single {operator: value} pair:
    {'parent_path': [{'eq': '/smb/gisusers/'}], 'element_type': [{'eq': 'DIR'}]}
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    path: str = "/"
    filters: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    fields: list[str] = Field(default_factory=lambda: ["name"], min_length=1)

    # validate_path rejects "/" and trailing slashes, both of which catalog queries use
    @field_validator("path")
    @classmethod
    def is_valid_catalog_path(cls, path: str) -> str:
        if not path.startswith("/"):
            raise ValueError("path must be absolute")
        if ".." in path.split("/"):
            raise ValueError("path must not contain '..'")
        if any(char.isspace() or not char.isprintable() for char in path):
            raise ValueError("path must not contain whitespace or control characters")
        return path

    @field_validator("filters")
    @classmethod
    def is_valid_filters(cls, filters: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
        for column, conditions in filters.items():
            if not column:
                raise ValueError("filter column names must not be empty")
            if not conditions:
                raise ValueError(f"filter on '{column}' has no conditions")
            for condition in conditions:
                if len(condition) != 1:
                    raise ValueError(f"each condition on '{column}' must be a single {{operator: value}} pair")
                operator = next(iter(condition))
                if operator not in CATALOG_FILTER_OPERATORS:
                    allowed = ", ".join(sorted(CATALOG_FILTER_OPERATORS))
                    raise ValueError(f"unknown operator '{operator}' on '{column}'; expected one of {allowed}")
        return filters

    @field_validator("fields")
    @classmethod
    def is_valid_fields(cls, fields: list[str]) -> list[str]:
        if not all(fields):
            raise ValueError("field names must not be empty")
        return fields
