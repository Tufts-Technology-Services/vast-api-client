import json
import re
from enum import UNIQUE, StrEnum, verify
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    PlainSerializer,
    PositiveInt,
    field_serializer,
    field_validator,
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


# a path checked by validate_path and sent as a POSIX string
VastPath = Annotated[Path, AfterValidator(validate_path), PlainSerializer(Path.as_posix)]


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
# against VAST 5.4.4 and 5.5 in the VAST KB, and eq, ge and le against the HPC VMS (2026-10-20); gt
# and lt appear in the KB's catalog examples. contains and gte are documented as rejected (400), so
# they are left out.
CatalogOperator = Literal["eq", "startswith", "gt", "ge", "lt", "le"]
# {operator: value, ...}: the operators in one condition are AND'ed, as in a range {"ge": 1, "lt": 10}
CatalogCondition = Annotated[dict[CatalogOperator, Any], Field(min_length=1)]
NonEmptyStr = Annotated[str, Field(min_length=1)]


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


class QuotaCreate(QuotaUpdate):
    """a new quota: QuotaUpdate's limits, plus where it goes"""

    name: str
    path: VastPath
    create_dir: bool = False


class ACL(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    perm: ACLPerm
    grantee: ACLGrantee
    sid_str: NonEmptyStr
    # None for SMB-only groups, which have no POSIX gid; left out of the body rather than sent as null
    uid_or_gid: NonNegativeInt | None = Field(default=None, exclude_if=lambda uid_or_gid: uid_or_gid is None)
    group_type: ProtocolEnum = ProtocolEnum.SMB

    def sort_key(self) -> tuple:
        return (
            self.grantee,
            self.sid_str,
            self.perm,
            self.group_type,
            -1 if self.uid_or_gid is None else self.uid_or_gid,
        )


class ViewCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    path: VastPath
    policy_id: int
    protocols: frozenset[ProtocolEnum] = Field(min_length=1)
    create_dir: bool = True

    # sorted, so the same view always produces the same body
    @field_serializer("protocols")
    def serialize_protocols(self, protocols: frozenset[ProtocolEnum], _info):
        return sorted(protocol.value for protocol in protocols)


class ShareCreate(ViewCreate):
    """a view with an SMB share: ViewCreate's fields, plus the share name and the ACLs for the new directory"""

    share: str | None = None  # share name must end with '$'
    inherit_acl: bool = True
    create_dir_acls: frozenset[ACL] = frozenset()

    @field_serializer("create_dir_acls")
    def serialize_create_dir_acls(self, create_dir_acls: frozenset[ACL], _info):
        return [acl.model_dump(mode="json") for acl in sorted(create_dir_acls, key=ACL.sort_key)]

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

    @model_validator(mode="after")
    def share_if_smb(self) -> "ShareCreate":
        if ProtocolEnum.SMB in self.protocols and self.share is None:
            raise ValueError("SMB views require a share name")
        return self


class ProtectionPolicyFrame(BaseModel):
    """
    VAST names the fields with hyphens, and so does the dump; build it with underscores or hyphens:
    {'every': '1D', 'start-at': '2024-01-22 17:00:00', 'keep-local': '2M', 'keep-remote': '0s'}
    """

    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, frozen=True, populate_by_name=True, serialize_by_alias=True
    )
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


class ProtectedPathCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    name: str
    source_dir: VastPath
    enabled: bool = True
    protection_policy_id: int
    tenant_id: int


class PathBody(BaseModel):
    """a path, as a query or a body; dump with exclude_none=True, so a tenant_id of None is left out"""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    path: VastPath
    tenant_id: int | None = None


class SourceDirQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_dir: VastPath


class FolderCreateOrUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    path: VastPath
    owner_is_group: bool = False
    user: str | None = None
    group: str | None = None


class CatalogQuery(BaseModel):
    """
    query for GET bigcatalogconfig/query_data. filters maps a catalog column to a list of conditions,
    which are OR'ed; the operators within one condition are AND'ed:
    {'parent_path': [{'eq': '/smb/gisusers/'}], 'size': [{'ge': 1024, 'lt': 1048576}]}
    There's no page: VAST 5.3 refuses it (400), and returns every matching row in one response anyway.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)
    path: str = "/"
    # VAST rejects a query without a filter
    filters: dict[NonEmptyStr, Annotated[list[CatalogCondition], Field(min_length=1)]] = Field(min_length=1)
    fields: list[NonEmptyStr] = Field(default_factory=lambda: ["name"], min_length=1)
    # left out unless given: VAST 5.3 has no default, and a limit below the number of rows that match
    # makes the query hang rather than cut the result short
    limit: PositiveInt | None = None
    ordering: list[NonEmptyStr] | None = None
    snapshot: NonEmptyStr | None = None
    tenant_id: PositiveInt | None = None

    def query_params(self) -> dict[str, Any]:
        """as GET query parameters: filters as the JSON string VAST expects, and None left out"""
        params = self.model_dump(mode="json", exclude_none=True)
        params["filters"] = json.dumps(params["filters"])
        return params

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
