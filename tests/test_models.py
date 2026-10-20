"""What the request models accept, reject and serialize."""

from pathlib import Path

import pytest

from vast_api_client.models import (
    ACL,
    ACLGrantee,
    ACLPerm,
    CatalogQuery,
    CloneTypeEnum,
    FolderCreateOrUpdate,
    ProtectionPolicyCreate,
    ProtectionPolicyFrame,
    ProtocolEnum,
    QuotaCreate,
    QuotaUpdate,
    ShareCreate,
    ViewCreate,
    validate_path,
)

ACL_BODY = {"perm": "FULL", "grantee": "groups", "sid_str": "S-1-5-21-1", "uid_or_gid": 5000, "group_type": "SMB"}


def make_acl(**overrides):
    fields = {"perm": ACLPerm.FULL, "grantee": ACLGrantee.GROUPS, "sid_str": "S-1-5-21-1", "uid_or_gid": 5000}
    return ACL(**(fields | overrides))


VIEW_FIELDS = {"path": Path("/cluster/lab"), "policy_id": 3, "protocols": [ProtocolEnum.NFS]}
SHARE_FIELDS = {"share": "lab$", "path": Path("/smb/lab"), "policy_id": 5, "protocols": [ProtocolEnum.SMB]}


def make_view(**overrides):
    return ViewCreate(**(VIEW_FIELDS | overrides))


def make_share(**overrides):
    return ShareCreate(**(SHARE_FIELDS | overrides))


def make_frame(**overrides):
    fields = {"every": "1D", "start_at": "2024-01-22 17:00:00", "keep_local": "2M"}
    return ProtectionPolicyFrame(**(fields | overrides))


# validate_path


@pytest.mark.parametrize("path", ["/rstore/lab", "/a", "/cluster/lab_01/sub-dir"])
def test_validate_path_accepts(path):
    assert validate_path(Path(path)) == Path(path)


@pytest.mark.parametrize("path", ["/", "rstore/lab", "/rstore/lab.old", "/rstore/my lab", "//rstore/lab"])
def test_validate_path_rejects(path):
    with pytest.raises(ValueError):
        validate_path(Path(path))


# ViewCreate and ShareCreate protocols


@pytest.mark.parametrize(
    "protocols", [[ProtocolEnum.NFS], {ProtocolEnum.NFS}, frozenset({ProtocolEnum.NFS}), (ProtocolEnum.NFS,)]
)
def test_view_accepts_protocols_as_any_collection(protocols):
    view = make_view(protocols=protocols)

    assert view.protocols == frozenset({ProtocolEnum.NFS})
    assert view.model_dump(mode="json")["protocols"] == ["NFS"]


@pytest.mark.parametrize("model", [ViewCreate, make_share])
def test_protocols_serialize_sorted(model):
    kwargs = {"path": Path("/smb/lab"), "policy_id": 5}
    first = model(**kwargs, protocols=[ProtocolEnum.SMB, ProtocolEnum.NFS])
    second = model(**kwargs, protocols=[ProtocolEnum.NFS, ProtocolEnum.SMB])

    assert first.model_dump(mode="json")["protocols"] == ["NFS", "SMB"]
    assert second.model_dump(mode="json") == first.model_dump(mode="json")


@pytest.mark.parametrize("make_model", [make_view, make_share])
@pytest.mark.parametrize("protocols", [[], set(), None])
def test_protocols_are_required(make_model, protocols):
    with pytest.raises(ValueError):
        make_model(protocols=protocols)


@pytest.mark.parametrize("model, fields", [(ViewCreate, VIEW_FIELDS), (ShareCreate, SHARE_FIELDS)])
@pytest.mark.parametrize("left_out", ["protocols", "policy_id"])
def test_protocols_and_policy_id_cannot_be_left_out(model, fields, left_out):
    with pytest.raises(ValueError):
        model(**{name: value for name, value in fields.items() if name != left_out})


def test_view_body():
    view = make_view()

    assert view.model_dump(mode="json") == {
        "path": "/cluster/lab",
        "policy_id": 3,
        "protocols": ["NFS"],
        "create_dir": True,
    }


# policy_id


@pytest.mark.parametrize("make_model", [make_view, make_share])
def test_policy_id_cannot_be_none(make_model):
    with pytest.raises(ValueError):
        make_model(policy_id=None)


# ShareCreate


def test_share_body():
    share = make_share(create_dir_acls=[make_acl()])

    assert share.model_dump(mode="json") == {
        "share": "lab$",
        "path": "/smb/lab",
        "policy_id": 5,
        "protocols": ["SMB"],
        "create_dir": True,
        "inherit_acl": True,
        "create_dir_acls": [ACL_BODY],
    }


@pytest.mark.parametrize("acls", [None, [], set(), frozenset()])
def test_share_create_dir_acls_can_be_empty(acls):
    share = make_share(create_dir_acls=acls)

    assert share.create_dir_acls == frozenset()
    assert share.model_dump(mode="json")["create_dir_acls"] == []


@pytest.mark.parametrize("container", [list, set, tuple])
def test_share_accepts_acls_as_any_collection(container):
    share = make_share(create_dir_acls=container([make_acl()]))

    assert share.model_dump(mode="json")["create_dir_acls"] == [ACL_BODY]


def test_share_acls_serialize_in_a_stable_order():
    acls = [
        make_acl(grantee=ACLGrantee.USERS, sid_str="S-1-5-21-9", uid_or_gid=1001),
        make_acl(sid_str="S-1-5-21-2", uid_or_gid=None),
        make_acl(sid_str="S-1-5-21-1", perm=ACLPerm.READ),
    ]

    bodies = {str(make_share(create_dir_acls=order).model_dump(mode="json")) for order in (acls, acls[::-1])}
    assert len(bodies) == 1
    assert [acl["sid_str"] for acl in make_share(create_dir_acls=acls).model_dump(mode="json")["create_dir_acls"]] == [
        "S-1-5-21-1",
        "S-1-5-21-2",
        "S-1-5-21-9",
    ]


def test_share_drops_duplicate_acls():
    share = make_share(create_dir_acls=[make_acl(), make_acl()])

    assert share.model_dump(mode="json")["create_dir_acls"] == [ACL_BODY]


def test_share_name_must_end_in_dollar():
    with pytest.raises(ValueError):
        make_share(share="lab")


def test_smb_share_needs_a_share_name():
    with pytest.raises(ValueError):
        make_share(share=None)


def test_nfs_only_share_needs_no_share_name():
    share = make_share(share=None, protocols=[ProtocolEnum.NFS])

    assert share.model_dump(mode="json")["share"] is None


# ACL


def test_acl_body():
    assert make_acl().model_dump(mode="json") == ACL_BODY


def test_acl_without_a_gid_leaves_it_out():
    acl = ACL(perm=ACLPerm.FULL, grantee=ACLGrantee.GROUPS, sid_str="S-1-5-21-1")

    assert acl.uid_or_gid is None
    assert acl.model_dump(mode="json") == {
        "perm": "FULL",
        "grantee": "groups",
        "sid_str": "S-1-5-21-1",
        "group_type": "SMB",
    }
    assert "uid_or_gid" not in acl.model_dump()


def test_acl_keeps_gid_zero():
    acl = make_acl(uid_or_gid=0)

    assert acl.model_dump(mode="json")["uid_or_gid"] == 0


def test_acl_rejects_a_negative_gid():
    with pytest.raises(ValueError):
        make_acl(uid_or_gid=-1)


def test_acl_needs_a_sid():
    with pytest.raises(ValueError):
        make_acl(sid_str="")
    with pytest.raises(ValueError):
        make_acl(sid_str="   ")


def test_acl_accepts_enum_values_as_strings():
    assert ACL(perm="FULL", grantee="groups", sid_str="S-1-5-21-1", uid_or_gid=5000).model_dump(mode="json") == ACL_BODY


@pytest.mark.parametrize("field, value", [("perm", "WRITE"), ("grantee", "others"), ("group_type", "S3")])
def test_acl_rejects_unknown_enum_values(field, value):
    with pytest.raises(ValueError):
        make_acl(**{field: value})


def test_acls_are_hashable_and_compare_by_value():
    assert {make_acl(), make_acl()} == {make_acl()}
    assert make_acl(uid_or_gid=None) != make_acl()


# FolderCreateOrUpdate


def test_folder_with_user_and_group():
    folder = FolderCreateOrUpdate(path=Path("/rstore/lab"), user="jdoe01", group="lab_group")

    assert folder.model_dump(mode="json") == {
        "path": "/rstore/lab",
        "owner_is_group": False,
        "user": "jdoe01",
        "group": "lab_group",
    }


def test_folder_without_user():
    folder = FolderCreateOrUpdate(path=Path("/rstore/lab"), group="lab_group")

    assert folder.model_dump(mode="json") == {
        "path": "/rstore/lab",
        "owner_is_group": False,
        "user": None,
        "group": "lab_group",
    }
    assert folder.model_dump(mode="json", exclude_unset=True, exclude_defaults=True) == {
        "path": "/rstore/lab",
        "group": "lab_group",
    }


def test_folder_accepts_explicit_none_owners():
    folder = FolderCreateOrUpdate(path=Path("/rstore/lab"), user=None, group=None)

    assert folder.user is None
    assert folder.group is None


def test_folder_rejects_a_bad_path():
    with pytest.raises(ValueError):
        FolderCreateOrUpdate(path=Path("/rstore/lab.old"), group="lab_group")


# ProtectionPolicyFrame and ProtectionPolicyCreate

FRAME_BODY = {"every": "1D", "start-at": "2024-01-22 17:00:00", "keep-local": "2M", "keep-remote": "0s"}


def test_frame_built_with_underscores_dumps_with_hyphens():
    assert make_frame().model_dump(mode="json") == FRAME_BODY
    assert make_frame().model_dump() == FRAME_BODY


def test_frame_built_from_vast_keys():
    assert ProtectionPolicyFrame(**FRAME_BODY) == make_frame()


def test_frame_rejects_an_unknown_every():
    with pytest.raises(ValueError):
        make_frame(every="2D")


def test_protection_policy_body_uses_hyphenated_frame_keys():
    policy = ProtectionPolicyCreate(name="daily", frames=[make_frame()], prefix="daily")

    assert policy.model_dump(mode="json") == {
        "name": "daily",
        "frames": [FRAME_BODY],
        "prefix": "daily",
        "clone_type": "LOCAL",
        "indestructible": False,
    }


def test_protection_policy_clone_type():
    frame = make_frame()

    remote = ProtectionPolicyCreate(name="daily", frames=[frame], prefix="daily", clone_type="REMOTE")
    assert remote.clone_type is CloneTypeEnum.REMOTE
    assert remote.model_dump(mode="json")["clone_type"] == "REMOTE"
    with pytest.raises(ValueError):
        ProtectionPolicyCreate(name="daily", frames=[frame], prefix="daily", clone_type="ELSEWHERE")


# QuotaCreate and QuotaUpdate


# each quota model, with the fields it needs besides its limits
QUOTA_MODELS = [(QuotaCreate, {"name": "lab", "path": Path("/rstore/lab")}), (QuotaUpdate, {})]


@pytest.mark.parametrize("model, base", QUOTA_MODELS)
def test_quota_soft_limit_defaults_to_hard_limit(model, base):
    assert model(**base, hard_limit=1000).soft_limit == 1000


@pytest.mark.parametrize("model, base", QUOTA_MODELS)
def test_quota_soft_limit_at_or_below_hard_limit(model, base):
    assert model(**base, hard_limit=1000, soft_limit=1000).soft_limit == 1000
    assert model(**base, hard_limit=1000, soft_limit=500).soft_limit == 500


@pytest.mark.parametrize("model, base", QUOTA_MODELS)
def test_quota_soft_limit_above_hard_limit(model, base):
    with pytest.raises(ValueError):
        model(**base, hard_limit=1000, soft_limit=2000)


@pytest.mark.parametrize("model, base", QUOTA_MODELS)
@pytest.mark.parametrize("limits", [{"hard_limit": 0}, {"hard_limit": -1}, {"hard_limit": 1000, "soft_limit": 0}])
def test_quota_limits_must_be_positive(model, base, limits):
    with pytest.raises(ValueError):
        model(**base, **limits)


def test_quota_needs_a_hard_limit():
    with pytest.raises(ValueError):
        QuotaUpdate(soft_limit=1000)


def test_quota_create_body():
    quota = QuotaCreate(name="lab", path=Path("/rstore/lab"), hard_limit=1000, soft_limit=800)

    assert quota.model_dump(mode="json") == {
        "name": "lab",
        "path": "/rstore/lab",
        "soft_limit": 800,
        "hard_limit": 1000,
        "create_dir": False,
    }


# CatalogQuery

GISUSERS_FILTERS = {"parent_path": [{"eq": "/smb/gisusers/"}], "element_type": [{"eq": "DIR"}]}


def test_catalog_query_defaults():
    query = CatalogQuery(filters=GISUSERS_FILTERS)

    assert query.model_dump(mode="json") == {"path": "/", "filters": GISUSERS_FILTERS, "fields": ["name"]}


def test_catalog_query_defaults_are_not_shared():
    first = CatalogQuery(filters=GISUSERS_FILTERS)
    first.fields.append("size")

    assert CatalogQuery(filters=GISUSERS_FILTERS).fields == ["name"]


@pytest.mark.parametrize("filters", [None, {}])
def test_catalog_query_needs_a_filter(filters):
    with pytest.raises(ValueError):
        CatalogQuery(filters=filters)


def test_catalog_query_filters_cannot_be_left_out():
    with pytest.raises(ValueError):
        CatalogQuery()


@pytest.mark.parametrize("path", ["/", "/smb/gisusers/", "/smb/gisusers", "/rstore/lab.old/my_dir-2"])
def test_catalog_query_accepts_path(path):
    assert CatalogQuery(path=path, filters=GISUSERS_FILTERS).path == path


@pytest.mark.parametrize(
    "path", ["", "smb/gisusers", "/smb/../etc", "/..", "/smb/my lab", "/smb/\tlab", "/smb/\x00lab"]
)
def test_catalog_query_rejects_path(path):
    with pytest.raises(ValueError):
        CatalogQuery(path=path, filters=GISUSERS_FILTERS)


@pytest.mark.parametrize("operator", ["eq", "startswith", "gt", "ge", "lt", "le"])
def test_catalog_query_accepts_operator(operator):
    filters = {"size": [{operator: 1048576}]}

    assert CatalogQuery(filters=filters).model_dump(mode="json")["filters"] == filters


def test_catalog_query_accepts_several_conditions_on_a_column():
    filters = {"size": [{"ge": 1048576}, {"lt": 1073741824}]}

    assert CatalogQuery(filters=filters).filters == filters


@pytest.mark.parametrize("operator", ["contains", "gte", "EQ", "like", ""])
def test_catalog_query_rejects_operator(operator):
    with pytest.raises(ValueError):
        CatalogQuery(filters={"name": [{operator: "report"}]})


@pytest.mark.parametrize(
    "filters",
    [
        {"size": [{"gt": 1, "lt": 10}]},
        {"size": [{}]},
        {"size": []},
        {"": [{"eq": "x"}]},
        {"name": "report"},
        {"name": {"eq": "report"}},
    ],
)
def test_catalog_query_rejects_malformed_filters(filters):
    with pytest.raises(ValueError):
        CatalogQuery(filters=filters)


@pytest.mark.parametrize("fields", [[], [""], None, "name"])
def test_catalog_query_rejects_fields(fields):
    with pytest.raises(ValueError):
        CatalogQuery(filters=GISUSERS_FILTERS, fields=fields)


def test_catalog_query_rejects_unknown_keys():
    with pytest.raises(ValueError):
        CatalogQuery(filters=GISUSERS_FILTERS, page_size=100)
