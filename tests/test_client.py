"""What each VASTClient method sends: method, endpoint, query and body.

These go through the _call seam, so they hold whichever transport sits beneath it.
"""

from pathlib import Path

import pytest

from vast_api_client import VASTClient
from vast_api_client.models import ProtocolEnum
from vast_api_client.utils import ResourceExistsError

ACL_BODY = {
    "perm": "FULL",
    "grantee": "groups",
    "sid_str": "S-1-5-21-1",
    "uid_or_gid": 5000,
    "group_type": "SMB",
}


def test_get_quotas(vast, api_calls):
    api_calls.responses.append([{"id": 1}])

    assert vast.get_quotas() == [{"id": 1}]
    assert api_calls.requests == [("GET", "quotas", None, None)]


def test_get_quotas_for_a_path(vast, api_calls):
    vast.get_quotas(path=Path("/rstore/lab"))

    assert api_calls.requests == [("GET", "quotas", {"path": "/rstore/lab"}, None)]


def test_get_quotas_rejects_a_bad_path(vast, api_calls):
    with pytest.raises(ValueError):
        vast.get_quotas(path=Path("/rstore/lab.old"))
    assert api_calls.requests == []


def test_get_views(vast, api_calls):
    vast.get_views()
    vast.get_views(path=Path("/rstore/lab"))

    assert api_calls.requests == [
        ("GET", "views", None, None),
        ("GET", "views", {"path": "/rstore/lab"}, None),
    ]


def test_add_view_without_a_share(vast, api_calls):
    vast.add_view(path=Path("/cluster/lab"), protocols={ProtocolEnum.NFS}, policy_id=3)

    assert api_calls.requests == [
        (
            "POST",
            "views",
            None,
            {"path": "/cluster/lab", "policy_id": 3, "protocols": ["NFS"], "create_dir": True},
        )
    ]


def test_add_view_with_a_share_and_acls(vast, api_calls):
    acl = VASTClient.create_acl_from_str("FULL", "groups", "S-1-5-21-1", 5000)

    vast.add_view(
        path=Path("/smb/lab"),
        protocols=[ProtocolEnum.SMB],
        share_name="lab$",
        policy_id=5,
        acls=[acl],
    )

    assert api_calls.requests == [
        (
            "POST",
            "views",
            None,
            {
                "share": "lab$",
                "path": "/smb/lab",
                "policy_id": 5,
                "protocols": ["SMB"],
                "create_dir": True,
                "inherit_acl": True,
                "create_dir_acls": [ACL_BODY],
            },
        )
    ]


def test_add_view_with_an_smb_only_group(vast, api_calls):
    # SMB-only groups have no POSIX gid; uid_or_gid is left out of the ACL
    acl = VASTClient.create_acl_from_str("CHANGE", "groups", "S-1-5-21-2", None)

    vast.add_view(path=Path("/smb/lab"), protocols=[ProtocolEnum.SMB], share_name="lab$", policy_id=5, acls=[acl])

    assert api_calls.requests[0][3]["create_dir_acls"] == [
        {"perm": "CHANGE", "grantee": "groups", "sid_str": "S-1-5-21-2", "group_type": "SMB"}
    ]


def test_add_view_with_a_share_and_no_acls(vast, api_calls):
    vast.add_view(path=Path("/smb/lab"), protocols={ProtocolEnum.SMB, ProtocolEnum.NFS}, share_name="lab$", policy_id=5)

    body = api_calls.requests[0][3]
    assert body["protocols"] == ["NFS", "SMB"]
    assert body["create_dir_acls"] == []


@pytest.mark.parametrize("protocols", [None, set(), []])
def test_add_view_needs_protocols(vast, api_calls, protocols):
    with pytest.raises(ValueError):
        vast.add_view(path=Path("/cluster/lab"), protocols=protocols, policy_id=3)
    assert api_calls.requests == []


def test_add_view_needs_a_policy_id(vast, api_calls):
    with pytest.raises(ValueError):
        vast.add_view(path=Path("/cluster/lab"), protocols={ProtocolEnum.NFS})
    with pytest.raises(ValueError):
        vast.add_view(path=Path("/smb/lab"), protocols={ProtocolEnum.SMB}, share_name="lab$")
    assert api_calls.requests == []


def test_add_view_dry_run_sends_nothing(vast, api_calls):
    assert vast.add_view(path=Path("/cluster/lab"), protocols={ProtocolEnum.NFS}, policy_id=3, dry_run=True) is None
    assert api_calls.requests == []


def test_add_view_needs_a_share_name_ending_in_dollar(vast, api_calls):
    with pytest.raises(ValueError):
        vast.add_view(path=Path("/smb/lab"), protocols={ProtocolEnum.SMB}, share_name="lab", policy_id=5, acls=[])
    assert api_calls.requests == []


def test_add_quota(vast, api_calls):
    vast.add_quota(name="lab$", path=Path("/rstore/lab"), hard_limit=1000)

    assert api_calls.requests == [
        (
            "POST",
            "quotas",
            None,
            {"name": "lab", "path": "/rstore/lab", "soft_limit": 1000, "hard_limit": 1000, "create_dir": False},
        )
    ]


def test_add_quota_soft_limit_above_hard_limit(vast, api_calls):
    with pytest.raises(ValueError):
        vast.add_quota(name="lab", path=Path("/rstore/lab"), hard_limit=1000, soft_limit=2000)
    assert api_calls.requests == []


def test_add_quota_dry_run_sends_nothing(vast, api_calls):
    assert vast.add_quota(name="lab", path=Path("/rstore/lab"), hard_limit=1000, dry_run=True) is None
    assert api_calls.requests == []


def test_add_folder(vast, api_calls):
    vast.add_folder(Path("/homedir/jdoe01"), group="jdoe01", user="jdoe01")
    vast.add_folder(Path("/rstore/lab"), group="lab_group", user="jdoe01")

    assert api_calls.requests == [
        (
            "POST",
            "folders/create_folder",
            None,
            {"path": "/homedir/jdoe01", "owner_is_group": True, "user": "jdoe01", "group": "jdoe01"},
        ),
        (
            "POST",
            "folders/create_folder",
            None,
            {"path": "/rstore/lab", "owner_is_group": False, "user": "jdoe01", "group": "lab_group"},
        ),
    ]


def test_add_folder_without_a_user(vast, api_calls):
    vast.add_folder(Path("/rstore/lab"), group="lab_group")

    assert api_calls.requests == [
        (
            "POST",
            "folders/create_folder",
            None,
            {"path": "/rstore/lab", "owner_is_group": False, "user": None, "group": "lab_group"},
        )
    ]


def test_modify_folder_without_a_user(vast, api_calls):
    vast.modify_folder(Path("/rstore/lab"), group="lab_group")

    assert api_calls.requests == [
        ("POST", "folders/create_folder", None, {"path": "/rstore/lab", "group": "lab_group"})
    ]


def test_modify_folder(vast, api_calls):
    vast.modify_folder(Path("/rstore/lab"), group="lab_group", user="jdoe01")

    assert api_calls.requests == [
        ("POST", "folders/create_folder", None, {"path": "/rstore/lab", "user": "jdoe01", "group": "lab_group"})
    ]


def test_get_folder(vast, api_calls):
    api_calls.responses.append({"is_directory": True})

    assert vast.get_folder(Path("/homedir/jdoe01")) == {"is_directory": True}
    vast.get_folder("/homedir/jdoe01", tenant_id=1)

    assert api_calls.requests == [
        ("POST", "folders/stat_path", None, {"path": "/homedir/jdoe01"}),
        ("POST", "folders/stat_path", None, {"path": "/homedir/jdoe01", "tenant_id": 1}),
    ]


def test_delete_folder(vast, api_calls):
    vast.delete_folder(Path("/rstore/lab"), tenant_id=1)

    assert api_calls.requests == [("DELETE", "folders/delete_folder", None, {"path": "/rstore/lab", "tenant_id": 1})]


def test_update_quota_size(vast, api_calls):
    vast.update_quota_size(5, 2000)

    assert api_calls.requests == [("PATCH", "quotas/5", None, {"soft_limit": 2000, "hard_limit": 2000})]


@pytest.mark.parametrize("quota_id, new_size", [("5", 2000), (5, 2000.0)])
def test_update_quota_size_needs_ints(vast, api_calls, quota_id, new_size):
    with pytest.raises(TypeError):
        vast.update_quota_size(quota_id, new_size)
    assert api_calls.requests == []


def test_delete_quota(vast, api_calls):
    vast.delete_quota(5)

    assert api_calls.requests == [("DELETE", "quotas/5", None, None)]


def test_delete_quota_needs_an_int(vast, api_calls):
    with pytest.raises(TypeError):
        vast.delete_quota("5")
    assert api_calls.requests == []


def test_get_total_capacity(vast, api_calls):
    vast.get_total_capacity()

    assert api_calls.requests == [("GET", "capacity", None, None)]


def test_get_protected_paths(vast, api_calls):
    vast.get_protected_paths()
    vast.get_protected_paths(source_dir=Path("/rstore/lab"))

    assert api_calls.requests == [
        ("GET", "protectedpaths", None, None),
        ("GET", "protectedpaths", {"source_dir": "/rstore/lab"}, None),
    ]


def test_add_protected_path(vast, api_calls):
    api_calls.responses.append([])

    vast.add_protected_path(name="lab", source_dir=Path("/rstore/lab"), protection_policy_id=6, tenant_id=1)

    assert api_calls.requests == [
        ("GET", "protectedpaths", {"source_dir": "/rstore/lab"}, None),
        (
            "POST",
            "protectedpaths",
            None,
            {
                "name": "lab",
                "source_dir": "/rstore/lab",
                "enabled": True,
                "protection_policy_id": 6,
                "tenant_id": 1,
            },
        ),
    ]


def test_add_protected_path_that_exists(vast, api_calls):
    api_calls.responses.append([{"source_dir": "/rstore/lab"}])

    with pytest.raises(ResourceExistsError):
        vast.add_protected_path(name="lab", source_dir=Path("/rstore/lab"), protection_policy_id=6, tenant_id=1)
    assert [request[0] for request in api_calls.requests] == ["GET"]


def test_get_protection_policies(vast, api_calls):
    vast.get_protection_policies(policy_id=14)
    vast.get_protection_policies(name="daily")
    vast.get_protection_policies(source_dir=Path("/rstore/lab"))
    vast.get_protection_policies()

    assert api_calls.requests == [
        ("GET", "protectionpolicies/14", None, None),
        ("GET", "protectionpolicies", {"name": "daily"}, None),
        ("GET", "protectionpolicies", {"source_dir": "/rstore/lab"}, None),
        ("GET", "protectionpolicies", None, None),
    ]


def test_query_catalog(vast, api_calls):
    vast.query_catalog({"parent_path": [{"eq": "/smb/gisusers/"}], "element_type": [{"eq": "DIR"}]})

    assert api_calls.requests == [
        (
            "POST",
            "bigcatalogconfig/query_data",
            None,
            {
                "path": "/",
                "filters": {"parent_path": [{"eq": "/smb/gisusers/"}], "element_type": [{"eq": "DIR"}]},
                "fields": ["name"],
            },
        )
    ]


def test_query_catalog_with_fields_and_path(vast, api_calls):
    vast.query_catalog({"size": [{"ge": 1048576}]}, fields=["name", "size"], path="/rstore/")

    assert api_calls.requests == [
        (
            "POST",
            "bigcatalogconfig/query_data",
            None,
            {"path": "/rstore/", "filters": {"size": [{"ge": 1048576}]}, "fields": ["name", "size"]},
        )
    ]


@pytest.mark.parametrize("filters", [{}, {"name": [{"contains": "report"}]}])
def test_query_catalog_rejects_bad_filters(vast, api_calls, filters):
    with pytest.raises(ValueError):
        vast.query_catalog(filters)
    assert api_calls.requests == []


def test_query_catalog_refuses_fields_as_a_string(vast, api_calls):
    with pytest.raises(TypeError):
        vast.query_catalog({"name": [{"eq": "report"}]}, fields="size")
    assert api_calls.requests == []
