"""What each VASTClient method sends: method, endpoint, query and body.

These go through the _call seam, so they hold whichever transport sits beneath it.
"""

from pathlib import Path

import pytest

from vast_api_client import VASTClient
from vast_api_client.exceptions import VastError, VastNotFoundError
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
            "GET",
            "bigcatalogconfig/query_data",
            {
                "path": "/",
                "filters": '{"parent_path": [{"eq": "/smb/gisusers/"}], "element_type": [{"eq": "DIR"}]}',
                "fields": ["name"],
            },
            None,
        )
    ]


def test_query_catalog_with_fields_and_path(vast, api_calls):
    vast.query_catalog(
        {"size": [{"ge": 1048576, "lt": 1073741824}]},
        fields=["name", "size"],
        path="/rstore/",
        limit=50,
        ordering=["name"],
        snapshot="daily",
        tenant_id=1,
    )

    assert api_calls.requests == [
        (
            "GET",
            "bigcatalogconfig/query_data",
            {
                "path": "/rstore/",
                "filters": '{"size": [{"ge": 1048576, "lt": 1073741824}]}',
                "fields": ["name", "size"],
                "limit": 50,
                "ordering": ["name"],
                "snapshot": "daily",
                "tenant_id": 1,
            },
            None,
        )
    ]


@pytest.mark.parametrize("filters", [{}, {"name": [{"contains": "report"}]}])
def test_query_catalog_rejects_bad_filters(vast, api_calls, filters):
    with pytest.raises(ValueError):
        vast.query_catalog(filters)
    assert api_calls.requests == []


def test_query_catalog_refuses_fields_as_a_string(vast, api_calls):
    with pytest.raises(ValueError):
        vast.query_catalog({"name": [{"eq": "report"}]}, fields="size")
    assert api_calls.requests == []


@pytest.mark.parametrize(
    "call",
    [
        lambda vast: vast.get_protected_paths(source_dir=Path("/rstore/lab.old")),
        lambda vast: vast.get_protection_policies(source_dir=Path("/rstore/lab.old")),
        lambda vast: vast.add_protected_path("lab", Path("/rstore/lab.old"), protection_policy_id=6, tenant_id=1),
        lambda vast: vast.get_folder(Path("/homedir/jdoe01"), tenant_id="one"),
    ],
)
def test_bad_paths_and_tenants_are_refused_before_sending(vast, api_calls, call):
    with pytest.raises(ValueError):
        call(vast)
    assert api_calls.requests == []


# One quota by id, and user quota entries

PAGE_2 = "https://vast.example.edu/api/v7/userquotas/?page=2"
EMPTY_PAGE = {"count": 0, "next": None, "previous": None, "results": []}


def test_get_quota(vast, api_calls):
    api_calls.responses.append({"id": 484, "path": "/homedir"})

    assert vast.get_quota(484) == {"id": 484, "path": "/homedir"}
    assert api_calls.requests == [("GET", "quotas/484", None, None)]


@pytest.mark.parametrize("quota_id", ["484", True, 4.0])
def test_get_quota_needs_an_int_id(vast, api_calls, quota_id):
    with pytest.raises(TypeError):
        vast.get_quota(quota_id)
    assert api_calls.requests == []


def test_get_quota_rejects_a_negative_id(vast, api_calls):
    with pytest.raises(ValueError):
        vast.get_quota(-1)
    assert api_calls.requests == []


def test_quota_changes_reject_negative_ids_and_sizes(vast, api_calls):
    with pytest.raises(ValueError):
        vast.update_quota_size(-1, 1024)
    with pytest.raises(ValueError):
        vast.update_quota_size(5, -1024)
    with pytest.raises(ValueError):
        vast.delete_quota(-1)
    assert api_calls.requests == []


def test_a_bool_isnt_a_quota_id_for_changes_either(vast, api_calls):
    with pytest.raises(TypeError):
        vast.update_quota_size(True, 1024)
    with pytest.raises(TypeError):
        vast.update_quota_size(5, True)
    with pytest.raises(TypeError):
        vast.delete_quota(True)
    assert api_calls.requests == []


def test_get_user_quotas_for_one_person_in_one_quota(vast, api_calls):
    api_calls.responses.append({"count": 1, "next": None, "previous": None, "results": [{"id": 7}]})

    assert vast.get_user_quotas(path=Path("/homedir"), identifier="tphimm01") == [{"id": 7}]
    assert api_calls.requests == [("GET", "userquotas", {"path": "/homedir", "entity_identifier": "tphimm01"}, None)]


def test_get_user_quotas_follows_next_across_pages(vast, api_calls):
    api_calls.responses += [
        {
            "count": 3,
            "next": "https://vast.example.edu/api/v7/userquotas/?entity_identifier=x&page=2&path=%2Fhomedir",
            "previous": None,
            "results": [{"id": 1}, {"id": 2}],
        },
        {"count": 3, "next": None, "previous": "https://vast.example.edu/api/v7/userquotas/", "results": [{"id": 3}]},
    ]

    assert vast.get_user_quotas(path=Path("/homedir"), identifier="x") == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert api_calls.requests == [
        ("GET", "userquotas", {"path": "/homedir", "entity_identifier": "x"}, None),
        ("GET", "userquotas", {"entity_identifier": "x", "page": "2", "path": "/homedir"}, None),
    ]


@pytest.mark.parametrize(
    "next_link, next_query",
    [
        # what VAST puts in the link is sent as it is: page_size here, or offset paging, or a cursor
        ("https://vast.example.edu/api/v7/userquotas/?page=2&page_size=50", {"page": "2", "page_size": "50"}),
        ("https://vast.example.edu/api/v7/userquotas/?limit=2&offset=2", {"limit": "2", "offset": "2"}),
        ("https://vast.example.edu/api/userquotas/?cursor=abc", {"cursor": "abc"}),
    ],
)
def test_get_user_quotas_follows_the_next_link_as_given(vast, api_calls, next_link, next_query):
    api_calls.responses += [
        {"next": next_link, "results": [{"id": 1}]},
        {"next": None, "results": [{"id": 2}]},
    ]

    assert vast.get_user_quotas() == [{"id": 1}, {"id": 2}]
    assert api_calls.requests[1] == ("GET", "userquotas", next_query, None)


@pytest.mark.parametrize(
    "next_link",
    [
        "https://vast.example.edu/api/v7/quotas/?page=2",
        # a segment before the endpoint that isn't a version makes it a different resource
        "https://vast.example.edu/api/quotas/userquotas/?page=2",
        "https://vast.example.edu/api/v7/extra/userquotas/?page=2",
        "https://vast.example.edu/other/v7/userquotas/?page=2",
    ],
)
def test_get_user_quotas_refuses_a_next_link_to_another_endpoint(vast, api_calls, next_link):
    api_calls.responses.append({"next": next_link, "results": [{"id": 1}]})

    with pytest.raises(VastError, match="next"):
        vast.get_user_quotas()
    assert len(api_calls.requests) == 1


def test_get_user_quotas_drops_an_entry_repeated_across_pages(vast, api_calls):
    # an entry added on VAST between two pages shifts the next page by one, repeating its last entry
    api_calls.responses += [
        {"next": PAGE_2, "results": [{"id": 1}, {"id": 2}]},
        {"next": None, "results": [{"id": 2}, {"id": 3}]},
    ]

    assert vast.get_user_quotas() == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_a_repeated_entry_keeps_its_newer_copy_in_its_first_place(vast, api_calls):
    api_calls.responses += [
        {"next": PAGE_2, "results": [{"id": 1, "used": 10}, {"id": 2}]},
        {"next": None, "results": [{"id": 1, "used": 15}, {"id": 3}]},
    ]

    assert vast.get_user_quotas() == [{"id": 1, "used": 15}, {"id": 2}, {"id": 3}]


@pytest.mark.parametrize(
    "page",
    [
        None,
        "unexpected",
        {"detail": "something went wrong"},
        {"next": None},
        {"next": None, "results": {"id": 1}},
        {"next": None, "results": "abc"},
        {"next": None, "results": None},
    ],
)
def test_a_page_without_a_list_of_results_is_an_error(vast, api_calls, page):
    api_calls.responses.append(page)

    with pytest.raises(VastError, match="results"):
        vast.get_user_quotas()


@pytest.mark.parametrize("later_page", [None, [{"id": 2}], "unexpected", {"next": None, "detail": "went wrong"}])
def test_a_later_page_without_a_list_of_results_is_an_error(vast, api_calls, later_page):
    api_calls.responses += [{"next": PAGE_2, "results": [{"id": 1}]}, later_page]

    with pytest.raises(VastError, match="results"):
        vast.get_user_quotas()


def test_an_empty_page_that_still_has_a_next_link_stops_the_walk(vast, api_calls):
    api_calls.responses += [
        {"next": PAGE_2, "results": [{"id": 1}]},
        {"next": "https://vast.example.edu/api/v7/userquotas/?page=3", "results": []},
        {"next": "https://vast.example.edu/api/v7/userquotas/?page=4", "results": []},
    ]

    with pytest.raises(VastError, match="empty"):
        vast.get_user_quotas()
    assert len(api_calls.requests) == 2


def test_an_empty_first_page_that_has_a_next_link_stops_the_walk(vast, api_calls):
    api_calls.responses.append({"next": PAGE_2, "results": []})

    with pytest.raises(VastError, match="empty"):
        vast.get_user_quotas()
    assert len(api_calls.requests) == 1


@pytest.mark.parametrize("max_pages", [0, -1, True, 2.5, "10"])
def test_max_pages_must_be_a_positive_whole_number(vast, api_calls, max_pages):
    with pytest.raises(ValueError):
        vast.get_user_quotas(max_pages=max_pages)
    assert api_calls.requests == []


@pytest.mark.parametrize("version", ["v5", "latest", "v7", "v7.1", "7"])
def test_a_next_link_under_another_api_version_is_still_followed(vast, api_calls, version):
    api_calls.responses += [
        {"next": f"https://vast.example.edu/api/{version}/userquotas/?page=2", "results": [{"id": 1}]},
        {"next": None, "results": [{"id": 2}]},
    ]

    assert vast.get_user_quotas() == [{"id": 1}, {"id": 2}]
    assert api_calls.requests[1] == ("GET", "userquotas", {"page": "2"}, None)


def test_a_page_gone_by_the_time_it_is_asked_for_isnt_a_not_found(vast, api_calls):
    # entries deleted on VAST mid-walk can leave the next link past the end: DRF answers it with a 404
    api_calls.responses += [
        {"count": 3, "next": PAGE_2, "results": [{"id": 1}]},
        VastNotFoundError(status=404, method="GET", endpoint="userquotas", body='{"detail": "Invalid page."}'),
    ]

    with pytest.raises(VastError, match="changed") as raised:
        vast.get_user_quotas()
    assert not isinstance(raised.value, VastNotFoundError)


def test_a_first_request_not_found_is_still_a_not_found(vast, api_calls):
    api_calls.responses.append(VastNotFoundError(status=404, method="GET", endpoint="userquotas", body="{}"))

    with pytest.raises(VastNotFoundError):
        vast.get_user_quotas()


@pytest.mark.parametrize("count, warned", [(3, True), (2, False)])
def test_a_listing_that_doesnt_match_its_count_is_logged(vast, api_calls, caplog, count, warned):
    api_calls.responses += [
        {"count": count, "next": PAGE_2, "results": [{"id": 1}]},
        {"count": count, "next": None, "results": [{"id": 2}]},
    ]

    with caplog.at_level("WARNING", logger="vast_api_client.vast_api_client"):
        assert vast.get_user_quotas() == [{"id": 1}, {"id": 2}]
    assert ["count" in record.getMessage() for record in caplog.records] == ([True] if warned else [])


def test_entries_that_share_only_an_id_are_both_kept(vast, api_calls):
    user = {"id": 1000, "quota_system_id": 466, "entity": {"identifier": "1000", "is_group": False}}
    group = {"id": 1000, "quota_system_id": 466, "entity": {"identifier": "1000", "is_group": True}}
    elsewhere = {"id": 1000, "quota_system_id": 500, "entity": {"identifier": "1000", "is_group": False}}
    api_calls.responses += [
        {"next": PAGE_2, "results": [user, group]},
        {"next": None, "results": [elsewhere, dict(user, used_capacity=5)]},
    ]

    assert vast.get_user_quotas() == [dict(user, used_capacity=5), group, elsewhere]


def test_a_next_link_back_to_the_first_page_is_caught(vast, api_calls):
    api_calls.responses += [
        {"next": "https://vast.example.edu/api/v7/userquotas/?path=%2Fhomedir", "results": [{"id": 1}]},
        {"next": None, "results": [{"id": 1}]},
    ]

    with pytest.raises(VastError, match="already"):
        vast.get_user_quotas(path=Path("/homedir"))
    assert len(api_calls.requests) == 1


def test_a_next_link_back_to_a_page_in_another_order_is_caught(vast, api_calls):
    api_calls.responses += [
        {"next": "https://vast.example.edu/api/v7/userquotas/?a=1&page=2", "results": [{"id": 1}]},
        {"next": "https://vast.example.edu/api/v7/userquotas/?page=2&a=1", "results": [{"id": 2}]},
    ]

    with pytest.raises(VastError, match="already"):
        vast.get_user_quotas()
    assert len(api_calls.requests) == 2


def test_errors_during_a_walk_dont_name_who_was_looked_up(vast, api_calls):
    api_calls.responses += [
        {
            "next": "https://vast.example.edu/api/v7/userquotas/?entity_identifier=smith01&page=2",
            "results": [{"id": 1}],
        },
        VastNotFoundError(status=404, method="GET", endpoint="userquotas", body="{}"),
    ]

    with pytest.raises(VastError) as raised:
        vast.get_user_quotas(identifier="smith01")
    assert "smith01" not in str(raised.value)


def test_entries_whose_ids_cant_be_compared_are_all_kept(vast, api_calls):
    api_calls.responses += [
        {"next": PAGE_2, "results": [{"id": [1]}]},
        {"next": None, "results": [{"id": [1]}, {"id": {"a": 1}}]},
    ]

    assert vast.get_user_quotas() == [{"id": [1]}, {"id": [1]}, {"id": {"a": 1}}]


def test_get_user_quotas_takes_a_timeout_for_every_page(vast, api_calls):
    api_calls.responses += [
        {"next": PAGE_2, "results": [{"id": 1}]},
        {"next": None, "results": [{"id": 2}]},
    ]

    vast.get_user_quotas(timeout=90)

    assert api_calls.timeouts == [90, 90]


def test_an_error_about_a_page_doesnt_carry_the_whole_page(vast, api_calls):
    api_calls.responses.append({"detail": "x" * 100_000})

    with pytest.raises(VastError) as raised:
        vast.get_user_quotas()
    assert len(str(raised.value)) < 500


def test_the_walk_stops_at_the_page_limit(vast, api_calls):
    api_calls.responses += [
        {"next": f"https://vast.example.edu/api/v7/userquotas/?page={number + 1}", "results": [{"id": number}]}
        for number in range(1, 10)
    ]

    with pytest.raises(VastError, match="pages"):
        vast.get_user_quotas(max_pages=3)
    assert len(api_calls.requests) == 3


@pytest.mark.parametrize(
    "query_string, expected",
    [
        # VAST's links are Django REST framework's, built with urlencode: + is a space, %2B a plus
        ("cursor=ab+cd", {"cursor": "ab cd"}),
        ("cursor=ab%2Bcd%20ef", {"cursor": "ab+cd ef"}),
        ("path=%2Fhomedir&page=2", {"path": "/homedir", "page": "2"}),
        ("flag", {"flag": ""}),
        ("tag=a&tag=b", {"tag": ["a", "b"]}),
    ],
)
def test_a_next_links_query_is_sent_back_as_given(vast, api_calls, query_string, expected):
    api_calls.responses += [
        {"next": f"https://vast.example.edu/api/v7/userquotas/?{query_string}", "results": [{"id": 1}]},
        {"next": None, "results": []},
    ]

    vast.get_user_quotas()

    assert api_calls.requests[1] == ("GET", "userquotas", expected, None)


def test_get_user_quotas_can_ask_for_bigger_pages(vast, api_calls):
    api_calls.responses.append(EMPTY_PAGE)

    vast.get_user_quotas(path=Path("/homedir"), page_size=1000)

    assert api_calls.requests == [("GET", "userquotas", {"path": "/homedir", "page_size": 1000}, None)]


@pytest.mark.parametrize("page_size", [0, -5, True, "100"])
def test_get_user_quotas_rejects_a_bad_page_size(vast, api_calls, page_size):
    with pytest.raises(ValueError):
        vast.get_user_quotas(page_size=page_size)
    assert api_calls.requests == []


def test_get_user_quotas_takes_an_unpaged_list_as_it_is(vast, api_calls):
    api_calls.responses.append([{"id": 1}])

    assert vast.get_user_quotas(path=Path("/homedir")) == [{"id": 1}]
    assert api_calls.requests == [("GET", "userquotas", {"path": "/homedir"}, None)]


def test_get_user_quotas_without_filters_lists_them_all(vast, api_calls):
    api_calls.responses.append(EMPTY_PAGE)

    assert vast.get_user_quotas() == []
    assert api_calls.requests == [("GET", "userquotas", None, None)]


def test_get_user_quotas_takes_a_uid_as_a_number(vast, api_calls):
    api_calls.responses.append(EMPTY_PAGE)

    vast.get_user_quotas(path=Path("/homedir"), identifier=12345)

    assert api_calls.requests == [("GET", "userquotas", {"path": "/homedir", "entity_identifier": "12345"}, None)]


@pytest.mark.parametrize("identifier", ["", "  ", "two words", True, 1.5, -1])
def test_get_user_quotas_rejects_a_bad_identifier(vast, api_calls, identifier):
    with pytest.raises(ValueError):
        vast.get_user_quotas(path=Path("/homedir"), identifier=identifier)
    assert api_calls.requests == []


def test_get_user_quotas_rejects_a_bad_path(vast, api_calls):
    with pytest.raises(ValueError):
        vast.get_user_quotas(path=Path("/homedir/x.old"))
    assert api_calls.requests == []
