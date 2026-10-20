"""
Tests for what goes over the wire: VASTClient._call through vastpy down to urllib3.
urllib3.PoolManager is replaced with a fake, so nothing leaves the process.
"""

import base64
import json
import logging
import pickle
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path

import certifi
import pytest
import urllib3
import vastpy

from vast_api_client import VastAPIError, VASTClient, VastConnectionError, VastError, VastNotFoundError
from vast_api_client.transport import TimedVASTClient

HOST = "vast.example.edu"


@dataclass
class FakeResponse:
    status: int = 200
    data: bytes = b""
    headers: dict = field(default_factory=dict)


@dataclass
class SentRequest:
    method: str
    url: str
    headers: dict
    fields: list | None
    body: bytes | None


class FakeVMS:
    """Stands in for urllib3.PoolManager: records pools and requests, and answers with queued replies."""

    def __init__(self):
        self.pool_settings = []
        self.requests = []
        self.replies = []

    def reply_json(self, payload, status=200):
        self.replies.append(FakeResponse(status, json.dumps(payload).encode(), {"Content-Type": "application/json"}))

    def reply_raw(self, data: bytes, status=200, content_type="text/plain"):
        self.replies.append(FakeResponse(status, data, {"Content-Type": content_type}))

    def fail_with(self, error: Exception):
        self.replies.append(error)

    def next_reply(self):
        reply = self.replies.pop(0) if self.replies else FakeResponse(204)
        if isinstance(reply, Exception):
            raise reply
        return reply


@pytest.fixture
def vms(monkeypatch):
    fake = FakeVMS()

    class FakePoolManager:
        def __init__(self, **kwargs):
            fake.pool_settings.append(kwargs)

        def request(self, method, url, headers=None, fields=None, body=None):
            fake.requests.append(SentRequest(method, url, headers, fields, body))
            return fake.next_reply()

    monkeypatch.setattr(urllib3, "PoolManager", FakePoolManager)
    # vastpy calls this when verification is off; keep it from changing warning filters for the whole test run
    monkeypatch.setattr(urllib3, "disable_warnings", lambda category=None: None)
    monkeypatch.delenv("VERIFY_CERTS", raising=False)
    return fake


@pytest.fixture
def client(vms):
    return VASTClient(HOST, "alice", "secret")


# URLs, query strings and bodies


def test_get_url(client, vms):
    vms.reply_json([{"id": 1}])
    assert client._call("GET", "quotas") == [{"id": 1}]
    assert vms.requests[0].method == "GET"
    assert vms.requests[0].url == f"https://{HOST}/api/v7/quotas/"
    assert vms.requests[0].body is None


def test_endpoint_with_an_id(client, vms):
    client._call("GET", "quotas/5")
    assert vms.requests[0].url == f"https://{HOST}/api/v7/quotas/5/"


def test_version_override(vms):
    VASTClient(HOST, "alice", "secret", version="v5")._call("GET", "quotas")
    assert vms.requests[0].url == f"https://{HOST}/api/v5/quotas/"


def test_query_fields(client, vms):
    client._call("GET", "quotas", query={"path": "/rstore/lab"})
    assert vms.requests[0].fields == [("path", "/rstore/lab")]


def test_real_pool_manager_encodes_the_query_and_delete_body(monkeypatch):
    # the fake above takes fields as given; check that real urllib3 turns them into a query string
    # and keeps a DELETE body, one level down at urlopen
    sent = []

    def fake_urlopen(self, method, url, redirect=True, **kw):
        sent.append((method, url, kw.get("body")))
        return FakeResponse(204)

    monkeypatch.setattr(urllib3.PoolManager, "urlopen", fake_urlopen)
    client = VASTClient(HOST, "alice", "secret")
    client._call("GET", "quotas", query={"path": "/rstore/lab"})
    client._call("DELETE", "folders/delete_folder", body={"path": "/rstore/lab"})
    assert sent[0] == ("GET", f"https://{HOST}/api/v7/quotas/?path=%2Frstore%2Flab", None)
    assert sent[1][:2] == ("DELETE", f"https://{HOST}/api/v7/folders/delete_folder/")
    assert json.loads(sent[1][2]) == {"path": "/rstore/lab"}


def test_post_sends_a_json_body(client, vms):
    vms.reply_json({"id": 7}, status=201)
    assert client._call("POST", "quotas", body={"name": "lab", "hard_limit": 1000}) == {"id": 7}
    sent = vms.requests[0]
    assert sent.method == "POST"
    assert sent.url == f"https://{HOST}/api/v7/quotas/"
    assert sent.headers["Content-Type"] == "application/json"
    assert json.loads(sent.body) == {"name": "lab", "hard_limit": 1000}
    assert sent.fields is None


def test_query_is_refused_with_post(client, vms):
    with pytest.raises(ValueError, match="POST"):
        client._call("POST", "quotas", query={"path": "/rstore/lab"})
    assert vms.requests == []


def test_api_attribute_reaches_unwrapped_endpoints(client, vms):
    vms.reply_json([])
    assert client.api["clusters"].get(name="vast") == []
    assert vms.requests[0].url == f"https://{HOST}/api/v7/clusters/"


# auth headers


def test_basic_auth(client, vms):
    client._call("GET", "quotas")
    expected = base64.b64encode(b"alice:secret").decode()
    assert vms.requests[0].headers["authorization"] == f"Basic {expected}"
    assert "X-Tenant-Name" not in vms.requests[0].headers


def test_api_token(vms):
    VASTClient(HOST, token="tok123")._call("GET", "quotas")
    assert vms.requests[0].headers["authorization"] == "Api-Token tok123"


def test_tenant_header(vms):
    VASTClient(HOST, token="tok123", tenant="research")._call("GET", "quotas")
    assert vms.requests[0].headers["X-Tenant-Name"] == "research"


# TLS


def test_certifi_bundle_by_default(client, vms):
    client._call("GET", "quotas")
    assert vms.pool_settings[0]["ca_certs"] == certifi.where()
    assert "cert_reqs" not in vms.pool_settings[0]


def test_ca_file(vms, tmp_path):
    ca_file = tmp_path / "campus-ca.pem"
    VASTClient(HOST, "alice", "secret", ca_file=ca_file)._call("GET", "quotas")
    assert vms.pool_settings[0]["ca_certs"] == str(ca_file)


def test_verify_false(vms, caplog):
    with caplog.at_level(logging.WARNING):
        client = VASTClient(HOST, "alice", "secret", verify=False)
    assert "verification is off" in caplog.text
    client._call("GET", "quotas")
    assert vms.pool_settings[0]["cert_reqs"] == "CERT_NONE"
    assert "ca_certs" not in vms.pool_settings[0]


@pytest.mark.parametrize("setting", ["false", "0", "no", "False", "off", ""])
def test_verify_certs_env_var(vms, monkeypatch, setting):
    monkeypatch.setenv("VERIFY_CERTS", setting)
    VASTClient(HOST, "alice", "secret")._call("GET", "quotas")
    assert vms.pool_settings[0]["cert_reqs"] == "CERT_NONE"
    assert "ca_certs" not in vms.pool_settings[0]


def test_verify_certs_env_var_true_keeps_verification(vms, monkeypatch):
    monkeypatch.setenv("VERIFY_CERTS", "true")
    VASTClient(HOST, "alice", "secret")._call("GET", "quotas")
    assert vms.pool_settings[0]["ca_certs"] == certifi.where()


# responses and errors


def test_404_raises_not_found(client, vms):
    vms.reply_raw(b'{"detail": "Not found."}', status=404, content_type="application/json")
    with pytest.raises(VastNotFoundError) as caught:
        client._call("GET", "quotas/5")
    error = caught.value
    assert error.status == 404
    assert error.method == "GET"
    assert error.endpoint == "quotas/5"
    assert error.body == '{"detail": "Not found."}'
    assert isinstance(error.__cause__, vastpy.RESTFailure)


def test_500_raises_api_error(client, vms):
    vms.reply_raw(b"server fell over", status=500)
    with pytest.raises(VastAPIError) as caught:
        client._call("POST", "quotas", body={"name": "lab"})
    assert not isinstance(caught.value, VastNotFoundError)
    assert caught.value.status == 500
    assert caught.value.body == "server fell over"
    assert isinstance(caught.value.__cause__, vastpy.RESTFailure)


@pytest.mark.parametrize(
    "transport_error",
    [
        urllib3.exceptions.NewConnectionError(None, "connection refused"),
        urllib3.exceptions.MaxRetryError(None, "/api/v7/quotas/", reason="connection refused"),
        urllib3.exceptions.SSLError("certificate verify failed"),
    ],
)
def test_transport_errors_raise_connection_error(client, vms, transport_error):
    vms.fail_with(transport_error)
    with pytest.raises(VastConnectionError, match="GET quotas") as caught:
        client._call("GET", "quotas")
    assert caught.value.__cause__ is transport_error


def test_empty_204_returns_none(client, vms):
    vms.reply_raw(b"", status=204, content_type="")
    assert client._call("DELETE", "quotas/5") is None


def test_non_json_body_returns_none(client, vms, caplog):
    vms.reply_raw(b"ok")
    assert client._call("POST", "folders/create_folder", body={"path": "/x"}) is None
    assert "isn't JSON" in caplog.text


def test_json_under_another_content_type_is_parsed(client, vms):
    vms.reply_raw(b'[{"id": 1}]', content_type="text/html")
    assert client._call("GET", "quotas") == [{"id": 1}]


def test_malformed_json_raises_vast_error(client, vms):
    vms.reply_raw(b'[{"id": 1', content_type="application/json")
    with pytest.raises(VastError) as caught:
        client._call("GET", "quotas")
    assert isinstance(caught.value.__cause__, ValueError)


def test_non_utf8_error_body_raises_vast_error(client, vms):
    vms.reply_raw(b"\xff\xfe bad gateway", status=502, content_type="text/html")
    with pytest.raises(VastError) as caught:
        client._call("GET", "quotas")
    assert isinstance(caught.value.__cause__, UnicodeDecodeError)


def test_delete_quota(client, vms):
    vms.reply_raw(b"", status=204, content_type="")
    assert client.delete_quota(5) == {"status": "deleted"}
    assert vms.requests[0].method == "DELETE"
    assert vms.requests[0].url == f"https://{HOST}/api/v7/quotas/5/"
    assert vms.requests[0].body is None


def test_delete_folder_sends_a_body(client, vms):
    assert client.delete_folder(Path("/rstore/lab"), tenant_id=1) == {"status": "deleted"}
    assert vms.requests[0].url == f"https://{HOST}/api/v7/folders/delete_folder/"
    assert json.loads(vms.requests[0].body) == {"path": "/rstore/lab", "tenant_id": 1}


@pytest.mark.parametrize("error_class", [VastAPIError, VastNotFoundError])
def test_api_errors_survive_pickling(error_class):
    error = error_class(status=404, method="GET", endpoint="quotas/5", body="gone")

    restored = pickle.loads(pickle.dumps(error))

    assert type(restored) is error_class
    assert (restored.status, restored.method, restored.endpoint, restored.body) == (404, "GET", "quotas/5", "gone")
    assert str(restored) == str(error)


# timeout and retries


def test_default_timeout_and_retries_reach_the_pool(client, vms):
    client._call("GET", "quotas")

    settings = vms.pool_settings[0]
    assert (settings["timeout"].connect_timeout, settings["timeout"].read_timeout) == (10, 30)
    assert (settings["retries"].total, settings["retries"].read) == (1, 0)


def test_timeout_and_retries_can_be_set(vms):
    VASTClient(HOST, "alice", "secret", timeout=5, retries=0)._call("GET", "quotas")

    assert vms.pool_settings[0]["timeout"] == 5
    assert vms.pool_settings[0]["retries"] == 0


def test_timeout_is_kept_by_child_clients(vms):
    client = VASTClient(HOST, "alice", "secret", timeout=5, verify=False)

    client.api["folders"]["stat_path"].post(path="/x")
    client.api.quotas.get()

    assert [settings["timeout"] for settings in vms.pool_settings] == [5, 5]


@pytest.mark.parametrize(
    "credentials, method, fields, data",
    [
        ({"user": "alice", "password": "secret"}, "GET", {"path": "/x", "fields": ["id", "name"]}, None),
        ({"token": "abc", "tenant": "research"}, "POST", None, {"path": "/x", "owner_is_group": True}),
        ({"user": "alice", "password": "secret"}, "DELETE", None, {"path": "/x"}),
    ],
)
def test_timed_request_matches_vastpy(vms, credentials, method, fields, data):
    """TimedVASTClient.request is a copy of vastpy's: what it sends must not drift from the original."""
    settings = {"address": HOST, "cert_file": certifi.where(), "version": "v7", **credentials}
    for client in (vastpy.VASTClient(**settings), TimedVASTClient(**settings)):
        vms.reply_json({"id": 1})
        assert client["folders/stat_path"].request(method, fields=fields, data=data) == {"id": 1}

    original, timed = vms.requests
    assert timed == original
    original_pool, timed_pool = vms.pool_settings
    assert {key: value for key, value in timed_pool.items() if key not in ("timeout", "retries")} == original_pool


def test_timed_request_raises_rest_failure_like_vastpy(vms):
    vms.reply_raw(b"nope", status=403)
    with pytest.raises(vastpy.RESTFailure) as caught:
        TimedVASTClient(address=HOST, user="alice", password="secret", version="v7")["quotas"].request("GET")
    assert (caught.value.status, caught.value.url) == (403, "api/v7/quotas")


@pytest.fixture
def silent_server():
    """A TCP port that accepts connections (through the listen backlog) and never answers."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("127.0.0.1", 0))
    except PermissionError:
        listener.close()
        pytest.skip("binding a local port isn't allowed here")
    listener.listen(8)
    yield f"127.0.0.1:{listener.getsockname()[1]}"
    listener.close()


def test_a_server_that_never_answers_times_out(silent_server):
    client = VASTClient(silent_server, "alice", "secret", timeout=0.2)

    started = time.monotonic()
    with pytest.raises(VastConnectionError):
        client.get_quotas()

    # 0.2s, and once more for the retry, rather than forever; and not at once, for some other reason
    assert 0.2 <= time.monotonic() - started < 2


def socket_io_allowed() -> bool:
    ours, theirs = socket.socketpair()
    try:
        ours.sendall(b"x")
        return theirs.recv(1) == b"x"
    except PermissionError:
        return False
    finally:
        ours.close()
        theirs.close()


def test_a_connection_that_never_answers_times_out(monkeypatch):
    """The same, without binding a port: urllib3 gets one end of a socket pair whose other end stays silent."""
    if not socket_io_allowed():
        pytest.skip("socket I/O isn't allowed here")
    silent_ends = []

    def connect_to_silence(address, timeout=None, *args, **kwargs):
        ours, theirs = socket.socketpair()
        silent_ends.append(theirs)
        ours.settimeout(timeout)
        return ours

    monkeypatch.setattr(urllib3.util.connection, "create_connection", connect_to_silence)
    # without a CA bundle to load, which some sandboxes refuse: this is about waiting, not verifying
    client = VASTClient(HOST, "alice", "secret", timeout=0.2, verify=False)

    started = time.monotonic()
    with pytest.raises(VastConnectionError):
        client.get_quotas()

    assert 0.2 <= time.monotonic() - started < 2
    # a stalled handshake is a read timeout, which isn't retried
    assert len(silent_ends) == 1
    for end in silent_ends:
        end.close()


def test_a_refused_connection_is_retried_once(monkeypatch):
    attempts = []

    def refuse(address, timeout=None, *args, **kwargs):
        attempts.append(address)
        raise ConnectionRefusedError("refused")

    monkeypatch.setattr(urllib3.util.connection, "create_connection", refuse)

    with pytest.raises(VastConnectionError):
        VASTClient(HOST, "alice", "secret", verify=False).get_quotas()
    assert attempts == [(HOST, 443), (HOST, 443)]


# get_folder of a missing path: VAST answers 503, not 404

MISSING_FOLDER_BODY = b'{"detail":"Template directory path wasn\'t found","code":"service_unavailable"}'


def test_get_folder_of_a_missing_path_raises_not_found(client, vms):
    vms.reply_raw(MISSING_FOLDER_BODY, status=503, content_type="application/json")

    with pytest.raises(VastNotFoundError) as caught:
        client.get_folder(Path("/projects/lab/missing"))

    error = caught.value
    assert (error.status, error.method, error.endpoint) == (503, "POST", "folders/stat_path")
    assert error.code == "service_unavailable"
    assert type(error.__cause__) is VastAPIError
    assert isinstance(error.__cause__.__cause__, vastpy.RESTFailure)


@pytest.mark.parametrize(
    "status, body",
    [
        (503, b'{"detail":"VMS is busy","code":"throttled"}'),
        (503, b'{"detail":"VMS is busy","code":"service_unavailable"}'),
        (503, b'{"code":"service_unavailable"}'),
        (503, b"<html>503 Service Unavailable</html>"),
        (500, MISSING_FOLDER_BODY),
    ],
)
def test_get_folder_keeps_other_errors(client, vms, status, body):
    vms.reply_raw(body, status=status, content_type="application/json")

    with pytest.raises(VastAPIError) as caught:
        client.get_folder(Path("/projects/lab"))

    assert type(caught.value) is VastAPIError


def test_get_folder_of_a_404_is_still_not_found(client, vms):
    vms.reply_raw(b'{"detail": "Not found."}', status=404, content_type="application/json")

    with pytest.raises(VastNotFoundError):
        client.get_folder(Path("/projects/lab"))


def test_a_missing_folder_503_elsewhere_is_not_mapped(client, vms):
    vms.reply_raw(MISSING_FOLDER_BODY, status=503, content_type="application/json")

    with pytest.raises(VastAPIError) as caught:
        client.get_quotas()

    assert type(caught.value) is VastAPIError


@pytest.mark.parametrize(
    "body, code",
    [
        ('{"detail": "x", "code": "service_unavailable"}', "service_unavailable"),
        ('{"detail": "x"}', None),
        ("[1, 2]", None),
        ("<html>bad gateway</html>", None),
        ("", None),
    ],
)
def test_api_error_code(body, code):
    assert VastAPIError(status=503, method="GET", endpoint="quotas", body=body).code == code


@pytest.mark.parametrize(
    "body, detail",
    [
        (MISSING_FOLDER_BODY.decode(), "Template directory path wasn't found"),
        ('{"code": "x"}', None),
        ("<html>bad gateway</html>", None),
    ],
)
def test_api_error_detail(body, detail):
    assert VastAPIError(status=503, method="GET", endpoint="quotas", body=body).detail == detail
