"""Tests for VastCredentials and for how VASTClient's constructor uses it."""

import pytest
import urllib3
from pydantic import ValidationError

from vast_api_client import VASTClient, VastCredentials

HOST = "vast.example.edu"
PASSWORD = "hunter2-password"
TOKEN = "tok-0123456789"


@pytest.fixture
def no_network(monkeypatch):
    """Fails the test if anything tries to open a connection pool."""

    def refuse(*args, **kwargs):
        raise AssertionError("the constructor must not make a request")

    monkeypatch.setattr(urllib3, "PoolManager", refuse)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"user": "alice"}, "user was supplied without a password"),
        ({"password": PASSWORD}, "password was supplied without a user"),
        ({"user": "alice", "password": PASSWORD, "token": TOKEN}, "not both"),
        ({"user": "alice", "token": TOKEN}, "not both"),
        ({"password": PASSWORD, "token": TOKEN}, "not both"),
        ({}, "no credentials"),
        ({"user": "", "password": ""}, "no credentials"),
        ({"tenant": "research"}, "no credentials"),
    ],
)
def test_bad_combinations(kwargs, message):
    with pytest.raises(ValueError, match=message) as caught:
        VastCredentials(**kwargs)
    assert PASSWORD not in str(caught.value)
    assert TOKEN not in str(caught.value)


def test_extra_fields_are_refused():
    with pytest.raises(ValueError, match="refresh_token"):
        VastCredentials(token=TOKEN, refresh_token="refresh")


def test_secrets_stay_out_of_repr():
    credentials = VastCredentials(user="alice", password=PASSWORD)
    assert PASSWORD not in repr(credentials)
    assert credentials.password.get_secret_value() == PASSWORD


def test_credentials_are_frozen():
    credentials = VastCredentials(token=TOKEN)
    with pytest.raises(ValidationError):
        credentials.token = "other"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"user": "alice"},
        {"password": PASSWORD},
        {"user": "alice", "password": PASSWORD, "token": TOKEN},
        {},
    ],
)
def test_client_refuses_bad_combinations(no_network, kwargs):
    with pytest.raises(ValueError) as caught:
        VASTClient(HOST, **kwargs)
    assert PASSWORD not in str(caught.value)
    assert TOKEN not in str(caught.value)


def test_client_positional_user_and_password(no_network):
    client = VASTClient(HOST, "alice", PASSWORD)
    assert client.host == HOST
    assert client.api is not None


def test_client_keyword_user_and_password(no_network):
    assert VASTClient(host=HOST, user="alice", password=PASSWORD).host == HOST


def test_token_is_keyword_only(no_network):
    with pytest.raises(TypeError):
        VASTClient(HOST, None, None, TOKEN)


def test_client_repr_has_no_secrets(no_network):
    for client in (VASTClient(HOST, "alice", PASSWORD), VASTClient(HOST, token=TOKEN)):
        text = repr(client)
        assert text == "VASTClient(host='vast.example.edu', version='v7')"
        assert PASSWORD not in text
        assert TOKEN not in text
