import pytest

from vast_api_client import VASTClient


class RecordedCalls:
    """Stands in for VASTClient._call: records each request and answers with queued responses."""

    def __init__(self):
        self.requests = []
        self.responses = []

    def __call__(self, method, endpoint, *, query=None, body=None):
        self.requests.append((method, endpoint, query, body))
        return self.responses.pop(0) if self.responses else {}


@pytest.fixture
def api_calls():
    return RecordedCalls()


@pytest.fixture
def vast(api_calls):
    # a token and refresh token, so the constructor doesn't log in
    client = VASTClient("vast.example.edu", token="access", refresh_token="refresh")
    client._call = api_calls
    return client
