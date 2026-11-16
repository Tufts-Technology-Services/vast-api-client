import pytest

from vast_api_client import VASTClient


class RecordedCalls:
    """Stands in for VASTClient._call: records each request and answers with queued responses, or raises queued exceptions."""

    def __init__(self):
        self.requests = []
        self.timeouts = []
        self.responses = []

    def __call__(self, method, endpoint, *, query=None, body=None, timeout=None):
        self.requests.append((method, endpoint, query, body))
        self.timeouts.append(timeout)
        response = self.responses.pop(0) if self.responses else {}
        # an exception queued as a response is raised, as _call would for a failed request
        if isinstance(response, BaseException):
            raise response
        return response


@pytest.fixture
def api_calls():
    return RecordedCalls()


@pytest.fixture
def vast(api_calls):
    # the constructor sends nothing, so a user and password are safe here
    client = VASTClient("vast.example.edu", "alice", "secret")
    client._call = api_calls
    return client
