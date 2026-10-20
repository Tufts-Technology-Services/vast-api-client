import json


class VastError(Exception):
    """Base class for every error this library raises."""


class VastAPIError(VastError):
    """
    The API answered with a non-2xx status. Chained from the vastpy RESTFailure.
    :ivar status: HTTP status code
    :ivar method: HTTP method of the request
    :ivar endpoint: path below api/<version>/ that was requested: "quotas/5"
    :ivar body: response body, decoded as text
    """

    def __init__(self, status: int, method: str, endpoint: str, body: str):
        self.status = status
        self.method = method
        self.endpoint = endpoint
        self.body = body
        super().__init__(f"{method} {endpoint} failed with HTTP {status}: {body}")

    # pickle rebuilds an exception from its args, which hold only the message
    def __reduce__(self):
        return (self.__class__, (self.status, self.method, self.endpoint, self.body))

    @property
    def code(self) -> str | None:
        """the "code" in VAST's JSON error body, such as "service_unavailable"; None if there isn't one"""
        try:
            error_body = json.loads(self.body)
        except ValueError:
            return None
        return error_body.get("code") if isinstance(error_body, dict) else None


class VastNotFoundError(VastAPIError):
    """What was asked for doesn't exist: the API answered 404, or, for get_folder, VAST's 503 for a missing path."""


class VastConnectionError(VastError):
    """The request never got an HTTP answer: connection, TLS or protocol failure. Chained from urllib3's error."""


class ResourceExistsError(VastError, ValueError):
    """The resource to be created already exists."""
