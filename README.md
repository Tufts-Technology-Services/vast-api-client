# vast-api-client

## Description
This is a client library for interacting with the VAST web API. VAST is the storage platform used by Tufts HPC cluster and research storage.


## Usage
Requests go through [vastpy](https://github.com/vast-data/vastpy). Authenticate with a user and password
(HTTP basic auth) or with an API token, not both. Nothing is sent until the first request.
```
from vast_api_client import VASTClient

vast = VASTClient('vast.example.edu', 'username', 'password')
vast = VASTClient('vast.example.edu', token='api_token', tenant='tenant_name')
```

The server certificate is checked against certifi's CA bundle, or `ca_file='/path/to/ca.pem'`.
`verify=False` (or the env var `VERIFY_CERTS=false`) turns checking off. The API version defaults to `v7`.

Failed requests raise `VastAPIError` (`VastNotFoundError` for a 404) or `VastConnectionError`, all subclasses
of `VastError`. Endpoints without a wrapper method are reachable through vastpy directly: `vast.api['clusters'].get()`.

currently supports:
get/create quota
get/create view
get dashboard data
