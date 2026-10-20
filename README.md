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

Wrapped endpoints, each validating its request with a pydantic model before anything is sent:
- quotas: `get_quotas`, `add_quota`, `update_quota_size`, `delete_quota`
- views and SMB shares: `get_views`, `add_view` (ACLs from `create_acl_from_str`)
- folders: `get_folder`, `add_folder`, `modify_folder`, `delete_folder`
- protection: `get_protected_paths`, `add_protected_path`, `get_protection_policies`
- capacity: `get_total_capacity`
- catalog: `query_catalog`

## Development
```
uv sync
uv run pytest
uv run ruff check . && uv run ruff format --check .
```
