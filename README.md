# vast-api-client

## Description
This is a client library for interacting with the VAST web API. VAST is the storage platform used by Tufts HPC cluster and research storage.


## Usage
To instantiate and authenticate:
```
vast = VASTClientAPI('https://url_to_api_endpoint')
vast.get_token('username', 'password')
```

 or

 ```
vast = VASTClientAPI('https://url_to_api_endpoint', token='api_token', refresh_token='api_refresh_token')
```

currently supports:
get/create quota
get/create view
get dashboard data
