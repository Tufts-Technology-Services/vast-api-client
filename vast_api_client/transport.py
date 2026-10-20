import json

import urllib3
import vastpy
from urllib3.exceptions import InsecureRequestWarning

# The old requests client gave up after 10-30s, depending on the method
DEFAULT_TIMEOUT = urllib3.Timeout(connect=10, read=30)
# Catalog queries took 14-67s on the HPC VMS (2026-10-20), whatever their filters or path
CATALOG_TIMEOUT = urllib3.Timeout(connect=10, read=300)
# One more try when the connection fails; none after a read timeout, which waiting again rarely fixes
# and which would send a POST twice
DEFAULT_RETRIES = urllib3.Retry(total=1, read=0)


class TimedVASTClient(vastpy.VASTClient):
    """
    vastpy's client, with a timeout and a retry policy. vastpy 0.3.22 builds a new PoolManager for each
    request and has no way to pass either, so request() below is vastpy's own, with the two added to the
    PoolManager. Delete this class once vastpy takes a timeout, and pass it to vastpy.VASTClient instead.
    """

    def __init__(self, *args, timeout=DEFAULT_TIMEOUT, retries=DEFAULT_RETRIES, **kwargs):
        super().__init__(*args, **kwargs)
        self._timeout = timeout
        self._retries = retries

    def __getitem__(self, part):
        # vastpy builds the child from its own constructor arguments, which don't include these
        child = super().__getitem__(part)
        child._timeout = self._timeout
        child._retries = self._retries
        return child

    def endpoint_client(self, part, timeout=None):
        """self[part], with a timeout of its own if one is given: for an endpoint slower than the rest"""
        child = self[part]
        if timeout is not None:
            child._timeout = timeout
        return child

    # vastpy 0.3.22's VASTClient.request, but for the timeout and retries passed to PoolManager
    def request(self, method, fields=None, data=None):
        pool_settings = {"timeout": self._timeout, "retries": self._retries}
        if self._cert_file:
            pm = urllib3.PoolManager(ca_certs=self._cert_file, server_hostname=self._cert_server_name, **pool_settings)
        else:
            pm = urllib3.PoolManager(cert_reqs="CERT_NONE", **pool_settings)
            urllib3.disable_warnings(category=InsecureRequestWarning)
        if self._token:
            headers = {"authorization": f"Api-Token {self._token}"}
        else:
            headers = urllib3.make_headers(basic_auth=self._user + ":" + self._password)
        if self._tenant:
            headers["X-Tenant-Name"] = self._tenant
        if data:
            headers["Content-Type"] = "application/json"
            data = json.dumps(data).encode("utf-8")
        if fields:
            result = []
            for k, v in fields.items():
                if isinstance(v, list):
                    result.extend((k, i) for i in v)
                else:
                    result.append((k, v))
            fields = result
        version_path = f"/{self._version}" if self._version else ""
        r = pm.request(
            method, f"https://{self._address}/{self._url}{version_path}/", headers=headers, fields=fields, body=data
        )
        if r.status not in vastpy.SUCCESS_CODES:
            raise vastpy.RESTFailure(method, self._url, fields, r.status, r.data)
        data = r.data
        if "application/json" in r.headers.get("Content-Type", "") and data:
            return json.loads(data.decode("utf-8"))
        return data
