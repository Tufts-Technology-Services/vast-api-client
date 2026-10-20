import json
import logging
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import certifi
import urllib3
import vastpy

from vast_api_client.credentials import VastCredentials
from vast_api_client.exceptions import (
    ResourceExistsError,
    VastAPIError,
    VastConnectionError,
    VastError,
    VastNotFoundError,
)
from vast_api_client.models import (
    ACL,
    ACLGrantee,
    ACLPerm,
    CatalogQuery,
    FolderCreateOrUpdate,
    PathBody,
    ProtectedPathCreate,
    ProtocolEnum,
    QuotaCreate,
    QuotaUpdate,
    ShareCreate,
    SourceDirQuery,
    ViewCreate,
)
from vast_api_client.transport import DEFAULT_RETRIES, DEFAULT_TIMEOUT, TimedVASTClient

logger = logging.getLogger(__name__)

# urllib3 puts fields in the query string for these methods; for any other it would send them as a form body
QUERY_STRING_METHODS = {"GET", "HEAD", "OPTIONS", "DELETE"}


def _verify_certs_from_env() -> bool:
    """Legacy switch, read as the old client read it: anything but true, 1 or yes turns off verification."""
    return os.getenv("VERIFY_CERTS", "true").strip().lower() in ("true", "1", "yes")


def _reveal(secret) -> str | None:
    return secret.get_secret_value() if secret is not None else None


class VASTClient:
    """
    used to get information from the VAST storage unit.
    Requests go through vastpy. Its client is exposed as `api`, for endpoints not wrapped here:
    vast.api["quotas"].get(path="/rstore/lab")
    """

    def __init__(
        self,
        host: str,
        user: str = None,
        password: str = None,
        *,
        token: str = None,
        tenant: str = None,
        version: str = "v7",
        ca_file: str | Path = None,
        verify: bool = True,
        timeout: float | urllib3.Timeout | None = DEFAULT_TIMEOUT,
        retries: int | urllib3.Retry = DEFAULT_RETRIES,
    ):
        """
        supply either user and password, or an API token. Nothing is sent until the first request.
        :param host: VMS address, without scheme: "vast.example.edu"
        :param user:
        :param password:
        :param token: API token, sent as "Authorization: Api-Token <token>"
        :param tenant: tenant name, sent as X-Tenant-Name
        :param version: API version to use (default is 'v7')
        :param ca_file: CA bundle to verify the server certificate with (default is certifi's)
        :param verify: False skips certificate verification, as does setting the env var VERIFY_CERTS to
            anything but true, 1 or yes
        :param timeout: seconds, or a urllib3.Timeout for separate connect and read limits (default 10s to
            connect, 30s to read). None waits indefinitely.
        :param retries: retries, or a urllib3.Retry (default: one more try if the connection fails)
        """
        credentials = VastCredentials(user=user, password=password, token=token, tenant=tenant)
        self.host = host
        self.version = version

        if verify and _verify_certs_from_env():
            cert_file = str(ca_file) if ca_file is not None else certifi.where()
        else:
            # vastpy skips verification when it gets no cert_file
            logger.warning(f"TLS certificate verification is off for {host}")
            cert_file = None

        self.api = TimedVASTClient(
            user=credentials.user,
            password=_reveal(credentials.password),
            token=_reveal(credentials.token),
            tenant=credentials.tenant,
            address=host,
            cert_file=cert_file,
            version=version,
            timeout=timeout,
            retries=retries,
        )

    def __repr__(self):
        return f"VASTClient(host={self.host!r}, version={self.version!r})"

    def _call(self, method: str, endpoint: str, *, query: dict = None, body: dict = None):
        """
        Send one request to the API. Every request the client makes goes through here.
        :param method: GET, POST, PATCH or DELETE
        :param endpoint: path below api/<version>/, without leading or trailing slashes: "quotas/5"
        :param query: query parameters, for GET and DELETE
        :param body: JSON body, for POST, PATCH and DELETE
        :return: the parsed JSON response, or None if the response has no body or one that isn't JSON
        :raises VastNotFoundError: the API answered 404
        :raises VastAPIError: the API answered with another non-2xx status
        :raises VastConnectionError: no HTTP answer: connection refused, TLS failure, etc.
        :raises VastError: an answer that couldn't be read: malformed JSON, or an error body that isn't UTF-8
        """
        if query and method not in QUERY_STRING_METHODS:
            raise ValueError(f"query parameters can't be sent with {method}")
        logger.debug(f"{method} {endpoint} query={query} body={body}")
        try:
            result = self.api[endpoint].request(method, fields=query, data=body)
        except vastpy.RESTFailure as failure:
            error_class = VastNotFoundError if failure.status == 404 else VastAPIError
            raise error_class(
                status=failure.status,
                method=method,
                endpoint=endpoint,
                body=failure.data.decode("utf-8", errors="replace"),
            ) from failure
        except urllib3.exceptions.HTTPError as error:
            raise VastConnectionError(f"{method} {endpoint} on {self.host} failed: {error}") from error
        except ValueError as error:
            # vastpy's json.loads of a malformed body, or RESTFailure decoding a non-UTF-8 error body
            raise VastError(f"{method} {endpoint} on {self.host}: unreadable response: {error}") from error
        # vastpy returns raw bytes unless the Content-Type says JSON: b"" for a 204, but also JSON
        # under another Content-Type, which the old requests transport parsed regardless
        if isinstance(result, bytes):
            if not result:
                return None
            try:
                return json.loads(result)
            except ValueError:
                logger.warning(f"{method} {endpoint} answered with a body that isn't JSON; ignoring it")
                return None
        return result

    def get_quotas(self, path: Path = None):
        """
        Get all quotas or quotas for a specific path
        :param path: path for which to get quotas
        :return: list of quotas
        """
        if path is not None:
            query = PathBody(path=path).model_dump(mode="json", exclude_none=True)
            return self._call("GET", "quotas", query=query)
        return self._call("GET", "quotas")

    def get_views(self, path: Path = None):
        if path is not None:
            query = PathBody(path=path).model_dump(mode="json", exclude_none=True)
            return self._call("GET", "views", query=query)
        return self._call("GET", "views")

    @staticmethod
    def create_acl_from_str(perm: str, grantee: str, sid_str: str, uid_or_gid: int | None) -> ACL:
        """
        create an ACL object to be used with add_view if you don't easy access to model enums
        :return: ACL object
        """
        return ACL(
            perm=ACLPerm(perm),
            grantee=ACLGrantee(grantee),
            sid_str=sid_str,
            uid_or_gid=uid_or_gid,
        )

    def add_view(
        self,
        path: Path,
        protocols: set[ProtocolEnum] = None,
        share_name: str = None,
        policy_id: int = None,
        acls: set[ACL] = None,
        dry_run=False,
    ):
        """
        Add a view to the storage system
        :param path: path of the view
        :param protocols: set of protocols to be used with the view
        :param policy_id: policy to use with the view. create a new enum value if needed
        :param share_name: name of the share to be used with the view. must end with '$' if specified
        :param acls: set of ACLs to be used with the view.
        :param dry_run: if True, will not actually create the view
        :return: the API's response, or None on a dry run; a failure raises
        """
        # --gid-inheritance linux|bsd ; default is linux, but we should select bsd. new files inherit the gid of the parent directory
        if share_name is None:
            vc = ViewCreate(path=path, protocols=protocols, policy_id=policy_id)
        else:
            vc = ShareCreate(
                path=path,
                protocols=protocols,
                share=share_name,
                policy_id=policy_id,
                create_dir_acls=acls,
            )
        if dry_run:
            logger.info(f"dry run, not creating view: {vc.model_dump(mode='json')}")
            return None
        logger.info(f"creating view {vc}")
        return self._call("POST", "views", body=vc.model_dump(mode="json"))

    def add_quota(
        self,
        name: str,
        path: Path,
        hard_limit: int,
        soft_limit: int = None,
        dry_run=False,
    ):
        """
        Add a quota to the storage system
        :param name: name of the quota
        :param path: path of the quota
        :param hard_limit: hard limit of the quota in bytes
        :param soft_limit: soft limit of the quota in bytes
        :param dry_run: if True, will not actually create the quota
        :return: the API's response, or None on a dry run; a failure raises
        """
        name = name.removesuffix("$")
        qc = QuotaCreate(name=name, path=path, soft_limit=soft_limit, hard_limit=hard_limit)
        if dry_run:
            logger.info(f"dry run, not creating quota: {qc.model_dump(mode='json')}")
            return None
        logger.info(f"creating quota {qc}")
        return self._call("POST", "quotas", body=qc.model_dump(mode="json"))

    def add_folder(self, path: Path, group: str, user: str = None):
        """
        Add a folder to the storage system
        :param path: path of the folder
        :param group: group that owns the folder
        :param user: user that owns the folder
        """
        owner_is_group = group == user
        folder = FolderCreateOrUpdate(path=path, owner_is_group=owner_is_group, user=user, group=group)
        return self._call("POST", "folders/create_folder", body=folder.model_dump(mode="json"))

    def modify_folder(self, path: Path, group: str, user: str = None):
        """
        update folder
        :param path: path of the folder
        :param group: group that owns the folder
        :param user: user that owns the folder
        """
        folder = FolderCreateOrUpdate(path=path, group=group, user=user, owner_is_group=group == user)
        # only what's changing: a user of None and owner_is_group False are left out
        return self._call("POST", "folders/create_folder", body=folder.model_dump(exclude_defaults=True, mode="json"))

    def delete_folder(self, path: Path, tenant_id: int = None):
        """
        DELETE /folders/delete_folder/
        :return: {"status": "deleted"}; a failure raises
        """
        body = PathBody(path=path, tenant_id=tenant_id).model_dump(mode="json", exclude_none=True)
        self._call("DELETE", "folders/delete_folder", body=body)
        return {"status": "deleted"}

    def get_folder(self, path: Path, tenant_id: int = None):
        """
        response:
        {
            "owning_user": "string",
            "owning_uid": "string",
            "owning_group": "string",
            "owning_gid": "string",
            "has_default_acl": true,
            "is_directory": true,
            "children": 0
        }
        """
        body = PathBody(path=path, tenant_id=tenant_id).model_dump(mode="json", exclude_none=True)
        return self._call("POST", "folders/stat_path", body=body)

    def update_quota_size(self, quota_id: int, new_size: int):
        """
        :param quota_id: int
        :param new_size: int in bytes
        :return: the API's response; a failure raises
        """
        if isinstance(quota_id, int) and isinstance(new_size, int):
            body = QuotaUpdate(soft_limit=new_size, hard_limit=new_size)
            return self._call("PATCH", f"quotas/{quota_id}", body=body.model_dump(mode="json"))
        else:
            raise TypeError("quota_id and size must be of type int")

    def delete_quota(self, quota_id: int):
        """
        :return: {"status": "deleted"}; a failure raises
        """
        if isinstance(quota_id, int):
            self._call("DELETE", f"quotas/{quota_id}")
            return {"status": "deleted"}
        else:
            raise TypeError("quota_id must be of type int")

    def get_total_capacity(self):
        """
        :return:
        """
        return self._call("GET", "capacity")

    def get_protected_paths(self, source_dir: Path = None):
        if source_dir is None:
            return self._call("GET", "protectedpaths")
        query = SourceDirQuery(source_dir=source_dir).model_dump(mode="json")
        return self._call("GET", "protectedpaths", query=query)

    def add_protected_path(self, name: str, source_dir: Path, protection_policy_id: int, tenant_id: int):
        """
        Add a protected path to the storage system
        :param name: name of the protected path
        :param source_dir: source directory of the protected path
        :param protection_policy_id: id of the protection policy to use
        :param tenant_id: id of the tenant
        :return: the API's response; a failure raises
        """
        ppc = ProtectedPathCreate(
            name=name,
            source_dir=source_dir,
            protection_policy_id=protection_policy_id,
            tenant_id=tenant_id,
        )
        body = ppc.model_dump(mode="json")
        protected_paths = self.get_protected_paths(ppc.source_dir)
        if body["source_dir"] in [i["source_dir"] for i in protected_paths]:
            raise ResourceExistsError(f"This path already exists as a protected path on {self.host}!")
        return self._call("POST", "protectedpaths", body=body)

    def get_protection_policies(self, name: str = None, policy_id: int = None, source_dir: Path = None):
        if policy_id is not None:
            return self._call("GET", f"protectionpolicies/{policy_id}")
        if name is not None:
            return self._call("GET", "protectionpolicies", query={"name": name})
        if source_dir is not None:
            query = SourceDirQuery(source_dir=source_dir).model_dump(mode="json")
            return self._call("GET", "protectionpolicies", query=query)
        return self._call("GET", "protectionpolicies")

    def query_catalog(
        self,
        filters: dict[str, list[dict[str, Any]]],
        fields: Sequence[str] = ("name",),
        path: str = "/",
    ):
        """
        Query the VAST Catalog. The directories in /smb/gisusers/, say:
        vast.query_catalog({"parent_path": [{"eq": "/smb/gisusers/"}], "element_type": [{"eq": "DIR"}]})
        :param filters: catalog column to a list of {operator: value} conditions; VAST needs at least one
        :param fields: catalog columns to return
        :param path: where in the catalog to look
        :return: catalog entries matching the query
        """
        query = CatalogQuery(path=path, filters=filters, fields=fields)
        return self._call("POST", "bigcatalogconfig/query_data", body=query.model_dump(mode="json"))
