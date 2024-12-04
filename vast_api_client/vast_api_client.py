from pathlib import Path
from vast_api_client.models import *
from vast_api_client.abstract_client import AbstractClient


class ResourceExistsError(ValueError):
    pass


def gib_to_bytes(gb: int):
    return 1073741824 * gb


def gb_to_bytes(gb: int):
    return 1000000000 * gb


def bytes_to_gib(in_bytes: int):
    return in_bytes / 1073741824


class VASTClient(AbstractClient):
    """
    used to get information from the VAST storage unit
    """

    def __init__(self, host: str, user: str = None, password: str = None, token: str = None, refresh_token: str = None):
        """
        you can supply a token and refresh token directly if you have one already
        :param user:
        :param password:
        :param host:
        :param token:
        :param refresh_token:
        """
        self.host = host
        self.url = Path(f'https://{host}', 'api/latest')
       
        if token is not None and refresh_token is not None:
            self.token = token
            self.refresh_token = refresh_token
        elif user is not None and password is not None:
            self.get_token(user, password)
        else:
            raise ValueError('You must supply either a username and password or a token and refresh token')

    def get_token(self, username, passwd) -> None:
        """
        use to get a token and refresh token. token has key 'access', refresh token has key 'refresh'
        :param username:
        :param passwd:
        """
        body = {'username': username, 'password': passwd}
        r = self._send_post_request('token/', body, skip_auth=True)
        self.token = r['access']
        self.refresh_token = r['refresh']

    def renew_token(self, refresh_token) -> None:
        body = {'refresh': refresh_token}
        r = self._send_post_request('token/refresh/', body, skip_auth=True)
        self.token = r['access']
        self.refresh_token = r['refresh']

    def get_quotas(self, path: Path = None):
        """
        Get all quotas or quotas for a specific path
        :param path: path for which to get quotas
        :return: list of quotas
        """
        if path is not None:
            return self._send_get_request('quotas/', params={'path': path.as_posix()})
        else:
            return self._send_get_request('quotas/')

    def get_views(self, path: Path = None):
        if path is not None:
            return self._send_get_request('views/', params={'path': path.as_posix()})
        return self._send_get_request('views/')

    def get_status(self):
        return self._send_get_request('latest/dashboard/status/')

    def add_view(self, name: str, path: Path,
                protocols: set[ProtocolEnum] = {},
                policy_id: PolicyEnum = None,
                dry_run=False):
        """
        Add a view to the storage system
        :param name: name of the view
        :param path: path of the view
        :param protocols: set of protocols to be used with the view
        :param policy_id: policy to use with the view. create a new enum value if needed
        :param dry_run: if True, will not actually create the view
        :return: message indicating success or failure
        """
        view_matches = self.get_views(path=path)
        if len(view_matches) > 0:
            raise ResourceExistsError(f'This path already exists as a view on {self.host}!')

        vc = ViewCreate(share=name, path=path, protocols=protocols, policy_id=policy_id)
        print(f"creating view {vc}")
        if not dry_run:
            return self._send_post_request('views/', vc.model_dump())
        else:
            print('skipping creation for dry run!')
            print(vc.model_dump())

    def add_quota(self, name: str, path: Path,
                hard_limit: int, soft_limit: int = None,
                dry_run=False):
        """
        Add a quota to the storage system
        :param name: name of the quota
        :param path: path of the quota
        :param hard_limit: hard limit of the quota in bytes
        :param soft_limit: soft limit of the quota in bytes
        :param dry_run: if True, will not actually create the quota
        :return: message indicating success or failure
        """
        matches = self.get_quotas(path=path)
        if len(matches) > 0:
            raise ResourceExistsError(f'A quota for this path already exists on {self.host}!')
        
        name = name[:-1] if name.endswith('$') else name
        qc = QuotaCreate(name=name, path=path, soft_limit=soft_limit, hard_limit=hard_limit)
        print(f"creating quota {qc}")
        if dry_run:
            print('skipping creation for dry run!')
            print(qc.model_dump())
        else:
            return self._send_post_request('quotas/', qc.model_dump())
    
    def update_quota_size(self, quota_id: int, new_size: int):
        if isinstance(quota_id, int) and isinstance(new_size, int):
            return self._send_put_request(f'quotas/{str(quota_id)}/', {'size': new_size})
        else:
            raise TypeError('quota_id and size must be of type int')

    def delete_quota(self, quota_id: int):
        if isinstance(quota_id, int):
            return self._send_delete_request(f'quotas/{str(quota_id)}/')
        else:
            raise TypeError('quota_id must be of type int')

    def is_base10(self):
        r = self.get_status()
        return r['vms'][0]['capacity_base_10']

    def get_total_capacity(self):
        """
        :return:
        """
        r = self.get_status()
        return r['vms'][0]
    
    def get_protected_paths(self, source_dir: Path = None):
        if source_dir is None:
            return self._send_get_request('protectedpaths/')
        return self._send_get_request('protectedpaths/', params={'source_dir': source_dir.as_posix()})

    def add_protected_path(self, name: str, source_dir: Path, protection_policy_id: int, tenant_id: int):
        """
        Add a protected path to the storage system
        :param name: name of the protected path
        :param source_dir: source directory of the protected path
        :param protection_policy_id: id of the protection policy to use
        :param tenant_id: id of the tenant
        :return: message indicating success or failure
        """
        protected_paths = self._send_get_request('protectedpaths/', params={'source_dir': source_dir.as_posix()})
        if source_dir.as_posix() in [i['source_dir'] for i in protected_paths]:
            raise ResourceExistsError(f'This path already exists as a protected path on {self.host}!')
        ppc = ProtectedPathCreate(name=name, source_dir=source_dir, protection_policy_id=protection_policy_id, tenant_id=tenant_id)
        return self._send_post_request('protectedpaths/', ppc.model_dump())
    
    def get_protection_policies(self, name: str = None, policy_id: int = None, source_dir: Path = None):
        if id is not None:
            return self._send_get_request(f'protectionpolicies/{str(policy_id)}/')
        if name is not None:
            return self._send_get_request('protectionpolicies/', params={'name': name})
        if source_dir is not None:
            return self._send_get_request('protectionpolicies/', params={'source_dir': source_dir.as_posix()})
        return self._send_get_request('protectionpolicies/')

