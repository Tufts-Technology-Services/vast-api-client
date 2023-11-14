import requests
import os
import json
import re
from pathlib import Path


class VASTClient:
    """
    used to get information from the VAST hpc storage unit
    """

    def __init__(self, url: str, token: str = None, refresh_token: str = None):
        """
        you can supply a token and refresh token directly if you have one already
        :param token:
        :param refresh_token:
        :param url:
        """
        self.url = url
        self.token = token
        self.refresh_token = refresh_token
        if token is None and refresh_token is None:
            self.load_token()

    def get_token(self, username, passwd):
        """
        use to get a token and refresh token. token has key 'access', refresh token has key 'refresh'
        'access_token_lifetime': '01:00:00', 'refresh_token_lifetime': '1 00:00:00'
        :param username:
        :param passwd:
        :return:
        """
        body = {'username': username, 'password': passwd}
        r = self._send_post_request('token/', body, skip_auth=True)
        self.token = r['access']
        self.refresh_token = r['refresh']
        self.store_token()
        return r

    def renew_token(self, refresh_token):
        body = {'refresh': refresh_token}
        r = self._send_post_request('token/refresh/', body, skip_auth=True)
        self.token = r['access']
        self.refresh_token = r['refresh']
        self.store_token()
        return r

    def store_token(self):
        with open('vast_tokens.json', 'w') as f:
            json.dump({'access': self.token, 'refresh': self.refresh_token}, f)

    def load_token(self):
        if Path('vast_tokens.json').exists():
            with open('vast_tokens.json') as f:
                tokens = json.load(f)
                self.token = tokens['access']
                self.refresh_token = tokens['refresh']

    def get_quotas(self):
        return self._send_get_request('quotas/')

    def get_views(self, path: str = None):
        if path is not None:
            return self._send_get_request('views/', params={'path': path})
        return self._send_get_request('views/')

    def get_status(self):
        return self._send_get_request('latest/dashboard/status/')

    def create_view(self, path: str, policy_id: int, alias: str = None, protocols: set[str] = {'NFS'}):
        if not VASTClient.is_valid_unix_path(path):
            raise TypeError(f'the path provided [{path}] is not a valid unix path')
        alias = '' if alias is None else alias  # set default
        body = {
            'path': path,
            'alias': alias,
            'policy_id': policy_id,
            'protocols': protocols,
            'create_dir': True
        }
        return self._send_post_request('views/', body)

    def create_quota(self, name, path, soft_limit, hard_limit=None):
        hard_limit = soft_limit if hard_limit is None else hard_limit  # set default
        body = {
            'name': name,
            'path': path,
            'hard_limit': hard_limit,
            'soft_limit': soft_limit,
            'create_dir': False
        }
        return self._send_post_request('quotas/', body)

    def is_base10(self):
        r = self.get_status()
        return r['vms'][0]['capacity_base_10']

    def get_total_capacity(self):
        """
        :return:
        """
        r = self.get_status()
        return r['vms'][0]

    def _get_headers(self, additional_headers=None, skip_auth=False):
        headers = {
            'Accept': 'application/json'
        }
        if not skip_auth:
            headers['Authorization'] = f'Bearer {self.token}'
        if additional_headers is not None:
            headers.update(additional_headers)
        return headers

    def _send_get_request(self, endpoint, params=None, retries=2):
        if self.token is None and self.refresh_token is not None:
            self.renew_token(self.refresh_token)

        r = requests.get(os.path.join(self.url, endpoint),
                         params=params if not None else {},
                         headers=self._get_headers(),
                         verify=False)
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if self.refresh_token is not None and retries > 0 and e.response.status_code == 403:
                self.renew_token(self.refresh_token)
                self._send_get_request(endpoint, params, retries=(retries - 1))
            else:
                raise e
        return r.json()

    def _send_post_request(self, endpoint, payload, headers=None, skip_auth=False):
        if not skip_auth and self.token is None and self.refresh_token is not None:
            self.renew_token(self.refresh_token)

        headers = {'Content-Type': 'application/json'}.update(headers if headers is not None else {})

        r = requests.post(os.path.join(self.url, endpoint),
                          json=payload,
                          headers=self._get_headers(headers, skip_auth=skip_auth),
                          verify=False)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def is_valid_unix_path(path):
        # Regular expression pattern for Unix paths
        pattern = r'^/([A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+$'

        # Use re.match to check if the path matches the pattern
        if re.match(pattern, path):
            return True
        else:
            return False
