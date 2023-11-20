import requests
import os
import re
from .models import *
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

    def get_quotas(self):
        return self._send_get_request('quotas/')

    def get_views(self, path: Path = None):
        if path is not None:
            return self._send_get_request('views/', params={'path': path.as_posix()})
        return self._send_get_request('views/')

    def get_status(self):
        return self._send_get_request('latest/dashboard/status/')

    def create_view(self, view_model: ViewCreate):
        return self._send_post_request('views/', view_model.model_dump())

    def create_quota(self, quota_model: QuotaCreate):
        return self._send_post_request('quotas/', quota_model.model_dump())

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


