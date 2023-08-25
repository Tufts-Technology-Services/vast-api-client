import requests
import os
import json
from pathlib import Path


class VASTClient:
    """
    used to get information from the VAST hpc storage unit
    """

    def __init__(self, token=None, refresh_token=None, url='https://hpcvast-vms.mgmt.pax.tufts.edu/api'):
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

    def get_views(self):
        return self._send_get_request('views/')

    def get_status(self):
        return self._send_get_request('latest/dashboard/status/')

    def is_base10(self):
        r = self.get_status()
        return r['vms'][0]['capacity_base_10']

    def get_total_capacity(self):
        """
        'capacity_base_10': True,
        'total_usage_capacity_percentage': 50.243462836192265, 'total_active_capacity': 3375.0,
        'total_remaining_capacity': 1679.283129278511
        'physical_space': 2481333861099766, 'physical_space_in_use': 1760415986330966,
        'logical_space': 3831255246307328,
        'logical_space_in_use': 2609877566328160, 'drr': 1.606232902433872, 'drr_text': '1.6:1',
        'physical_space_tb': 2481.334, 'physical_space_in_use_tb': 1760.416, 'logical_space_tb': 3831.255,
        'free_physical_space': 720917874768800, 'free_physical_space_tb': 720.918,
        'free_logical_space': 1107539015342268, 'free_logical_space_tb': 1107.539,
        'estore_capacity_in_use_bytes': 2723716230965060, 'estore_capacity_in_use_tb': 2723.716,
        'logical_space_in_use_tb': 2609.878,
        'physical_space_in_use_percent': 70.95,
        'logical_space_in_use_percent': 68.12, 'logical_drr_percent': 60.62,
        'physical_space_wo_overhead': 2697102022934528,
        'physical_space_in_use_wo_overhead': 1695716870721489,
        'free_physical_space_wo_overhead': 1001385152213039, 'free_physical_space_wo_overhead_tb': 1001.385,
        'usable_auxiliary_space_in_use': 70873074797810.47, 'usable_capacity_bytes': 2385245584431655,
        'free_usable_capacity': 689528713710166, 'free_usable_capacity_tb': 689.529, 'usable_capacity_tb': 2385.246,
        'auxiliary_space_in_use': 73577196776612.06, 'auxiliary_space_in_use_tb': 73.577,
        'logical_auxiliary_space_in_use': 113838664636900, 'logical_auxiliary_space_in_use_tb': 113.839

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



