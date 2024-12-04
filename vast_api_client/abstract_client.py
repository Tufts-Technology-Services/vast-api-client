from pathlib import Path
import requests


class AbstractClient:
    url = None
    refresh_token = None
    token = None

    def renew_token(self, refresh_token):
        raise NotImplementedError

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

        r = requests.get(Path(self.url, endpoint).as_posix(),
                         params=params if not None else {},
                         headers=self._get_headers(),
                         verify=False,
                         timeout=10)
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

        headers = headers if headers is not None else {}
        headers.update({'Content-Type': 'application/json'})

        r = requests.post(Path(self.url, endpoint).as_posix(),
                          json=payload,
                          headers=self._get_headers(headers, skip_auth=skip_auth),
                          verify=False,
                          timeout=20)
        r.raise_for_status()
        return r.json()

    def _send_delete_request(self, endpoint):
        if self.token is None and self.refresh_token is not None:
            self.renew_token(self.refresh_token)

        r = requests.delete(Path(self.url, endpoint).as_posix(),
                            headers=self._get_headers(),
                            verify=False,
                            timeout=10)

        r.raise_for_status()
        return {'status': r.status_code}