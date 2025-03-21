from urllib.parse import urljoin
import os
import requests

VERIFY_CERTS = os.getenv('VERIFY_CERTS', 'True').lower() in ['true', '1', 'yes']

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

        r = requests.get(urljoin(self.url, endpoint),
                         params=params if not None else {},
                         headers=self._get_headers(),
                         verify=VERIFY_CERTS,
                         timeout=30)
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
        return self._send_body('POST', endpoint, payload, headers, skip_auth)
    
    def _send_patch_request(self, endpoint, payload, headers=None, skip_auth=False):
        return self._send_body('PATCH', endpoint, payload, headers, skip_auth)
    
    def _send_body(self, http_method, endpoint, payload, headers=None, skip_auth=False):
        if not skip_auth and self.token is None and self.refresh_token is not None:
            self.renew_token(self.refresh_token)

        # list of valid http methods
        if http_method not in ['POST', 'PUT', 'PATCH', 'DELETE']:
            raise ValueError(f'Invalid http method: {http_method}')
        
        headers = headers if headers is not None else {}
        headers.update({'Content-Type': 'application/json'})

        r = requests.request(http_method, urljoin(self.url, endpoint),
                          json=payload,
                          headers=self._get_headers(headers, skip_auth=skip_auth),
                          verify=VERIFY_CERTS,
                          timeout=20)
        r.raise_for_status()
        return r.json()

    def _send_delete_request(self, endpoint, body=None):
        if body is not None:
            return self._send_body('DELETE', endpoint, body)
        if self.token is None and self.refresh_token is not None:
            self.renew_token(self.refresh_token)

        r = requests.delete(urljoin(self.url, endpoint),
                            headers=self._get_headers(),
                            verify=VERIFY_CERTS,
                            timeout=10)

        r.raise_for_status()
        return {'status': r.status_code}