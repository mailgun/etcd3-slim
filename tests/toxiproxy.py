import platform

import os
from time import sleep

import requests
from requests import ConnectionError
from six.moves import http_client
import subprocess

_DEFAULT_VERSION = 'v2.1.3'
_API_PORT = 8474


class ToxiProxyClient(object):

    def __init__(self, version=None):
        self._version = version or _DEFAULT_VERSION
        self._filename = '/tmp/toxiproxy_' + self._version
        if not os.path.isfile(self._filename):
            self._download_binary()
        self._proc = None
        self._api_base_url = 'http://127.0.0.1:%d' % (_API_PORT,)

    def start(self):
        self._proc = subprocess.Popen([self._filename])

        # Wait for the toxiproxy process to get in service.
        url = self._api_base_url + '/proxies'
        for i in range(10):
            try:
                rs = requests.get(url, timeout=1)
                if rs.status_code == http_client.OK:
                    return

            except ConnectionError:
                pass

            sleep(0.2)

        raise RuntimeError('Failed to start toxiproxy')

    def stop(self):
        if not self._proc:
            return
        try:
            self._proc.terminate()
        finally:
            self._proc = None

    def reset(self):
        rs = requests.post(self._api_base_url + '/reset')
        if rs.status_code != http_client.NO_CONTENT:
            raise RuntimeError('Failed to reset proxy: %d %s'
                               % (rs.status_code, rs.text))

    def add_proxy(self, proxy_name, upstream, listen=None, enabled=True):
        json_rq = {
            'upstream': upstream,
            'name': proxy_name,
            'enabled': enabled,
        }
        if listen is not None:
            json_rq['listen'] = listen

        rs = requests.post(self._api_base_url + '/proxies', json=json_rq)
        if rs.status_code != http_client.CREATED:
            raise RuntimeError('Failed to add proxy: %d %s'
                               % (rs.status_code, rs.text))
        return rs.json()

    def update_proxy(self, proxy_name, enabled=True):
        url = self._api_base_url + '/proxies/' + proxy_name
        rs = requests.post(url, json={'enabled': enabled})
        if rs.status_code != http_client.OK:
            raise RuntimeError('Failed to update proxy: %d %s'
                               % (rs.status_code, rs.text))

    def _download_binary(self):
        system = platform.system().lower()
        url = ('https://github.com/Shopify/toxiproxy/releases/download/%s'
               '/toxiproxy-server-%s-amd64' % (self._version, system))
        rs = requests.get(url)
        if rs.status_code != http_client.OK:
            raise RuntimeError('Failed to download toxiproxy: url=%s, '
                               '%d %s' % (url, rs.status_code, rs.text))

        with open(self._filename, 'wb') as f:
            f.write(rs.content)

        os.chmod(self._filename, 0o755)
