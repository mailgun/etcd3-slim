from __future__ import absolute_import

import os

from etcd3._client import Client

ENV_ETCD3_CA = 'ETCD3_CA'
ENV_ETCD3_ENDPOINT = 'ETCD3_ENDPOINT'
ENV_ETCD3_PASSWORD = 'ETCD3_PASSWORD'
ENV_ETCD3_USER = 'ETCD3_USER'
ENV_ETCD3_TLS_CERT = 'ETCD3_TLS_CERT'
ENV_ETCD3_TLS_KEY = 'ETCD3_TLS_KEY'


__all__ = [
    'Client'
]

_clt = None


def client():
    global _clt
    if _clt:
        return _clt

    endpoint = os.getenv(ENV_ETCD3_ENDPOINT, '127.0.0.1:2379')
    # Remove the schema if it exists (http://|https://)
    if '//' in endpoint:
        endpoint = endpoint.split('//')[1]

    user = os.getenv(ENV_ETCD3_USER)
    password = os.getenv(ENV_ETCD3_PASSWORD)
    cert_ca = os.getenv(ENV_ETCD3_CA)
    cert = os.getenv(ENV_ETCD3_TLS_CERT)
    cert_key = os.getenv(ENV_ETCD3_TLS_KEY)
    _clt = Client(endpoint, user, password, cert, cert_key, cert_ca)
    return _clt
