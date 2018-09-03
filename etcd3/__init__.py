from __future__ import absolute_import

import os

from etcd3._client import Client

ENV_ETCD3_CA = 'ETCD3_CA'
ENV_ETCD3_ENDPOINT = 'ETCD3_ENDPOINT'
ENV_ETCD3_PASSWORD = 'ETCD3_PASSWORD'
ENV_ETCD3_USER = 'ETCD3_USER'
ENV_ETCD3_TLS = 'ETCD3_TLS'
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

    _clt = Client(endpoints=os.getenv(ENV_ETCD3_ENDPOINT),
                  user=os.getenv(ENV_ETCD3_USER),
                  password=os.getenv(ENV_ETCD3_PASSWORD),
                  cert=os.getenv(ENV_ETCD3_TLS_CERT),
                  cert_key=os.getenv(ENV_ETCD3_TLS_KEY),
                  cert_ca=os.getenv(ENV_ETCD3_CA),
                  with_tls=bool(os.getenv(ENV_ETCD3_TLS)))
    return _clt
