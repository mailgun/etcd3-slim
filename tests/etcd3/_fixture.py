from __future__ import absolute_import

import os

from etcd3 import Client, ENV_ETCD3_ENDPOINT, ENV_ETCD3_TLS, ENV_ETCD3_USER
from tests.toxiproxy import ToxiProxyClient


def setup():
    global _toxi_proxy_clt
    _toxi_proxy_clt = ToxiProxyClient()
    _toxi_proxy_clt.start()

    seed_endpoint = os.getenv(ENV_ETCD3_ENDPOINT)
    user = os.getenv(ENV_ETCD3_USER)
    password = user  # That is how the test etcd cluster is configured.

    cert_ca = None
    if os.getenv(ENV_ETCD3_TLS):
        cert_ca = 'tests/fixtures/ca.pem'

    global _direct_clt
    _direct_clt = Client(seed_endpoint, user, password, cert_ca=cert_ca)
    global _aux_clt
    _aux_clt = Client(seed_endpoint, user, password, cert_ca=cert_ca)

    _aux_clt._reset_grpc_channel()
    discovered_endpoints = _aux_clt._endpoint_balancer._endpoints

    global _proxy_endpoint_index
    _proxy_endpoint_index = {}
    proxy_endpoints = []
    for i, discovered_endpoint in enumerate(discovered_endpoints):
        proxy_name = str(i)
        proxy_spec = _toxi_proxy_clt.add_proxy(proxy_name, discovered_endpoint)
        proxy_endpoint = proxy_spec['listen']
        # Make sure to access proxy via 127.0.0.1 otherwise TLS verification fails.
        if proxy_endpoint.startswith('[::]:'):
            proxy_endpoint = '127.0.0.1:' + proxy_endpoint[5:]

        proxy_endpoints.append(proxy_endpoint)
        _proxy_endpoint_index[proxy_endpoint] = proxy_name

    # Proxied client is assumed to have not established a connection with Etcd.
    global _proxied_clt
    _proxied_clt = Client(proxy_endpoints, user, password, cert_ca=cert_ca)
    _proxied_clt._skip_endpoint_discovery = True

    # Clean leftovers from previous tests.
    _aux_clt.delete('/test', is_prefix=True)


def teardown():
    _toxi_proxy_clt.stop()


def proxied_clt():
    """
    Returns a client that connects to Etcd cluster though a toxi proxy that
    can be controlled via enable/disable_endpoint(s) function family.
    """
    assert isinstance(_proxied_clt, Client)
    return _proxied_clt


def direct_clt():
    """
    Returns a client that connects to Etcd cluster directly.
    """
    assert isinstance(_direct_clt, Client)
    return _direct_clt


def aux_clt():
    """
    Returns a client that connects to Etcd cluster directly.
    """
    assert isinstance(_aux_clt, Client)
    return _aux_clt


def disable_endpoint(endpoint=None):
    endpoint = endpoint or _proxied_clt.current_endpoint
    _update_endpoints([endpoint], enabled=False)


def enable_endpoint(endpoint=None):
    endpoint = endpoint or _proxied_clt.current_endpoint
    _update_endpoints([endpoint], enabled=True)


def disable_all_endpoints():
    _update_endpoints(endpoints=None, enabled=False)


def enabled_all_endpoints():
    _update_endpoints(endpoints=None, enabled=True)


def _update_endpoints(endpoints, enabled):
    for endpoint in endpoints or _proxy_endpoint_index.keys():
        proxy_name = _proxy_endpoint_index[endpoint]
        _toxi_proxy_clt.update_proxy(proxy_name, enabled)


