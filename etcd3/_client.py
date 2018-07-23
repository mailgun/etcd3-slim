from __future__ import absolute_import

import logging
from threading import Lock

import grpc

from etcd3 import _utils
from etcd3._grpc_stubs.rpc_pb2 import (AuthStub, AuthenticateRequest,
                                       DeleteRangeRequest, KVStub,
                                       LeaseGrantRequest,
                                       LeaseRevokeRequest, LeaseStub,
                                       PutRequest, RangeRequest,
                                       WatchStub)
from etcd3._keep_aliver import KeepAliver
from etcd3._watcher import Watcher

_DEFAULT_ETCD_ENDPOINT = '127.0.0.1:23790'
_DEFAULT_REQUEST_TIMEOUT = 1  # Seconds


_log = logging.getLogger(__name__)


def _reconnect(f):

    def wrapper(*args, **kwargs):
        etcd3_clt = args[0]
        assert isinstance(etcd3_clt, Client)
        etcd3_clt._ensure_grpc_channel()
        try:
            return f(*args, **kwargs)

        except Exception:
            etcd3_clt._close_grpc_channel()
            raise

    return wrapper


class Client(object):

    def __init__(self, endpoint, user, password, cert=None, cert_key=None,
                 cert_ca=None, timeout=None):
        self._endpoint = endpoint
        self._user = user
        self._password = password
        self._ssl_creds = grpc.ssl_channel_credentials(
            _read_file(cert_ca), _read_file(cert_key), _read_file(cert))
        self._timeout = timeout or _DEFAULT_REQUEST_TIMEOUT

        self._grpc_channel_mu = Lock()
        self._grpc_channel = None
        self._kv_stub = None
        self._watch_stub = None
        self._lease_stub = None

    @_reconnect
    def get(self, key, is_prefix=False):
        rq = RangeRequest(key=_utils.to_bytes(key))
        if is_prefix:
            rq.range_end = _utils.range_end(rq.key)

        return self._kv_stub.Range(rq, timeout=self._timeout)

    def get_value(self, key):
        """
        Convenience wrapper around `get`. It returns value only, or None if the
        key does not exist.
        """
        rs = self.get(key)
        if rs.count == 0:
            return None

        return rs.kvs[0].value

    @_reconnect
    def put(self, key, val, lease_id=None):
        rq = PutRequest(key=_utils.to_bytes(key),
                        value=_utils.to_bytes(val),
                        lease=lease_id)
        return self._kv_stub.Put(rq, timeout=self._timeout)

    @_reconnect
    def delete(self, key, is_prefix=False):
        rq = DeleteRangeRequest(key=_utils.to_bytes(key))
        if is_prefix:
            rq.range_end = _utils.range_end(rq.key)

        return self._kv_stub.DeleteRange(rq, timeout=self._timeout)

    @_reconnect
    def lease_grant(self, ttl):
        rq = LeaseGrantRequest(TTL=ttl)
        return self._lease_stub.LeaseGrant(rq, timeout=self._timeout)

    @_reconnect
    def lease_revoke(self, lease_id):
        rq = LeaseRevokeRequest(ID=lease_id)
        return self._lease_stub.LeaseRevoke(rq, timeout=self._timeout)

    def new_watcher(self, key, event_handler, is_prefix=False,
                    start_revision=0, spin_pause=None):
        return Watcher(
            self, key, event_handler, is_prefix, start_revision, spin_pause)

    def new_keep_aliver(self, key, value, ttl, spin_pause=None):
        return KeepAliver(self, key, value, ttl, spin_pause)

    def _ensure_grpc_channel(self):
        with self._grpc_channel_mu:
            if self._grpc_channel:
                return

            self._grpc_channel = self._dial()
            self._kv_stub = KVStub(self._grpc_channel)
            self._watch_stub = WatchStub(self._grpc_channel)
            self._lease_stub = LeaseStub(self._grpc_channel)

    def _close_grpc_channel(self):
        with self._grpc_channel_mu:
            if not self._grpc_channel:
                return
            try:
                self._grpc_channel.close()
            except Exception:
                _log.exception('Failed to close Etcd client gRPC channel')

            self._grpc_channel = None

    def _dial(self):
        token = self._authenticate()
        token_plugin = _TokenAuthMetadataPlugin(token)
        token_creds = grpc.metadata_call_credentials(token_plugin)
        creds = grpc.composite_channel_credentials(self._ssl_creds,
                                                   token_creds)
        return grpc.secure_channel(self._endpoint, creds)

    def _authenticate(self):
        grpc_channel = grpc.secure_channel(self._endpoint, self._ssl_creds)
        try:
            auth_stub = AuthStub(grpc_channel)
            rq = AuthenticateRequest(
                name=self._user,
                password=self._password
            )
            rs = auth_stub.Authenticate(rq, timeout=self._timeout)
            return rs.token

        finally:
            grpc_channel.close()


class _TokenAuthMetadataPlugin(grpc.AuthMetadataPlugin):

    def __init__(self, token):
        self._token = token

    def __call__(self, context, callback):
        metadata = (('token', self._token),)
        callback(metadata, None)


def _read_file(filename):
    if not filename:
        return None

    with open(filename, 'rb') as f:
        return f.read()
