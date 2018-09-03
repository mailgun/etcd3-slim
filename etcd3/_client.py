from __future__ import absolute_import

import logging
from random import shuffle
from threading import Lock

import grpc
import six

from etcd3 import _utils
from etcd3._grpc_stubs.rpc_pb2 import (AuthStub, AuthenticateRequest,
                                       ClusterStub, DeleteRangeRequest, KVStub,
                                       LeaseGrantRequest, LeaseRevokeRequest,
                                       LeaseStub, MemberListRequest, PutRequest,
                                       RangeRequest, WatchStub)
from etcd3._keep_aliver import KeepAliver
from etcd3._watcher import Watcher

_DEFAULT_ETCD_ENDPOINT = '127.0.0.1:2379'
# It was observed in production that during failovers timeouts are getting
# really high. Thundering herd of reconnections is probably at play here.
_DEFAULT_REQUEST_TIMEOUT = 30  # Seconds

_log = logging.getLogger(__name__)


def _reconnect(f):

    def wrapper(*args, **kwargs):
        etcd3_clt = args[0]
        assert isinstance(etcd3_clt, Client)
        etcd3_clt._ensure_grpc_channel()
        try:
            try:
                return f(*args, **kwargs)

            except grpc.RpcError as err:
                severity = logging.ERROR
                if (err.code() == grpc.StatusCode.UNAUTHENTICATED and
                        err.details().endswith('invalid auth token')):
                    severity = logging.WARN

                _log.log(severity, 'Retrying error: %s(*%s, **%s)',
                         f, args, kwargs, exc_info=True)
                etcd3_clt._reset_grpc_channel()
                return f(*args, **kwargs)

        except Exception:
            etcd3_clt._close_grpc_channel()
            raise

    return wrapper


class Client(object):

    def __init__(self, endpoints=None, user=None, password=None, timeout=None,
                 cert=None, cert_key=None, cert_ca=None):
        endpoints = endpoints or [_DEFAULT_ETCD_ENDPOINT]
        self._endpoint_balancer = _EndpointBalancer(endpoints)
        self._auth_rq = _new_auth_rq(user, password)

        self._ssl_creds = grpc.ssl_channel_credentials(
            _read_file(cert_ca), _read_file(cert_key), _read_file(cert))
        self._timeout = timeout or _DEFAULT_REQUEST_TIMEOUT

        self._grpc_channel_mu = Lock()
        self._grpc_channel = None
        self._kv_stub = None
        self._watch_stub = None
        self._lease_stub = None

        # For tests only!
        self._skip_endpoint_discovery = False

    @property
    def current_endpoint(self):
        return self._endpoint_balancer.current_endpoint

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

    @_reconnect
    def _get_watch_stub(self):
        return self._watch_stub

    @_reconnect
    def _get_lease_stub(self):
        return self._lease_stub

    def _ensure_grpc_channel(self):
        with self._grpc_channel_mu:
            if self._grpc_channel:
                return

            self._ensure_grpc_channel_unsafe()

    def _close_grpc_channel(self):
        with self._grpc_channel_mu:
            self._close_grpc_channel_unsafe()

    def _reset_grpc_channel(self):
        with self._grpc_channel_mu:
            self._close_grpc_channel_unsafe()
            self._ensure_grpc_channel_unsafe()

    def _ensure_grpc_channel_unsafe(self):
        endpoint = self._endpoint_balancer.rotate_endpoint()
        self._grpc_channel = self._dial(endpoint)

        if not self._skip_endpoint_discovery:
            cluster_stub = ClusterStub(self._grpc_channel)
            rs = cluster_stub.MemberList(MemberListRequest(),
                                         timeout=self._timeout)
            self._endpoint_balancer.refresh(rs.members, endpoint)

        self._kv_stub = KVStub(self._grpc_channel)
        self._watch_stub = WatchStub(self._grpc_channel)
        self._lease_stub = LeaseStub(self._grpc_channel)

    def _close_grpc_channel_unsafe(self):
        if not self._grpc_channel:
            return
        try:
            self._grpc_channel.close()
        except Exception:
            _log.exception('Failed to close Etcd client gRPC channel')

        self._grpc_channel = None

    def _dial(self, endpoint):
        if self._auth_rq:
            token = self._authenticate(endpoint)
            token_plugin = _TokenAuthMetadataPlugin(token)
            token_creds = grpc.metadata_call_credentials(token_plugin)
            creds = grpc.composite_channel_credentials(self._ssl_creds,
                                                       token_creds)
        else:
            creds = self._ssl_creds

        return grpc.secure_channel(endpoint, creds)

    def _authenticate(self, endpoint):
        grpc_channel = grpc.secure_channel(endpoint, self._ssl_creds)
        try:
            auth_stub = AuthStub(grpc_channel)
            rs = auth_stub.Authenticate(self._auth_rq, timeout=self._timeout)
            return rs.token

        finally:
            grpc_channel.close()


class _TokenAuthMetadataPlugin(grpc.AuthMetadataPlugin):

    def __init__(self, token):
        self._token = token

    def __call__(self, context, callback):
        metadata = (('token', self._token),)
        callback(metadata, None)


def _new_auth_rq(user, password):
    if bool(user) != bool(password):
        raise AttributeError('Neither or both user and password '
                             'should be specified')
    if not user:
        return None

    return AuthenticateRequest(name=user, password=password)


def _read_file(filename):
    if not filename:
        return None

    with open(filename, 'rb') as f:
        return f.read()


class _EndpointBalancer(object):

    def __init__(self, endpoints):
        self._mu = Lock()

        if isinstance(endpoints, six.string_types):
            endpoints = endpoints.split(',')

        self._endpoints = [_normalize_endpoint(ep) for ep in endpoints]
        shuffle(self._endpoints)

    @property
    def current_endpoint(self):
        return self._endpoints[0]

    def rotate_endpoint(self):
        with self._mu:
            rotated_endpoint = self._endpoints[0]
            self._endpoints = self._endpoints[1:]
            self._endpoints.append(rotated_endpoint)
            return self._endpoints[0]

    def refresh(self, members, current_endpoint):
        with self._mu:
            self._endpoints = []
            for member in members:
                if len(member.clientURLs) < 1:
                    continue

                endpoint = _normalize_endpoint(member.clientURLs[0])
                if endpoint == current_endpoint:
                    continue

                self._endpoints.append(endpoint)

            shuffle(self._endpoints)
            # Ensure that current endpoint is the first in the list
            self._endpoints.insert(0, current_endpoint)
            _log.info('Endpoints refreshed: %s', self._endpoints)


def _normalize_endpoint(ep):
    parts = ep.lower().strip().split('//')
    return parts[-1]
