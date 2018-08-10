from __future__ import absolute_import

import logging
from threading import Thread
from time import sleep, time

from etcd3._grpc_bd_stream import GrpcBDStream
from etcd3._grpc_stubs.rpc_pb2 import LeaseKeepAliveRequest, LeaseStub

_DEFAULT_SPIN_PAUSE = 3  # seconds

_log = logging.getLogger(__name__)


class KeepAliver(object):

    def __init__(self, client, key, value, ttl, spin_pause=None):
        self._client = client
        self._key = key
        self._value = value
        self._ttl = ttl
        self._spin_pause = spin_pause or _DEFAULT_SPIN_PAUSE

        self._name = 'keep_aliver_' + key
        self._stop = False
        self._thread = Thread(name=self._name, target=self._run)
        self._thread.daemon = True
        
    def start(self):
        self._thread.start()

    def stop(self, timeout=None):
        self._stop = True
        self._thread.join(timeout)
        return not self._thread.is_alive()

    def _run(self):
        _log.info('%s started', self._name)
        lease_id = None
        while not self._stop:
            try:
                lease_grant_rs = self._client.lease_grant(self._ttl)
                lease_id = lease_grant_rs.ID
                refresh_interval = lease_grant_rs.TTL * 2 / 3.
                lease_keep_alive_rq = LeaseKeepAliveRequest(ID=lease_id)

                self._client.put(self._key, self._value, lease_id)
                _log.debug('Volatile key stored: %s, value=%s, ttl=%d, ',
                           self._key, self._value, lease_grant_rs.TTL)

                lease_stub = LeaseStub(self._client._grpc_channel)
                grpc_stream = GrpcBDStream(self._name + '_stream',
                                           lease_stub.LeaseKeepAlive)
            except Exception:
                _log.exception('Failed to register: %s', self._key)
                sleep(self._spin_pause)
                continue

            effective_spin_pause = min(refresh_interval, self._spin_pause)
            try:
                refresh_at = time() + refresh_interval
                while not self._stop:
                    sleep(effective_spin_pause)
                    now = time()
                    if now < refresh_at:
                        continue

                    refresh_at = now + refresh_interval
                    grpc_stream.send(lease_keep_alive_rq, self._client._timeout)
                    try:
                        rs = grpc_stream.recv(self._client._timeout)
                        _log.debug('Lease refreshed: %s, rs=%s', lease_id, rs)

                    except Exception:
                        _log.exception('Failed to refresh lease: %s', lease_id)
                        break
            finally:
                grpc_stream.close(self._client._timeout)
    
        if lease_id:
            try:
                self._client.lease_revoke(lease_id)
                self._client.delete(self._key)
    
            except Exception:
                _log.exception('Failed to revoke lease: %s', lease_id)

        _log.info('%s stopped', self._name)
