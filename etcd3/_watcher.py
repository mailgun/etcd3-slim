from __future__ import absolute_import

import logging
from threading import Thread

from etcd3 import _utils
from etcd3._grpc_bd_stream import GrpcBDStream
from etcd3._grpc_stubs.rpc_pb2 import (WatchCreateRequest, WatchRequest,
                                       WatchStub)

_DEFAULT_SPIN_PAUSE = 3  # seconds

_log = logging.getLogger(__name__)


class Watcher(object):

    def __init__(self, client, key, event_handler, is_prefix=False,
                 start_revision=0, spin_pause=None):
        self._client = client
        self._key = key
        self._start_revision = start_revision
        self._event_handler = event_handler
        self._spin_pause = spin_pause or _DEFAULT_SPIN_PAUSE

        watch_create_rq = WatchCreateRequest(
            key=_utils.to_bytes(key),
            start_revision=start_revision)

        if is_prefix:
            watch_create_rq.range_end = _utils.range_end(watch_create_rq.key)

        self._watch_rq = WatchRequest(create_request=watch_create_rq)

        self._name = 'watcher_' + key
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
        start_revision = None
        while not self._stop:
            watch_stub = WatchStub(self._client._grpc_channel)
            grpc_stream = GrpcBDStream(self._name + '_stream', watch_stub.Watch)
            if start_revision:
                self._watch_rq.create_request.start_revision = start_revision

            grpc_stream.send(self._watch_rq, self._client._timeout)
            try:
                while not self._stop:
                    rs = grpc_stream.recv(self._spin_pause)
                    if not rs:
                        continue

                    for e in rs.events:
                        start_revision = e.kv.mod_revision + 1
                        try:
                            self._event_handler(e)

                        except Exception:
                            _log.exception('Event handler failed: %s', e)

            except Exception:
                _log.exception('Watch stream failed: %s', self._key)

            finally:
                grpc_stream.close(self._client._timeout)

        _log.info('%s stopped', self._name)
