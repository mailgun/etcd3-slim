from __future__ import absolute_import

import logging
import threading

from six.moves import queue

_log = logging.getLogger(__name__)


class GrpcBDStream(object):

    def __init__(self, name, make_stream, buf_size=2):
        self._name = name
        self._requests = queue.Queue(maxsize=buf_size)
        self._responses = queue.Queue(maxsize=buf_size)
        self._response_iter = make_stream(self._request_iter())
        self._thread = threading.Thread(name=name, target=self._run)
        self._thread.daemon = True
        self._thread.start()
        self._closed_mu = threading.Lock()
        self._closed = False

    def send(self, rq, timeout=None):
        try:
            self._requests.put(rq, timeout=timeout)

        except queue.Full:
            raise RuntimeError('%s request submit timeout' % (self._name,))

    def recv(self, timeout=None):
        try:
            rs = self._responses.get(timeout=timeout)
            if rs is None:
                with self._closed_mu:
                    self._closed = True

                raise RuntimeError('%s closed by server' % (self._name,))

            if isinstance(rs, Exception):
                with self._closed_mu:
                    self._closed = True

                raise rs

            return rs

        except queue.Empty:
            return None

    def close(self, timeout):
        with self._closed_mu:
            if self._closed:
                return
        try:
            self._requests.put(None, timeout=timeout)
        except queue.Full:
            _log.error('[%s] timed out on close request', self._name)
            return

        try:
            # Drain unhandled responses
            while True:
                rs = self._responses.get(timeout=timeout)
                if rs is None:
                    break

                if isinstance(rs, Exception):
                    _log.info('[%s] error discarded: %s', self._name, rs)
                    break

                _log.info('[%s] response discarded: %s', self._name, rs)

        except queue.Empty:
            _log.error('[%s] timed out draining responses', self._name)

        with self._closed_mu:
            self._closed = True

    def _request_iter(self):
        while True:
            rq = self._requests.get()
            if rq is None:
                return

            yield rq

    def _run(self):
        _log.info('[%s] thread started', self._name)
        try:
            for rs in self._response_iter:
                self._responses.put(rs)

            self._responses.put(None)

        except Exception as err:
            self._responses.put(err)

        _log.info('[%s] thread stopped', self._name)
