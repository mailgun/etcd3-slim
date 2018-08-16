from __future__ import absolute_import

from time import sleep

import os
from nose.tools import eq_
from six.moves import queue

from etcd3 import (ENV_ETCD3_CA, ENV_ETCD3_ENDPOINT, ENV_ETCD3_PASSWORD,
                   ENV_ETCD3_USER, _utils)
from etcd3._client import Client
from etcd3._grpc_stubs.kv_pb2 import Event
from tests.toxiproxy import ToxiProxyClient

_ETCD_PROXY = 'etcd'


def setup_module():
    global _toxi_proxy_clt
    _toxi_proxy_clt = ToxiProxyClient()
    _toxi_proxy_clt.start()

    etcd_endpoint = os.getenv(ENV_ETCD3_ENDPOINT, '127.0.0.1:2379')
    user = os.getenv(ENV_ETCD3_USER, 'test')
    password = os.getenv(ENV_ETCD3_PASSWORD, 'test')
    cert_ca = os.getenv(ENV_ETCD3_CA, 'tests/fixtures/ca.pem')

    global _direct_clt
    _direct_clt = Client(etcd_endpoint, user, password, cert_ca=cert_ca)

    proxy_spec = _toxi_proxy_clt.add_proxy(_ETCD_PROXY, etcd_endpoint)
    proxy_endpoint = proxy_spec['listen']
    # Make sure to access proxy via 127.0.0.1 otherwise TLS verification fails.
    if proxy_endpoint.startswith('[::]:'):
        proxy_endpoint = '127.0.0.1:' + proxy_endpoint[5:]

    global _proxied_clt
    _proxied_clt = Client(proxy_endpoint, user, password, cert_ca=cert_ca)


def teardown_module():
    _toxi_proxy_clt.stop()


def test_watch_key():
    _proxied_clt.delete('/test')
    watch_queue = queue.Queue()

    _proxied_clt.put('/test/foo2', 'bar1')
    w = _proxied_clt.new_watcher('/test/foo2', spin_pause=0.2,
                                 event_handler=lambda e: watch_queue.put(e))
    w.start()
    try:
        sleep(0.5)

        # When
        _proxied_clt.put('/test/foo1', 'bar2')
        _proxied_clt.put('/test/foo3', 'bar5')
        _proxied_clt.put('/test/foo2', 'bar6')
        _proxied_clt.put('/test/foo21', 'bar7')
        _proxied_clt.delete('/test/foo2')
        _proxied_clt.put('/test/foo212', 'bar8')
        _proxied_clt.put('/test/foo2', 'bar9')
        _proxied_clt.put('/test/foo21', 'bar10')

        # Then
        events = _collect_events(watch_queue, timeout=3)
        eq_(3, len(events))
        _assert_event(Event.PUT, '/test/foo2', 'bar6', events[0])
        _assert_event(Event.DELETE, '/test/foo2', '', events[1])
        _assert_event(Event.PUT, '/test/foo2', 'bar9', events[2])

    finally:
        w.stop(timeout=1)


def test_watch_key_prefix():
    _proxied_clt.delete('/test')
    watch_queue = queue.Queue()

    _proxied_clt.put('/test/foo2', 'bar1')
    w = _proxied_clt.new_watcher('/test/foo2', is_prefix=True, spin_pause=0.2,
                                 event_handler=lambda e: watch_queue.put(e))
    w.start()
    try:
        sleep(0.5)

        # When
        _proxied_clt.put('/test/foo1', 'bar6')
        _proxied_clt.put('/test/foo21', 'bar7')
        _proxied_clt.delete('/test/foo2')
        _proxied_clt.put('/test/foo212', 'bar8')
        _proxied_clt.delete('/test/foo1')
        _proxied_clt.put('/test/foo21', 'bar10')
        _proxied_clt.put('/test/foo3', 'bar11')

        # Then
        events = _collect_events(watch_queue, timeout=3)
        eq_(4, len(events))
        _assert_event(Event.PUT, '/test/foo21', 'bar7', events[0])
        _assert_event(Event.DELETE, '/test/foo2', '', events[1])
        _assert_event(Event.PUT, '/test/foo212', 'bar8', events[2])
        _assert_event(Event.PUT, '/test/foo21', 'bar10', events[3])

    finally:
        w.stop(timeout=1)


def test_watch_from_the_past():
    # A revision to start watching from can be specified.

    _proxied_clt.delete('/test')
    watch_queue = queue.Queue()

    _proxied_clt.put('/test/foo2', 'bar1')
    _proxied_clt.put('/test/foo2', 'bar2')
    put_rs = _proxied_clt.put('/test/foo2', 'bar3')
    _proxied_clt.put('/test/foo2', 'bar4')
    _proxied_clt.put('/test/foo2', 'bar5')
    _proxied_clt.put('/test/foo3', 'bar6')

    # When
    w = _proxied_clt.new_watcher('/test/foo2', spin_pause=0.2,
                                 start_revision=put_rs.header.revision,
                                 event_handler=lambda e: watch_queue.put(e))
    w.start()
    try:
        _proxied_clt.delete('/test/foo2')

        # Then
        events = _collect_events(watch_queue, timeout=3)
        eq_(4, len(events))
        _assert_event(Event.PUT, '/test/foo2', 'bar3', events[0])
        _assert_event(Event.PUT, '/test/foo2', 'bar4', events[1])
        _assert_event(Event.PUT, '/test/foo2', 'bar5', events[2])
        _assert_event(Event.DELETE, '/test/foo2', '', events[3])

    finally:
        w.stop(timeout=1)


def test_auto_reconnect():
    # A revision to start watching from can be specified.

    _proxied_clt.delete('/test')
    watch_queue = queue.Queue()

    put_rs = _direct_clt.put('/test/foo', 'bar0')
    w = _proxied_clt.new_watcher('/test/foo', spin_pause=0.2,
                                 start_revision=put_rs.header.revision + 1,
                                 event_handler=lambda e: watch_queue.put(e))
    w.start()
    try:
        _direct_clt.put('/test/foo', 'bar1')
        _direct_clt.put('/test/foo', 'bar2')
        sleep(0.5)

        # When connection with Etcd is lost...
        _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=False)

        # New events stop coming.
        _direct_clt.put('/test/foo', 'bar3')
        _direct_clt.delete('/test/foo')
        _direct_clt.put('/test/foo', 'bar4')
        events = _collect_events(watch_queue, timeout=3)
        eq_(2, len(events))
        _assert_event(Event.PUT, '/test/foo', 'bar1', events[0])
        _assert_event(Event.PUT, '/test/foo', 'bar2', events[1])

        # But as soon as connection with Etcd is restored...
        _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=True)

        # ...backed up events are received
        events = _collect_events(watch_queue, timeout=5)
        eq_(3, len(events))
        _assert_event(Event.PUT, '/test/foo', 'bar3', events[0])
        _assert_event(Event.DELETE, '/test/foo', '', events[1])
        _assert_event(Event.PUT, '/test/foo', 'bar4', events[2])

    finally:
        w.stop(timeout=1)


def test_etcd_down_on_start():
    # If connection with Etcd cannot be established when a watch is started,
    # then events are received as soon as the connection is up.

    _proxied_clt.delete('/test')
    watch_queue = queue.Queue()

    put_rs = _proxied_clt.put('/test/foo', 'bar1')
    _proxied_clt.delete('/test/foo')
    _proxied_clt.put('/test/foo', 'bar2')

    w = _proxied_clt.new_watcher('/test/foo', spin_pause=0.2,
                                 start_revision=put_rs.header.revision,
                                 event_handler=lambda e: watch_queue.put(e))

    # When connection with Etcd is lost...
    _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=False)

    w.start()
    try:
        events = _collect_events(watch_queue, timeout=3)
        eq_(0, len(events))

        # But as soon as connection with Etcd is restored...
        _toxi_proxy_clt.update_proxy(_ETCD_PROXY, enabled=True)

        # ...backed up events are received
        events = _collect_events(watch_queue, timeout=5)
        eq_(3, len(events))
        _assert_event(Event.PUT, '/test/foo', 'bar1', events[0])
        _assert_event(Event.DELETE, '/test/foo', '', events[1])
        _assert_event(Event.PUT, '/test/foo', 'bar2', events[2])

    finally:
        w.stop(timeout=1)


def test_handler_exception():
    # Event handler failures do not disrupt watch operation.

    _proxied_clt.delete('/test')
    watch_queue = queue.Queue()

    handle_count = [0]

    def event_handler(e):
        handle_count[0] += 1
        if handle_count[0] == 2:
            raise RuntimeError('Kaboom!')

        watch_queue.put(e)

    put_rs = _direct_clt.put('/test/foo', 'bar0')
    w = _proxied_clt.new_watcher('/test/foo', spin_pause=0.2,
                                 start_revision=put_rs.header.revision + 1,
                                 event_handler=event_handler)
    w.start()
    try:
        # When
        _proxied_clt.put('/test/foo', 'bar1')
        _proxied_clt.put('/test/foo', 'bar2')
        _proxied_clt.put('/test/foo', 'bar3')
        _proxied_clt.put('/test/foo', 'bar4')

        # Then
        events = _collect_events(watch_queue, timeout=3)
        eq_(3, len(events))
        _assert_event(Event.PUT, '/test/foo', 'bar1', events[0])
        _assert_event(Event.PUT, '/test/foo', 'bar3', events[1])
        _assert_event(Event.PUT, '/test/foo', 'bar4', events[2])

    finally:
        w.stop(timeout=1)


def _assert_event(t, k, v, got):
    eq_((t, _utils.to_bytes(k), _utils.to_bytes(v)),
        (got.type, got.kv.key, got.kv.value))


def _collect_events(watch_queue, timeout):
    events = []
    while True:
        try:
            e = watch_queue.get(timeout=timeout)
            events.append(e)

        except queue.Empty:
            break

    return events
