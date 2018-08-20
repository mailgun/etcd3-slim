from __future__ import absolute_import

from time import sleep

from nose.tools import eq_, with_setup, assert_not_equal
from six.moves import queue

from etcd3 import _utils
from etcd3._grpc_stubs.kv_pb2 import Event
from tests.etcd3 import _fixture


@with_setup(_fixture.setup, _fixture.teardown)
def test_watch_key():
    proxied_clt = _fixture.proxied_clt()
    watch_queue = queue.Queue()

    proxied_clt.put('/test/foo2', 'bar1')
    w = proxied_clt.new_watcher('/test/foo2', spin_pause=0.2,
                                event_handler=lambda e: watch_queue.put(e))
    w.start()
    try:
        sleep(0.5)

        # When
        proxied_clt.put('/test/foo1', 'bar2')
        proxied_clt.put('/test/foo3', 'bar5')
        proxied_clt.put('/test/foo2', 'bar6')
        proxied_clt.put('/test/foo21', 'bar7')
        proxied_clt.delete('/test/foo2')
        proxied_clt.put('/test/foo212', 'bar8')
        proxied_clt.put('/test/foo2', 'bar9')
        proxied_clt.put('/test/foo21', 'bar10')

        # Then
        events = _collect_events(watch_queue, timeout=3)
        eq_(3, len(events))
        _assert_event(Event.PUT, '/test/foo2', 'bar6', events[0])
        _assert_event(Event.DELETE, '/test/foo2', '', events[1])
        _assert_event(Event.PUT, '/test/foo2', 'bar9', events[2])

    finally:
        w.stop(timeout=1)


@with_setup(_fixture.setup, _fixture.teardown)
def test_watch_key_prefix():
    proxied_clt = _fixture.proxied_clt()
    watch_queue = queue.Queue()

    proxied_clt.put('/test/foo2', 'bar1')
    w = proxied_clt.new_watcher('/test/foo2', is_prefix=True, spin_pause=0.2,
                                event_handler=lambda e: watch_queue.put(e))
    w.start()
    try:
        sleep(0.5)

        # When
        proxied_clt.put('/test/foo1', 'bar6')
        proxied_clt.put('/test/foo21', 'bar7')
        proxied_clt.delete('/test/foo2')
        proxied_clt.put('/test/foo212', 'bar8')
        proxied_clt.delete('/test/foo1')
        proxied_clt.put('/test/foo21', 'bar10')
        proxied_clt.put('/test/foo3', 'bar11')

        # Then
        events = _collect_events(watch_queue, timeout=3)
        eq_(4, len(events))
        _assert_event(Event.PUT, '/test/foo21', 'bar7', events[0])
        _assert_event(Event.DELETE, '/test/foo2', '', events[1])
        _assert_event(Event.PUT, '/test/foo212', 'bar8', events[2])
        _assert_event(Event.PUT, '/test/foo21', 'bar10', events[3])

    finally:
        w.stop(timeout=1)


@with_setup(_fixture.setup, _fixture.teardown)
def test_watch_from_the_past():
    # A revision to start watching from can be specified.

    proxied_clt = _fixture.proxied_clt()
    watch_queue = queue.Queue()

    proxied_clt.put('/test/foo2', 'bar1')
    proxied_clt.put('/test/foo2', 'bar2')
    put_rs = proxied_clt.put('/test/foo2', 'bar3')
    proxied_clt.put('/test/foo2', 'bar4')
    proxied_clt.put('/test/foo2', 'bar5')
    proxied_clt.put('/test/foo3', 'bar6')

    # When
    w = proxied_clt.new_watcher('/test/foo2', spin_pause=0.2,
                                start_revision=put_rs.header.revision,
                                event_handler=lambda e: watch_queue.put(e))
    w.start()
    try:
        proxied_clt.delete('/test/foo2')

        # Then
        events = _collect_events(watch_queue, timeout=3)
        eq_(4, len(events))
        _assert_event(Event.PUT, '/test/foo2', 'bar3', events[0])
        _assert_event(Event.PUT, '/test/foo2', 'bar4', events[1])
        _assert_event(Event.PUT, '/test/foo2', 'bar5', events[2])
        _assert_event(Event.DELETE, '/test/foo2', '', events[3])

    finally:
        w.stop(timeout=1)


@with_setup(_fixture.setup, _fixture.teardown)
def test_auto_reconnect_node():
    # Watch is restored even after a cluster wide outage.

    proxied_clt = _fixture.proxied_clt()
    watch_queue = queue.Queue()

    put_rs = _fixture.aux_clt().put('/test/foo', 'bar0')
    w = proxied_clt.new_watcher('/test/foo', spin_pause=0.2,
                                start_revision=put_rs.header.revision + 1,
                                event_handler=lambda e: watch_queue.put(e))
    w.start()
    try:
        _fixture.aux_clt().put('/test/foo', 'bar1')
        events = _collect_events(watch_queue, timeout=3)
        eq_(1, len(events))

        orig_endpoint = proxied_clt.current_endpoint
        _fixture.aux_clt().put('/test/foo', 'bar2')
        _fixture.aux_clt().put('/test/foo', 'bar3')
        _fixture.aux_clt().put('/test/foo', 'bar4')
        events = _collect_events(watch_queue, timeout=3)
        eq_(3, len(events))
        eq_(orig_endpoint, proxied_clt.current_endpoint)

        # When
        _fixture.disable_endpoint(orig_endpoint)

        # Then: immediately reconnected to another endpoint.
        _fixture.aux_clt().delete('/test/foo')
        _fixture.aux_clt().put('/test/foo', 'bar5')
        events = _collect_events(watch_queue, timeout=3)
        eq_(2, len(events))
        _assert_event(Event.DELETE, '/test/foo', '', events[0])
        _assert_event(Event.PUT, '/test/foo', 'bar5', events[1])

        assert_not_equal(orig_endpoint, proxied_clt.current_endpoint)

    finally:
        w.stop(timeout=1)


@with_setup(_fixture.setup, _fixture.teardown)
def test_auto_reconnect_outage():
    # Watch is restored even after a cluster wide outage.

    proxied_clt = _fixture.proxied_clt()
    watch_queue = queue.Queue()

    put_rs = _fixture.aux_clt().put('/test/foo', 'bar0')
    w = proxied_clt.new_watcher('/test/foo', spin_pause=0.2,
                                start_revision=put_rs.header.revision + 1,
                                event_handler=lambda e: watch_queue.put(e))
    w.start()
    try:
        _fixture.aux_clt().put('/test/foo', 'bar1')
        _fixture.aux_clt().put('/test/foo', 'bar2')
        sleep(0.5)

        # When connection with Etcd is lost...
        _fixture.disable_all_endpoints()

        # New events stop coming.
        _fixture.aux_clt().put('/test/foo', 'bar3')
        _fixture.aux_clt().delete('/test/foo')
        _fixture.aux_clt().put('/test/foo', 'bar4')
        events = _collect_events(watch_queue, timeout=3)
        eq_(2, len(events))
        _assert_event(Event.PUT, '/test/foo', 'bar1', events[0])
        _assert_event(Event.PUT, '/test/foo', 'bar2', events[1])

        # But as soon as the cluster is back in service...
        _fixture.enabled_all_endpoints()

        # ...backed up events are received
        events = _collect_events(watch_queue, timeout=5)
        eq_(3, len(events))
        _assert_event(Event.PUT, '/test/foo', 'bar3', events[0])
        _assert_event(Event.DELETE, '/test/foo', '', events[1])
        _assert_event(Event.PUT, '/test/foo', 'bar4', events[2])

    finally:
        w.stop(timeout=1)


@with_setup(_fixture.setup, _fixture.teardown)
def test_etcd_down_on_start():
    # If connection with Etcd cannot be established when a watch is started,
    # then events are received as soon as the connection is up.

    proxied_clt = _fixture.proxied_clt()
    watch_queue = queue.Queue()

    put_rs = proxied_clt.put('/test/foo', 'bar1')
    proxied_clt.delete('/test/foo')
    proxied_clt.put('/test/foo', 'bar2')

    w = proxied_clt.new_watcher('/test/foo', spin_pause=0.2,
                                start_revision=put_rs.header.revision,
                                event_handler=lambda e: watch_queue.put(e))

    # When connection with Etcd is lost...
    _fixture.disable_all_endpoints()

    w.start()
    try:
        events = _collect_events(watch_queue, timeout=3)
        eq_(0, len(events))

        # But as soon as connection with Etcd is restored...
        _fixture.enabled_all_endpoints()

        # ...backed up events are received
        events = _collect_events(watch_queue, timeout=5)
        eq_(3, len(events))
        _assert_event(Event.PUT, '/test/foo', 'bar1', events[0])
        _assert_event(Event.DELETE, '/test/foo', '', events[1])
        _assert_event(Event.PUT, '/test/foo', 'bar2', events[2])

    finally:
        w.stop(timeout=1)


@with_setup(_fixture.setup, _fixture.teardown)
def test_handler_exception():
    # Event handler failures do not disrupt watch operation.

    proxied_clt = _fixture.proxied_clt()
    watch_queue = queue.Queue()

    handle_count = [0]

    def event_handler(e):
        handle_count[0] += 1
        if handle_count[0] == 2:
            raise RuntimeError('Kaboom!')

        watch_queue.put(e)

    put_rs = _fixture.aux_clt().put('/test/foo', 'bar0')
    w = proxied_clt.new_watcher('/test/foo', spin_pause=0.2,
                                start_revision=put_rs.header.revision + 1,
                                event_handler=event_handler)
    w.start()
    try:
        # When
        proxied_clt.put('/test/foo', 'bar1')
        proxied_clt.put('/test/foo', 'bar2')
        proxied_clt.put('/test/foo', 'bar3')
        proxied_clt.put('/test/foo', 'bar4')

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
