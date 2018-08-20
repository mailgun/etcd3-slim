from __future__ import absolute_import

from time import sleep

from nose.tools import eq_, with_setup, assert_not_equal

from tests.etcd3 import _fixture


@with_setup(_fixture.setup, _fixture.teardown)
def test_normal_operation():
    # KeepAliver ensure that key exists while running and deletes it on stop.

    proxied_clt = _fixture.proxied_clt()

    ttl = 2
    keep_aliver = proxied_clt.new_keep_aliver('/test/keep-alive-0', 'bar', ttl,
                                              spin_pause=0.2)
    keep_aliver.start()
    try:
        for i in range(3):
            sleep(ttl - 0.5)
            eq_(b'bar', proxied_clt.get_value('/test/keep-alive-0'))

    finally:
        eq_(True, keep_aliver.stop(timeout=3))

    eq_(None, proxied_clt.get_value('/test/keep-alive-0'))


@with_setup(_fixture.setup, _fixture.teardown)
def test_spin_pause_too_long():
    # Spin pause is adjusted if needed to be less then effective TTL.

    proxied_clt = _fixture.proxied_clt()

    ttl = 3
    keep_aliver = proxied_clt.new_keep_aliver('/test/keep-alive-1', 'bar', ttl,
                                              spin_pause=10)
    keep_aliver.start()
    try:
        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-1'))

    finally:
        eq_(True, keep_aliver.stop(timeout=3))

    eq_(None, proxied_clt.get_value('/test/keep-alive-1'))


@with_setup(_fixture.setup, _fixture.teardown)
def test_auto_reconnect_node():
    # If the node the client is connected to goes down, then the client
    # recoonects to another node immediately.

    proxied_clt = _fixture.proxied_clt()

    ttl = 2
    keep_aliver = proxied_clt.new_keep_aliver('/test/keep-alive-4', 'bar', ttl,
                                              spin_pause=0.2)
    keep_aliver.start()
    try:
        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-4'))
        orig_endpoint = proxied_clt.current_endpoint

        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-4'))
        eq_(orig_endpoint, proxied_clt.current_endpoint)

        # When
        _fixture.disable_endpoint(orig_endpoint)

        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-4'))
        assert_not_equal(orig_endpoint, proxied_clt.current_endpoint)
        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-4'))
        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-4'))

    finally:
        eq_(True, keep_aliver.stop(timeout=3))

    eq_(None, proxied_clt.get_value('/test/keep-alive-4'))


@with_setup(_fixture.setup, _fixture.teardown)
def test_auto_reconnect_outage():
    # If cluster wide outage lasts longer than the TTL, the key obviously
    # expires, but it gets recreated after a connection is restored.

    proxied_clt = _fixture.proxied_clt()

    ttl = 2
    keep_aliver = proxied_clt.new_keep_aliver('/test/keep-alive-2', 'bar', ttl,
                                              spin_pause=0.2)
    keep_aliver.start()
    try:
        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-2'))
        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-2'))

        # If connection with Etcd is lost...
        _fixture.disable_all_endpoints()
        # ...the key expires
        sleep(ttl + 0.5)
        eq_(None, _fixture.aux_clt().get_value('/test/keep-alive-2'))

        # When: etcd gets back in service
        _fixture.enabled_all_endpoints()

        # Then: the key is automatically restored.
        sleep(2)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-2'))
        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-2'))

    finally:
        eq_(True, keep_aliver.stop(timeout=3))

    eq_(None, proxied_clt.get_value('/test/keep-alive-2'))


@with_setup(_fixture.setup, _fixture.teardown)
def test_etcd_down_on_start():
    # It is ok to create a keep_aliver when connection with Etcd is down. The
    # key will be created as soon as connection is restored.

    proxied_clt = _fixture.proxied_clt()

    # Simulate Etcd down on start.
    _fixture.disable_all_endpoints()

    ttl = 2
    keep_aliver = proxied_clt.new_keep_aliver('/test/keep-alive-3', 'bar', ttl,
                                              spin_pause=0.2)
    keep_aliver.start()
    try:
        sleep(0.5)
        eq_(None, _fixture.aux_clt().get_value('/test/keep-alive-3'))

        # When: etcd gets back in service.
        _fixture.enabled_all_endpoints()

        # Then: the key is automatically created.
        sleep(0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-3'))
        sleep(ttl - 0.5)
        eq_(b'bar', proxied_clt.get_value('/test/keep-alive-3'))

    finally:
        eq_(True, keep_aliver.stop(timeout=3))

    eq_(None, proxied_clt.get_value('/test/keep-alive-3'))
