import six


def to_bytes(val):
    if isinstance(val, six.text_type):
        return val.encode('utf-8')

    if not isinstance(val, six.binary_type):
        raise TypeError('Unexpected type %s of %s' % (type(val), val))

    return val


def range_end(key):
    ba = bytearray(key)
    ba[-1] = ba[-1] + 1
    return six.binary_type(ba)
