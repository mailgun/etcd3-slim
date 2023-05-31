

def to_bytes(val):
    if isinstance(val, str):
        return val.encode('utf-8')

    if not isinstance(val, bytes):
        raise TypeError('Unexpected type %s of %s' % (type(val), val))

    return val


def range_end(key):
    ba = bytearray(key)
    ba[-1] = ba[-1] + 1
    return bytes(ba)
