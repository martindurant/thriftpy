import numpy as np
cimport numpy as np
from libc.stdint cimport (
    int8_t, int16_t, int32_t, int64_t,
    uint8_t, uint16_t, uint32_t, uint64_t
)
from libc.string cimport memcpy


cdef class bytesIO:
    cdef char * buff
    cdef int loc
    cdef int size

    def __init__(self, bytes data):
        self.size = len(data)
        self.buff = data
        self.loc = 0

    cdef int check(self, int n):
        return self.loc + n < self.size

    cdef void advance(self, int n):
        self.loc += n

    cdef char * data(self):
        return self.buff + self.loc

    def read(self, int i):
        if self.loc >= self.size:
            return b''
        self.loc += i
        if self.loc > self.size:
            return self.buff[self.loc - i:self.size]
        return self.buff[self.loc - i:self.loc]

    def seek(self, int i):
        self.loc = i

    def tell(self):
        return self.loc


cdef int64_t read_varint(bytesIO data):
    cdef int64_t result = 0
    cdef short shift = 0
    cdef int i = 0

    while True:
        byte = data.data()[0]
        data.advance(1)
        result |= (byte & 0x7f) << shift
        if byte >> 7 == 0:
            return result
        shift += 7


cdef int64_t from_zig_zag(int64_t n):
    return (n >> 1) ^ -(n & 1)


cdef int64_t read_int(bytesIO data):
    return from_zig_zag(read_varint(data))


cdef int8_t byte(bytesIO data):
    cdef int8_t out
    memcpy(&out, data.data(), 1)
    data.advance(1)
    return out


cdef uint8_t ubyte(bytesIO data):
    cdef uint8_t out
    memcpy(&out, data.data(), 1)
    data.advance(1)
    return out


cdef double double(bytesIO data):
    cdef double out
    memcpy(&out, data.data(), 8)
    data.advance(8)


def read_collection_begin(bytesIO data):
    cdef uint8_t size_type
    cdef int size
    cdef uint8_t type
    if not data.check(1):
        raise RuntimeError('Input ended')
    size_type = ubyte(data)
    size = size_type >> 4
    type = size_type & 0x0f
    if size == 15:
        if not data.check(1):
            raise RuntimeError('Input ended')
        size = read_varint(data)
    return type, size


def read_map_begin(bytesIO data):
    cdef uint8_t size
    cdef uint8_t types = 0
    if not data.check(1):
        raise RuntimeError('Input ended')
    size = ubyte(data)
    if size > 0:
        if not data.check(1):
            raise RuntimeError('Input ended')
        types = ubyte(data)
    vtype = types & 0x0f
    ktype = types >> 4
    return (ktype, vtype, size)


def read_string(bytesIO data):
    cdef int64_t size
    if not data.check(1):
        raise RuntimeError('Input ended')
    size = read_varint(data)
    if not data.check(size):
        raise RuntimeError('Input ended')
    out = data.data()[:size].decode('UTF-8', 'strict')
    return out


cdef enum:
    STOP = 0x00
    TRUE = 0x01
    FALSE = 0x02
    BYTE = 0x03
    I16 = 0x04
    I32 = 0x05
    I64 = 0x06
    DOUBLE = 0x07
    BINARY = 0x08
    LIST = 0x09
    SET = 0x0A
    MAP = 0x0B
    STRUCT = 0x0C


def read_thrift(object thrift_struct, bytesIO data):
    """
    Read compact binary thrift data into specification
    
    :param thrift_spec: object
        Must have thrift_spec class attribute giving field types (which can
        point to further nested thrift objects.
    :param data: bytes
    :return: instance of thrift_spec
    """
    cdef int fid = 0  # field position
    cdef int delta
    cdef uint8_t type
    cdef dict out = {}
    while True:
        if not data.check(1):
            raise RuntimeError('Input ended')
        type = ubyte(data) & 0x0f
        if type & 0x0f == STOP:
            break

        delta = type >> 4
        if delta == 0:
            if not data.check(1):
                # could be more bytes here, but most commonly one
                raise RuntimeError('Input ended')
            fid = from_zig_zag(read_varint(data))
        else:
            fid += delta

        fspec = thrift_struct.thrift_spec[fid]
        ttype = fspec[0] & 0x0f
        if ttype in [MAP, SET, LIST]:
            fname, spec, req = fspec[1:]
        else:
            spec = None
            fname, req = fspec[1:]

        out[fname] = read_val(type, data, spec)
    return thrift_struct(**out)


def read_val(uint8_t type, bytesIO data, object spec):
    if type == TRUE:
        return True
    elif type == FALSE:
        return False
    elif type == BYTE:
        if not data.check(1):
            raise RuntimeError('Input ended')
        return byte(data)

    elif type in (I16, I32, I64):
        if not data.check(1):
            raise RuntimeError('Input ended')
        return from_zig_zag(read_varint(data))

    elif type == DOUBLE:
        if not data.check(8):
            raise RuntimeError('Input ended')
        return double(data)

    elif type == BINARY:
        return read_string(data)

    elif type in (LIST, SET):
        if isinstance(spec, tuple):
            v_type, v_spec = spec[0], spec[1]
        else:
            v_type, v_spec = spec, None
        result = []
        r_type, sz = read_collection_begin(data)

        for i in range(sz):
            if v_spec is None:
                result.append(read_val(v_type, data, None))
            else:
                result.append(read_thrift(spec, data))

        return result

    elif type == MAP:
        if isinstance(spec, tuple):
            k_type, v_type, v_spec = spec[0], spec[1], spec[1]
        else:
            k_type, v_type, v_spec = spec, None, None

        result = {}
        k_type, v_type, sz = read_map_begin(data)

        for i in range(sz):
            k_val = read_val(k_type, data)
            if v_spec:
                v_val = read_thrift(v_spec, data)
            else:
                v_val = read_val(v_type, data)
            result[k_val] = v_val
        return result

    elif type == STRUCT:
        return read_thrift(spec, data)
