# -*- coding: utf-8 -*-

"""
    thriftpy.thrift
    ~~~~~~~~~~~~~~~~~~

    Thrift simplified.
"""

from __future__ import absolute_import

import linecache
import types

from ._compat import with_metaclass


def args2kwargs(thrift_spec, *args):
    arg_names = [item[1][1] for item in sorted(thrift_spec.items())]
    return dict(zip(arg_names, args))


def parse_spec(ttype, spec=None):
    name_map = TType._VALUES_TO_NAMES

    def _type(s):
        return parse_spec(*s) if isinstance(s, tuple) else name_map[s]

    if spec is None:
        return name_map[ttype]

    if ttype == TType.STRUCT:
        return spec.__name__

    if ttype in (TType.LIST, TType.SET):
        return "%s<%s>" % (name_map[ttype], _type(spec))

    if ttype == TType.MAP:
        return "MAP<%s, %s>" % (_type(spec[0]), _type(spec[1]))


def init_func_generator(cls, spec):
    """Generate `__init__` function based on TPayload.default_spec

    For example::

        spec = [('name', 'Alice'), ('number', None)]

    will generate a types.FunctionType object representing::

        def __init__(self, name='Alice', number=None):
            self.name = name
            self.number = number
    """
    if not spec:
        def __init__(self):
            pass
        return __init__

    varnames, defaults = zip(*spec)

    args = ', '.join(map('{0[0]}={0[1]!r}'.format, spec))
    init = "def __init__(self, {0}):\n".format(args)
    init += "\n".join(map('    self.{0} = {0}'.format, varnames))

    name = '<generated {0}.__init__>'.format(cls.__name__)
    code = compile(init, name, 'exec')
    func = next(c for c in code.co_consts if isinstance(c, types.CodeType))

    # Add a fake linecache entry so debuggers and the traceback module can
    # better understand our generated code.
    linecache.cache[name] = (len(init), None, init.splitlines(True), name)

    return types.FunctionType(func, {}, argdefs=defaults)


class TType(object):
    STOP = 0
    VOID = 1
    BOOL = 2
    BYTE = 3
    I08 = 3
    DOUBLE = 4
    I16 = 6
    I32 = 8
    I64 = 10
    STRING = 11
    UTF7 = 11
    BINARY = 11  # This here just for parsing. For all purposes, it's a string
    STRUCT = 12
    MAP = 13
    SET = 14
    LIST = 15
    UTF8 = 16
    UTF16 = 17

    _VALUES_TO_NAMES = {
        STOP: 'STOP',
        VOID: 'VOID',
        BOOL: 'BOOL',
        BYTE: 'BYTE',
        I08: 'BYTE',
        DOUBLE: 'DOUBLE',
        I16: 'I16',
        I32: 'I32',
        I64: 'I64',
        STRING: 'STRING',
        UTF7: 'STRING',
        BINARY: 'STRING',
        STRUCT: 'STRUCT',
        MAP: 'MAP',
        SET: 'SET',
        LIST: 'LIST',
        UTF8: 'UTF8',
        UTF16: 'UTF16'
    }


class TMessageType(object):
    CALL = 1
    REPLY = 2
    EXCEPTION = 3
    ONEWAY = 4


class TPayloadMeta(type):

    def __new__(cls, name, bases, attrs):
        if "default_spec" in attrs:
            spec = attrs.pop("default_spec")
            attrs["__init__"] = init_func_generator(cls, spec)
        return super(TPayloadMeta, cls).__new__(cls, name, bases, attrs)


def gen_init(cls, thrift_spec=None, default_spec=None):
    if thrift_spec is not None:
        cls.thrift_spec = thrift_spec

    if default_spec is not None:
        cls.__init__ = init_func_generator(cls, default_spec)
    return cls


class TPayload(with_metaclass(TPayloadMeta, object)):

    __hash__ = None

    def read(self, iprot):
        iprot.read_struct(self)

    def write(self, oprot):
        oprot.write_struct(self)

    def __repr__(self):
        l = ['%s=%r' % (key, value) for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(l))

    def __str__(self):
        return repr(self)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


class TException(TPayload, Exception):
    """Base class for all thrift exceptions."""

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return id(self) == id(other)


class TDecodeException(TException):
    def __init__(self, name, fid, field, value, ttype, spec=None):
        self.struct_name = name
        self.fid = fid
        self.field = field
        self.value = value

        self.type_repr = parse_spec(ttype, spec)

    def __str__(self):
        return (
            "Field '%s(%s)' of '%s' needs type '%s', "
            "but the value is `%r`"
        ) % (self.field, self.fid, self.struct_name, self.type_repr,
             self.value)
