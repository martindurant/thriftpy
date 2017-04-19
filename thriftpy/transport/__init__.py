# -*- coding: utf-8 -*-

from __future__ import absolute_import

from ..thrift import TType, TException


def readall(read_fn, sz):
    return read_fn.read(sz)


class TTransportBase(object):
    """Base class for Thrift transport layer."""

    def _read(self, sz):
        raise NotImplementedError

    def read(self, sz):
        return readall(self._read, sz)


class TTransportException(TException):
    """Custom Transport Exception class"""

    thrift_spec = {
        1: (TType.STRING, 'message'),
        2: (TType.I32, 'type'),
    }

    UNKNOWN = 0
    NOT_OPEN = 1
    ALREADY_OPEN = 2
    TIMED_OUT = 3
    END_OF_FILE = 4

    def __init__(self, type=UNKNOWN, message=None):
        super(TTransportException, self).__init__()
        self.type = type
        self.message = message


__all__ = ["TTransportBase", "TTransportException"]
