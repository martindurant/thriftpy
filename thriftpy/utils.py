# -*- coding: utf-8 -*-

from __future__ import absolute_import

import binascii


def hexlify(byte_array, delimeter=' '):
    s = binascii.hexlify(byte_array).decode('utf-8')
    return delimeter.join(a+b for a, b in zip(s[::2], s[1::2]))


def hexprint(byte_array, delimeter=' ', count=10):
    print("Bytes:")
    print(byte_array)

    print("\nHex:")
    g = hexlify(byte_array, delimeter).split(delimeter)
    print('\n'.join(' '.join(g[i:i+10]) for i in range(0, len(g), 10)))
