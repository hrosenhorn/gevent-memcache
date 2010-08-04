# Copyright (C) 2009, Hyves (Startphone Ltd.)
#
# This module is part of the Concurrence Framework and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
import logging
logging.TRACE = 5

class MemcacheError(Exception):
    pass

class MemcacheResult(object):
    """representation of memcache result (codes)"""

    _interned = {}

    def __init__(self, name, msg = ''):
        self._name = name
        self._msg = msg

    @property
    def msg(self):
        return self._msg

    def __repr__(self):
        return "MemcacheResult.%s" % self._name

    def __eq__(self, other):
        return isinstance(other, MemcacheResult) and other._name == self._name

    @classmethod
    def get(cls, line):
        code = cls._interned.get(line, None)
        if code is None:
            #try client or server error
            if line.startswith('CLIENT_ERROR'):
                return MemcacheResult("CLIENT_ERROR", line[13:])
            elif line.startswith('SERVER_ERROR'):
                return MemcacheResult("SERVER_ERROR", line[13:])
            else:
                raise MemcacheError("unknown response: %s" % repr(line))
        else:
            return code

    @classmethod
    def _intern(cls, name):
        cls._interned[name] = MemcacheResult(name)
        return cls._interned[name]

MemcacheResult.OK = MemcacheResult._intern("OK")
MemcacheResult.STORED = MemcacheResult._intern("STORED")
MemcacheResult.NOT_STORED = MemcacheResult._intern("NOT_STORED")
MemcacheResult.EXISTS = MemcacheResult._intern("EXISTS")
MemcacheResult.NOT_FOUND = MemcacheResult._intern("NOT_FOUND")
MemcacheResult.DELETED = MemcacheResult._intern("DELETED")
MemcacheResult.ERROR = MemcacheResult._intern("ERROR")
MemcacheResult.TIMEOUT = MemcacheResult._intern("TIMEOUT")

from geventmemcache.client import Memcache, MemcacheConnection, MemcacheConnectionManager
from geventmemcache.behaviour import MemcacheBehaviour
from geventmemcache.protocol import MemcacheProtocol
from geventmemcache.codec import MemcacheCodec
