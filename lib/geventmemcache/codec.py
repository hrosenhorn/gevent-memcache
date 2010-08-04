import cPickle as pickle

from geventmemcache import MemcacheError

class MemcacheCodec(object):
    def decode(self, flags, encoded_value):
        assert False, "implement" #pragma: no cover

    def encode(self, value, flags):
        assert False, "implement" #pragma: no cover

    @classmethod
    def create(self, type_):
        if isinstance(type_, MemcacheCodec):
            return type_
        elif type_ == "default":
            return MemcacheDefaultCodec()
        elif type_ == "raw":
            return MemcacheRawCodec()
        else:
            raise MemcacheError("unknown codec: %s" % type_)

class MemcacheDefaultCodec(MemcacheCodec):
    _FLAG_PICKLE = 1<<0
    _FLAG_INTEGER = 1<<1
    _FLAG_LONG = 1<<2
    _FLAG_UNICODE = 1<<3

    def decode(self, flags, encoded_value):
        if flags & self._FLAG_INTEGER:
            return int(encoded_value)
        elif flags & self._FLAG_LONG:
            return long(encoded_value)
        elif flags & self._FLAG_UNICODE:
            return encoded_value.decode('utf-8')
        elif flags & self._FLAG_PICKLE:
            return pickle.loads(encoded_value)
        else:
            return encoded_value

    def encode(self, value, flags):
        if isinstance(value, str):
            encoded_value = value
        elif isinstance(value, int):
            flags |= self._FLAG_INTEGER
            encoded_value = str(value)
        elif isinstance(value, long):
            flags |= self._FLAG_LONG
            encoded_value = str(value)
        elif isinstance(value, unicode):
            flags |= self._FLAG_UNICODE
            encoded_value = value.encode('utf-8')
        else:
            flags |= self._FLAG_PICKLE
            # When we support the binary protocol we can change this
            encoded_value = pickle.dumps(value, 0)
        return encoded_value, flags

class MemcacheRawCodec(MemcacheCodec):
    def decode(self, flags, encoded_value):
        return encoded_value

    def encode(self, value, flags):
        return str(value), flags

