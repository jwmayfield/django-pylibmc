"pylibmc memcached cache backend"

from django.core.cache.backends.base import (BaseCache,
                                             InvalidCacheBackendError)
from django.utils.encoding import smart_unicode, smart_str

try:
    import pylibmc as memcache
    is_pylibmc = True
except ImportError:
    is_pylibmc = False
    try:
        import memcache
    except:
        raise InvalidCacheBackendError(
                "pylibmc cache backend requires either the "
                "'memcache' or 'pylibmc' library")


# See http://sendapatch.se/projects/pylibmc/#behaviors
# for a description of these.
DEFAULT_BEHAVIORS = {"cache_lookups": True,
                     "no_block": False,
                     "tcp_nodelay": True}


class CacheClass(BaseCache):

    def __init__(self, server, params):
        from django.conf import settings
        BaseCache.__init__(self, params)
        if is_pylibmc:
            # use the binary protocol by default.
            binary = getattr(settings, "PYLIBMC_USE_BINARY", True)
            # default is to use compression for objects larger than 10kb.
            self.compress_at = getattr(settings, "PYLIBMC_COMPRESS_AT",
                                       10 * 1024)
            # Manage behaviors.
            behaviors = getattr(settings, "PYLIBMC_BEHAVIORS", {})
            behaviors = dict(DEFAULT_BEHAVIORS, **behaviors)
            self._cache = memcache.Client(server.split(';'), binary=binary)
            self._cache.behaviors = behaviors
        else:
            self._cache = memcache.Client(server.split(';'))

    def add(self, key, value, timeout=0):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        return self._cache.add(smart_str(key), value,
                timeout or self.default_timeout)

    def get(self, key, default=None):
        val = self._cache.get(smart_str(key))
        if val is None:
            return default
        else:
            if isinstance(val, basestring):
                return smart_unicode(val)
            else:
                return val

    def incr(self, key, delta=1):
        self._cache.incr(smart_str(key), delta)

    def decr(self, key, delta=1):
        self._cache.decr(smart_str(key), delta)

    def set(self, key, value, timeout=0, min_compress_len=None):
        if min_compress_len is None:
            min_compress_len = self.compress_at
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        kwargs = {}
        if is_pylibmc and min_compress_len is not None:
            kwargs = {"min_compress_len": min_compress_len}

        return self._cache.set(smart_str(key), value,
                               timeout or self.default_timeout,
                               **kwargs)

    def delete(self, key):
        self._cache.delete(smart_str(key))

    def get_many(self, keys):
        return self._cache.get_multi(map(smart_str, keys))

    def close(self, **kwargs):
        self._cache.disconnect_all()
