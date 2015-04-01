from __future__ import unicode_literals
import io
import base64
try:
    from urllib.parse import quote_from_bytes, unquote_to_bytes
except ImportError:
    from urllib2 import quote as quote_from_bytes, unquote as unquote_to_bytes

try:
    from http.client import HTTPConnection, HTTPException
except ImportError:
    from httplib import HTTPConnection, HTTPException

import sys
if sys.version < '3':
    text_type = unicode
    binary_type = str
else:
    text_type = str
    binary_type = bytes

from .connection import Connection, ConnectionPool


class ValueEncoding(object):
    def __init__(self, name):
        self.name = name

    def encode(self, s):
        raise NotImplementedError

    def decode(self, s):
        raise NotImplementedError


class RawValueEncoding(ValueEncoding):
    def __init__(self):
        super(RawValueEncoding, self).__init__(None)

    def encode(self, s):
        return s

    def decode(self, s):
        return s


class URLValueEncoding(ValueEncoding):
    def __init__(self):
        super(URLValueEncoding, self).__init__("U")

    def encode(self, s):
        return quote_from_bytes(s).encode('ascii')

    def decode(self, s):
        return unquote_to_bytes(s)


class Base64ValueEncoding(ValueEncoding):
    def __init__(self):
        super(Base64ValueEncoding, self).__init__("B")

    def encode(self, s):
        return base64.standard_b64encode(s)

    def decode(self, s):
        return base64.standard_b64decode(s)


def assoc_append(assoc, key, value):
    assoc.append((key, value))


def assoc_append_if_not_none(assoc, key, value):
    if value is not None:
        assoc_append(assoc, key, value)


def assoc_get(assoc, key):
    for k, v in assoc:
        if k == key:
            return v
    raise KeyError(key)


def assoc_find(assoc, key):
    for k, v in assoc:
        if k == key:
            return v
    return None


class TsvRpc(object):
    RECORD_SEPARATOR = b'\n'
    COLUMN_SEPARATOR = b'\t'

    CONTENT_TYPES = {
        "text/tab-separated-values": RawValueEncoding(),
        "text/tab-separated-values; colenc=U": URLValueEncoding(),
        "text/tab-separated-values; colenc=B": Base64ValueEncoding()
    }

    @classmethod
    def column_encoding_for(cls, content_type):
        return cls.CONTENT_TYPES[content_type]

    @classmethod
    def content_type_for(cls, column_encoding):
        return 'text/tab-separated-values; colenc=%s' % column_encoding.name

    @classmethod
    def read(cls, s, encoding):
        result = []
        rows = s.split(cls.RECORD_SEPARATOR)
        for row in rows:
            if row:
                columns = row.split(cls.COLUMN_SEPARATOR)
                result.append(tuple(encoding.decode(column) for column in columns))
        return result

    @classmethod
    def write(cls, records, encoding):
        buffer = io.BytesIO()
        try:
            for columns in records:
                first = True
                for column in columns:
                    if not first:
                        buffer.write(cls.COLUMN_SEPARATOR)
                    buffer.write(encoding.encode(column))
                    first = False
                buffer.write(cls.RECORD_SEPARATOR)
            return buffer.getvalue()
        finally:
            buffer.close()


class KyotoError(Exception):
    pass


class LogicalInconsistencyError(KyotoError):
    pass


class KyotoTycoonConnection(Connection):
    NAME_KEY = b'key'
    NAME_VALUE = b'value'
    NAME_DB = b'DB'
    NAME_XT = b'xt'
    NAME_ORIG = b'orig'
    NAME_ATOMIC = b'atomic'
    NAME_NUM = b'num'
    NAME_PREFIX = b'prefix'
    NAME_MAX = b'max'
    NAME_VSIZ = b'vsiz'
    NAME__ = b'_'
    NAME_ERROR = b'ERROR'

    def __init__(self, host, port, timeout=None):
        super(KyotoTycoonConnection, self).__init__([HTTPException])
        self.connection = HTTPConnection(host, port, timeout=timeout)
        self.connection.connect()
        self.str = "%s#%d(%s:%d)" % (self.__class__.__name__, id(self), host, port)
        self._text_encoding = 'utf-8'

    def __str__(self):
        return self.str

    def close(self):
        self.connection.close()

    def _encode_text(self, t):
        return t.encode(self._text_encoding)

    def _decode_text(self, b):
        return b.decode(self._text_encoding)

    def _encode_int(self, i):
        if i is None:
            return None
        return self._encode_text(str(i))

    def _decode_int(self, b):
        if b is None:
            return None
        return int(self._decode_text(b))

    def call(self, name, input):
        in_encoding = URLValueEncoding()
        body = TsvRpc.write(input, in_encoding)
        headers = {"Content-Type": TsvRpc.content_type_for(in_encoding)}
        self.connection.request("POST", "/rpc/%s" % name, body, headers)
        response = self.connection.getresponse()
        status, reason = response.status, response.reason
        out_encoding = TsvRpc.column_encoding_for(response.getheader("Content-Type"))
        x = response.read()
        output = TsvRpc.read(x, out_encoding) if out_encoding else None
        if status == 200:
            return output
        message = self._decode_text(assoc_get(output, self.NAME_ERROR) if output else reason)
        if status == 450:
            raise LogicalInconsistencyError(message)
        else:
            raise KyotoError(message)

    def void(self):
        self.call("void", [])

    def echo(self, records):
        input = [(k, v) for k, v in records.iteritems()]
        output = self.call("echo", input)
        return dict(output)

    def clear(self, db=None):
        input = []
        assoc_append_if_not_none(input, self.NAME_DB, db)
        self.call("clear", input)

    def set(self, key, value, xt=None, db=None):
        input = []
        assoc_append(input, self.NAME_KEY, key)
        assoc_append(input, self.NAME_VALUE, value)
        assoc_append_if_not_none(input, self.NAME_XT, self._encode_int(xt))
        assoc_append_if_not_none(input, self.NAME_DB, db)
        self.call("set", input)

    def add(self, key, value, xt=None, db=None):
        input = []
        assoc_append(input, self.NAME_KEY, key)
        assoc_append(input, self.NAME_VALUE, value)
        assoc_append_if_not_none(input, self.NAME_XT, self._encode_int(xt))
        assoc_append_if_not_none(input, self.NAME_DB, db)
        self.call("add", input)

    def increment(self, key, num, orig=None, xt=None, db=None):
        input = []
        assoc_append(input, self.NAME_KEY, key)
        assoc_append(input, self.NAME_NUM, self._encode_int(num))
        assoc_append_if_not_none(input, self.NAME_ORIG, orig)
        assoc_append_if_not_none(input, self.NAME_XT, self._encode_int(xt))
        assoc_append_if_not_none(input, self.NAME_DB, db)
        output = self.call("increment", input)
        return int(assoc_get(output, self.NAME_NUM))

    def get(self, key, db=None):
        input = []
        assoc_append(input, self.NAME_KEY, key)
        assoc_append_if_not_none(input, self.NAME_DB, db)
        output = self.call("get", input)
        return assoc_get(output, self.NAME_VALUE), self._decode_int(assoc_find(output, self.NAME_XT))

    def check(self, key, db=None):
        input = []
        assoc_append(input, self.NAME_KEY, key)
        assoc_append_if_not_none(input, self.NAME_DB, db)
        output = self.call("check", input)
        return int(assoc_get(output, self.NAME_VSIZ)), self._decode_int(assoc_find(output, self.NAME_XT))

    def remove_bulk(self, keys, atomic=None, db=None):
        input = []
        if atomic:
            assoc_append(input, self.NAME_ATOMIC, b'')
        assoc_append_if_not_none(input, self.NAME_DB, db)
        for key in keys:
            assoc_append(input, self.NAME__ + key, b'')
        output = self.call("remove_bulk", input)
        return int(assoc_get(output, self.NAME_NUM))

    def get_bulk(self, keys, atomic=None, db=None):
        input = []
        if atomic:
            assoc_append(input, self.NAME_ATOMIC, b'')
        assoc_append_if_not_none(input, self.NAME_DB, db)
        for key in keys:
            assoc_append(input, self.NAME__ + key, b'')
        output = self.call("get_bulk", input)
        return dict([(k[1:], v) for k, v in output if k.startswith(self.NAME__)])

    def match_prefix(self, prefix, max=None, db=None):
        input = []
        assoc_append(input, self.NAME_PREFIX, prefix)
        assoc_append_if_not_none(input, self.NAME_MAX, self._encode_int(max))
        assoc_append_if_not_none(input, self.NAME_DB, db)
        output = self.call("match_prefix", input)
        return [k[1:] for k, v in output if k.startswith(self.NAME__)]


class KyotoTycoonClient(object):
    def __init__(self, host, port, db=None, timeout=1, pool_conf=None):
        self.host = host
        self.port = port
        self.db = db
        self.pool = ConnectionPool(pool_conf, KyotoTycoonConnection, host=host, port=port, timeout=timeout)

    def __str__(self):
        return "%s#%d(%s:%d/%s)" % (self.__class__.__name__, id(self), self.host, self.port, self.db)

    def dispose(self):
        self.pool.disconnect()

    def void(self):
        with self.pool.connection() as c:
            c.void()

    def echo(self, records):
        with self.pool.connection() as c:
            c.echo(records)

    def clear(self):
        with self.pool.connection() as c:
            c.clear(db=self.db)

    def set(self, key, value, xt=None):
        with self.pool.connection() as c:
            c.set(key, value, xt=xt, db=self.db)

    def add(self, key, value, xt=None):
        with self.pool.connection() as c:
            c.add(key, value, xt=xt, db=self.db)

    def increment(self, key, num, orig=None, xt=None):
        with self.pool.connection() as c:
            return c.increment(key, num, orig=orig, xt=xt, db=self.db)

    def get(self, key):
        with self.pool.connection() as c:
            return c.get(key, db=self.db)

    def check(self, key):
        with self.pool.connection() as c:
            return c.check(key, db=self.db)

    def remove_bulk(self, keys, atomic=None):
        with self.pool.connection() as c:
            return c.remove_bulk(keys, atomic=atomic, db=self.db)

    def get_bulk(self, keys, atomic=None):
        with self.pool.connection() as c:
            return c.get_bulk(keys, atomic=atomic, db=self.db)

    def match_prefix(self, prefix, max=None):
        with self.pool.connection() as c:
            return c.match_prefix(prefix, max=max, db=self.db)
