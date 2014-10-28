from cStringIO import StringIO
import base64
import httplib
import urllib2

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
        return urllib2.quote(s)

    def decode(self, s):
        return urllib2.unquote(s)


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


def assoc_to_tsv(assoc, encoding):
    buffer = StringIO()
    try:
        for k, v in assoc:
            buffer.write(encoding.encode(k))
            buffer.write("\t")
            buffer.write(encoding.encode(v))
            buffer.write("\r\n")
        return buffer.getvalue()
    finally:
        buffer.close()


def tsv_to_assoc(s, encoding):
    result = []
    rows = s.splitlines()
    for row in rows:
        if row:
            columns = row.split("\t")
            assoc_append(result, encoding.decode(columns[0]), encoding.decode(columns[1]))
    return result


def none_or_str(i):
    return None if i is None else str(i)


def none_or_int(s):
    return None if s is None else int(s)


CONTENT_TYPE_TO_COLUMN_ENCODING = {
    "text/tab-separated-values": RawValueEncoding(),
    "text/tab-separated-values; colenc=U": URLValueEncoding(),
    "text/tab-separated-values; colenc=B": Base64ValueEncoding()
}


def get_column_encoding_by_content_type(content_type):
    return CONTENT_TYPE_TO_COLUMN_ENCODING[content_type]


class KyotoError(Exception):
    pass


class LogicalInconsistencyError(KyotoError):
    pass


class KyotoTycoonConnection(Connection):
    def __init__(self, host, port, timeout=None):
        super(KyotoTycoonConnection, self).__init__([httplib.HTTPException])
        self.connection = httplib.HTTPConnection(host, port, timeout=timeout)
        self.connection.connect()
        self.str = "%s#%d(%s:%d)" % (self.__class__.__name__, id(self), host, port)

    def __str__(self):
        return self.str

    def close(self):
        self.connection.close()

    def call(self, name, input):
        in_encoding = URLValueEncoding()
        body = assoc_to_tsv(input, in_encoding)
        headers = {"Content-Type": "text/tab-separated-values; colenc=%s" % in_encoding.name}
        self.connection.request("POST", "/rpc/%s" % name, body, headers)
        response = self.connection.getresponse()
        status, reason = response.status, response.reason
        out_encoding = get_column_encoding_by_content_type(response.getheader("Content-Type"))
        output = tsv_to_assoc(response.read(), out_encoding) if out_encoding else None
        #print (status, reason, output)
        if status == 200:
            return output
        message = assoc_get(output, "ERROR") if output else reason
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
        assoc_append_if_not_none(input, "DB", db)
        self.call("clear", input)

    def set(self, key, value, xt=None, db=None):
        input = []
        assoc_append(input, "key", key)
        assoc_append(input, "value", value)
        assoc_append_if_not_none(input, "xt", none_or_str(xt))
        assoc_append_if_not_none(input, "DB", db)
        self.call("set", input)

    def add(self, key, value, xt=None, db=None):
        input = []
        assoc_append(input, "key", key)
        assoc_append(input, "value", value)
        assoc_append_if_not_none(input, "xt", none_or_str(xt))
        assoc_append_if_not_none(input, "DB", db)
        self.call("add", input)

    def increment(self, key, num, orig=None, xt=None, db=None):
        input = []
        assoc_append(input, "key", key)
        assoc_append(input, "num", str(num))
        assoc_append_if_not_none(input, "orig", orig)
        assoc_append_if_not_none(input, "xt", none_or_str(xt))
        assoc_append_if_not_none(input, "DB", db)
        output = self.call("increment", input)
        return int(assoc_get(output, "num"))

    def get(self, key, db=None):
        input = []
        assoc_append(input, "key", key)
        assoc_append_if_not_none(input, "DB", db)
        output = self.call("get", input)
        return assoc_get(output, "value"), none_or_int(assoc_find(output, "xt"))

    def check(self, key, db=None):
        input = []
        assoc_append(input, "key", key)
        assoc_append_if_not_none(input, "DB", db)
        output = self.call("check", input)
        return int(assoc_get(output, "vsiz")), none_or_int(assoc_find(output, "xt"))

    def remove_bulk(self, keys, atomic=None, db=None):
        input = []
        assoc_append_if_not_none(input, "atomic", atomic)
        assoc_append_if_not_none(input, "DB", db)
        for key in keys:
            assoc_append(input, "_" + key, "")
        output = self.call("remove_bulk", input)
        return int(assoc_get(output, "num"))

    def get_bulk(self, keys, atomic=None, db=None):
        input = []
        assoc_append_if_not_none(input, "atomic", atomic)
        assoc_append_if_not_none(input, "DB", db)
        for key in keys:
            assoc_append(input, "_" + key, "")
        output = self.call("get_bulk", input)
        return dict([(k[1:], v) for k, v in output if k[0] == "_"])

    def match_prefix(self, prefix, max=None, db=None):
        input = []
        assoc_append(input, "prefix", prefix)
        assoc_append_if_not_none(input, "max", none_or_str(max))
        assoc_append_if_not_none(input, "DB", db)
        output = self.call("match_prefix", input)
        return [k[1:] for k, v in output if k[0] == "_"]


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
