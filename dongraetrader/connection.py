import logging
import os
import time
try:
    from queue import LifoQueue, Full, Empty
except ImportError:
    from Queue import LifoQueue, Full, Empty


logger = logging.getLogger(__name__)


class Connection(object):
    def __init__(self, exc_types=None):
        self.exc_types = exc_types
        self.open_time = time.time()
        self.access_time = self.open_time

    def close(self):
        raise NotImplementedError

    def touch(self):
        self.access_time = time.time()


class ConnectionPool(object):
    def __init__(self, conf, connection_class, **connection_kwargs):
        self.pid = os.getpid()
        self.conf = {"max": 0, "min": 1, "timeout": 0.1, "idle_timeout": 60, "max_lifetime": 30 * 60}
        if conf:
            self.conf.update(conf)
        self.connection_class = connection_class
        self.connection_kwargs = connection_kwargs
        self.pool = LifoQueue(self.conf["max"])
        self.diet()

    def _checkpid(self):
        if self.pid != os.getpid():
            logger.info("This pool is created by other process.")
            self.dispose()
            self.__init__(self.conf, self.connection_class, **self.connection_kwargs)

    def dispose(self):
        self.clear()

    def clear(self):
        while True:
            try:
                conn = self.pool.get(block=False)
                if conn:
                    self.abandon(conn)
            except Empty:
                break

    def diet(self):
        for i in range(self.conf["min"]):
            try:
                self.pool.put(None, block=False)
            except Full:
                break

    def acquire(self):
        conn = None
        try:
            conn = self.pool.get(block=True, timeout=self.conf["timeout"])
        except Empty:
            logger.warning("No idle connection, create one more.")
        if conn:
            now = time.time()
            idle_time = now - conn.access_time
            life_time = now - conn.open_time
            if (idle_time > self.conf["idle_timeout"]) or (life_time > self.conf["max_lifetime"]):
                logger.debug("Discard obsolete connection %s. idle: %d, life: %d" % (conn, idle_time, life_time))
                conn.close()
                conn = None
        return conn or self.connection_class(**self.connection_kwargs)

    def release(self, conn):
        try:
            conn.touch()
            self.pool.put(conn, block=False)
        except Full:
            logger.warning("The pool is full, discard connection %s." % conn)
            conn.close()

    def abandon(self, conn):
        conn.close()
        self.clear()
        self.diet()

    def connection(self):
        self._checkpid()
        return ConnectionGuard(self)


class ConnectionGuard(object):
    def __init__(self, pool):
        self.pool = pool

    def __enter__(self):
        self.connection = self.pool.acquire()
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.pool.release(self.connection)
        else:
            if self.connection.exc_types and any(
                    isinstance(exc_value, exc_type) for exc_type in self.connection.exc_types):
                self.pool.abandon(self.connection)
            else:
                self.pool.release(self.connection)
            return False
