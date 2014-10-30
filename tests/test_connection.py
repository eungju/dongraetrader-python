import unittest

from dongraetrader import connection


class DummyException(Exception):
    pass


class DummyConnection(connection.Connection):
    def __init__(self):
        super(DummyConnection, self).__init__([DummyException])
        self.closed = False

    def close(self):
        self.closed = True

    def success(self):
        return True

    def cause_communication_error(self):
        raise DummyException()

    def cause_auxiliary_error(self):
        raise Exception()


class ConnectionPoolTest(unittest.TestCase):
    def setUp(self):
        self.dut = connection.ConnectionPool({}, DummyConnection)

    def test_guard_release_connection_if_no_error(self):
        with self.dut.connection() as c:
            acquired = c
            c.success()
        self.assertFalse(acquired.closed)
        self.assertTrue(acquired in self.dut.pool.queue)

    def test_guard_release_connection_if_non_communication_error(self):
        try:
            with self.dut.connection() as c:
                acquired = c
                c.cause_auxiliary_error()
                raise Exception
            self.fail()
        except Exception:
            self.assertFalse(acquired.closed)
        self.assertTrue(acquired in self.dut.pool.queue)

    def test_guard_abandon_connection_if_communication_error(self):
        try:
            with self.dut.connection() as c:
                acquired = c
                c.cause_communication_error()
            self.fail()
        except DummyException:
            self.assertTrue(acquired.closed)
            self.assertTrue(acquired not in self.dut.pool.queue)
