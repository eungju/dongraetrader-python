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

    def communication_failure(self):
        raise DummyException()


class ConnectionPoolTest(unittest.TestCase):
    def setUp(self):
        self.dut = connection.ConnectionPool({}, DummyConnection)

    def test_guard_release_connection_if_exception_is_not_raised(self):
        with self.dut.connection() as c:
            x = c
            c.success()
        self.assertFalse(x.closed)
        self.assertTrue(x in self.dut.pool.queue)

    def test_guard_release_connection_if_unexpected_exception_is_raised(self):
        try:
            with self.dut.connection() as c:
                x = c
                raise Exception
            self.fail()
        except Exception:
            self.assertFalse(x.closed)
        self.assertTrue(x in self.dut.pool.queue)

    def test_guard_abandon_connection_if_expected_exception_is_raised(self):
        try:
            with self.dut.connection() as c:
                x = c
                c.communication_failure()
            self.fail()
        except DummyException:
            self.assertTrue(x.closed)
            self.assertTrue(x not in self.dut.pool.queue)
