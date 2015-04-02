import time
import unittest

from dongraetrader import kyoto


class AssocTest(unittest.TestCase):
    def test_append(self):
        assoc = []
        kyoto.assoc_append(assoc, "k", "v")
        self.assertEqual(assoc, [("k", "v")])

    def test_get(self):
        assoc = []
        self.assertRaises(KeyError, kyoto.assoc_get, assoc, "k")
        kyoto.assoc_append(assoc, "k", "v")
        self.assertEqual(kyoto.assoc_get(assoc, "k"), "v")

    def test_find(self):
        assoc = []
        self.assertIsNone(kyoto.assoc_find(assoc, "k"))
        kyoto.assoc_append(assoc, "k", "v")
        self.assertEqual(kyoto.assoc_find(assoc, "k"), "v")


class ColumnEncodingTest(unittest.TestCase):
    def test_raw(self):
        dut = kyoto.RawColumnEncoding()
        self.assertEquals(dut.encode(b'abc'), b'abc')
        self.assertEquals(dut.decode(b'abc'), b'abc')

    def test_url(self):
        dut = kyoto.URLColumnEncoding()
        self.assertEquals(dut.encode(b'\t\n'), b'%09%0A')
        self.assertEquals(dut.decode(b'%09%0A'), b'\t\n')

    def test_base64(self):
        dut = kyoto.Base64ColumnEncoding()
        self.assertEquals(dut.encode(b'\t\n'), b'CQo=')
        self.assertEquals(dut.decode(b'CQo='), b'\t\n')


class TsvRpcTest(unittest.TestCase):
    def setUp(self):
        self.dut = kyoto.TsvRpc
        self.column_encoding = kyoto.URLColumnEncoding()

    def test_read_empty(self):
        self.assertEquals(self.dut.read(b'', self.column_encoding), [])

    def test_read_one_row(self):
        self.assertEquals(self.dut.read(b'a\tb\n', self.column_encoding), [(b'a', b'b')])

    def test_read_multiple_rows(self):
        self.assertEquals(self.dut.read(b'a\tb\nc\td\n', self.column_encoding), [(b'a', b'b'), (b'c', b'd')])

    def test_write_empty(self):
        self.assertEquals(self.dut.write([], self.column_encoding), b'')

    def test_write_one_row(self):
        self.assertEquals(self.dut.write([(b'a', b'b')], self.column_encoding), b'a\tb\n')

    def test_write_multiple_rows(self):
        self.assertEquals(self.dut.write([(b'a', b'b'), (b'c', b'd')], self.column_encoding), b'a\tb\nc\td\n')

    def test_column_encoding_awareness(self):
        self.assertEquals(self.dut.read(b'%20\t%62\n', self.column_encoding), [(b' ', b'b')])
        self.assertEquals(self.dut.write([(b' ', b'b')], self.column_encoding), b'%20\tb\n')

    def test_row_can_have_one_column(self):
        self.assertEquals(self.dut.read(b'a\n', self.column_encoding), [(b'a',)])
        self.assertEquals(self.dut.write([(b'a', )], self.column_encoding), b'a\n')


class KyotoTycoonConnectionTest(unittest.TestCase):
    def setUp(self):
        self.dut = kyoto.KyotoTycoonConnection("localhost", 1978)
        self.dut.clear()

    def tearDown(self):
        self.dut.close()

    def test_tsvrpc_call_ok(self):
        self.assertEqual(self.dut.call("void", []), [])

    def test_tsvrpc_call_not_implemented(self):
        self.assertRaises(kyoto.KyotoError, self.dut.call, "not_implemented", [])

    def test_void(self):
        self.dut.void()

    def test_clear(self):
        self.dut.add(b"k", b"v")
        self.dut.clear()
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.get, b"k")

    def test_set(self):
        self.dut.set(b"k", b"v")
        self.assertEqual(self.dut.get(b"k"), (b"v", None))
        self.dut.set(b"k", b"w")
        self.assertEqual(self.dut.get(b"k"), (b"w", None))

    def test_add(self):
        self.dut.add(b"k", b"v")
        self.assertEqual(self.dut.get(b"k"), (b"v", None))

    def test_add_error_existing_record_was_detected(self):
        self.dut.add(b"k", b"v")
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.add, b"k", b"w")

    def test_increment(self):
        self.assertEqual(self.dut.increment(b"count", 1), 1)
        self.assertEqual(self.dut.increment(b"count", 2), 3)
        self.assertEqual(self.dut.increment(b"count", 0), 3)
        self.assertEqual(self.dut.get(b"count"), (b"\x00\x00\x00\x00\x00\x00\x00\x03", None))

    def test_increment_error_incompatible_existing_record(self):
        self.dut.set(b"count", b"1")
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.increment, b"count", 2)

    def test_increment_with_try(self):
        self.assertEqual(self.dut.increment(b"count", 1), 1)
        self.assertEqual(self.dut.increment(b"count", 2, orig=b"try"), 3)

    def test_increment_with_try_error_incompatible_existing_record(self):
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.increment, b"count", 1, orig=b"try")

    def test_increment_with_set(self):
        self.assertEqual(self.dut.increment(b"count", 1, orig=b"set"), 1)
        self.assertEqual(self.dut.increment(b"count", 2, orig=b"set"), 2)

    def test_increment_with_set_error_incompatible_existing_record(self):
        self.dut.set(b"count", b"1")
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.increment, b"count", 2, orig=b"set")

    def test_get(self):
        self.dut.set(b"k", b"v")
        self.assertEqual(self.dut.get(b"k"), (b"v", None))

    def test_get_error_no_record_was_found(self):
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.get, b"k")

    def test_get_expired(self):
        self.dut.set(b"k", b"v", 1)
        self.assertEqual(self.dut.get(b"k"), (b"v", int(time.time() + 1)))
        time.sleep(2)
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.get, b"k")

    def test_check(self):
        self.dut.set(b"k", b"v")
        self.assertEqual(self.dut.check(b"k"), (1, None))

    def test_check_error_no_record_was_found(self):
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.check, b"k")

    def test_get_bulk(self):
        self.assertEqual(self.dut.get_bulk([b"k", b"l"]), {})
        self.dut.set(b"k", b"v")
        self.dut.set(b"l", b"w")
        self.assertEqual(self.dut.get_bulk([b"k", b"l"]), {b"k": b"v", b"l": b"w"})

    def test_get_bulk_with_atomic(self):
        self.assertEqual(self.dut.get_bulk([b"k", b"l"], atomic=True), {})

    def test_match_prefix(self):
        self.assertEqual(self.dut.match_prefix(b"k"), [])
        self.dut.set(b"k", b"v")
        self.dut.set(b"kk",b"vv")
        self.dut.set(b"l", b"w")
        self.assertEqual(self.dut.match_prefix(b"k"), [b"k", b"kk"])

    def test_match_prefix_with_max(self):
        self.dut.set(b"k", b"v")
        self.dut.set(b"kk", b"vv")
        self.dut.set(b"l", b"w")
        self.assertEqual(self.dut.match_prefix(b"k", max=1), [b"k"])


class KyotoTycoonConnectionPerformanceTest(unittest.TestCase):
    def test_connect_and_close(self):
        s = time.time()
        for i in range(100):
            dut = kyoto.KyotoTycoonConnection('localhost', 1978)
            dut.close()
        e = time.time()
        self.assertLess(e - s, 1)

    def test_get(self):
        s = time.time()
        dut = kyoto.KyotoTycoonConnection('localhost', 1978)
        dut.set(b"k", b"v" * 1024)
        for i in range(1000):
            dut.get(b"k")
        e = time.time()
        self.assertLess(e - s, 2)
        dut.close()
