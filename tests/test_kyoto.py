# -*- coding: utf-8 -*-

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

    def test_echo(self):
        self.assertEqual(self.dut.echo({'k': 'v'}), {'k': 'v'})

    def test_report(self):
        actual = self.dut.report()
        assert 'cnt_get' in actual

    def test_status(self):
        actual = self.dut.status()
        assert all(name in actual for name in ('count', 'size'))

    def test_clear(self):
        self.dut.add("k", "v")
        self.dut.clear()
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.get, "k")

    def test_set(self):
        self.dut.set("k", "v")
        self.assertEqual(self.dut.get("k"), ("v", None))
        self.dut.set("k", "w")
        self.assertEqual(self.dut.get("k"), ("w", None))

    def test_add(self):
        self.dut.add("k", "v")
        self.assertEqual(self.dut.get("k"), ("v", None))

    def test_add_error_existing_record_was_detected(self):
        self.dut.add("k", "v")
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.add, "k", "w")

    def test_increment(self):
        self.assertEqual(self.dut.increment("count", 1), 1)
        self.assertEqual(self.dut.increment("count", 2), 3)
        self.assertEqual(self.dut.increment("count", 0), 3)
        self.assertEqual(self.dut.get("count"), ("\x00\x00\x00\x00\x00\x00\x00\x03", None))

    def test_increment_error_incompatible_existing_record(self):
        self.dut.set("count", "1")
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.increment, "count", 2)

    def test_increment_with_try(self):
        self.assertEqual(self.dut.increment("count", 1), 1)
        self.assertEqual(self.dut.increment("count", 2, orig=b"try"), 3)

    def test_increment_with_try_error_incompatible_existing_record(self):
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.increment, "count", 1, orig=b"try")

    def test_increment_with_set(self):
        self.assertEqual(self.dut.increment("count", 1, orig=b"set"), 1)
        self.assertEqual(self.dut.increment("count", 2, orig=b"set"), 2)

    def test_increment_with_set_error_incompatible_existing_record(self):
        self.dut.set("count", "1")
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.increment, "count", 2, orig=b"set")

    def test_get(self):
        self.dut.set("k", "v")
        self.assertEqual(self.dut.get("k"), ("v", None))

    def test_get_error_no_record_was_found(self):
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.get, "k")

    def test_get_expired(self):
        self.dut.set("k", "v", 1)
        self.assertEqual(self.dut.get("k"), ("v", int(time.time() + 1)))
        time.sleep(2)
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.get, "k")

    def test_check(self):
        self.dut.set("k", "v")
        self.assertEqual(self.dut.check("k"), (1, None))

    def test_check_error_no_record_was_found(self):
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.check, "k")

    def test_remove_bulk(self):
        self.assertEqual(self.dut.remove_bulk(["k", "l"]), 0)
        self.dut.set("k", "v")
        self.dut.set("l", "w")
        self.assertEqual(self.dut.remove_bulk(["k", "l"]), 2)

    def test_remove_bulk_with_atomic(self):
        self.assertEqual(self.dut.remove_bulk(["k", "l"], atomic=True), 0)

    def test_get_bulk(self):
        self.assertEqual(self.dut.get_bulk(["k", "l"]), {})
        self.dut.set("k", "v")
        self.dut.set("l", "w")
        self.assertEqual(self.dut.get_bulk(["k", "l"]), {"k": "v", "l": "w"})

    def test_get_bulk_with_atomic(self):
        self.assertEqual(self.dut.get_bulk(["k", "l"], atomic=True), {})

    def test_match_prefix(self):
        self.assertEqual(self.dut.match_prefix("k"), [])
        self.dut.set("k", "v")
        self.dut.set("kk", "vv")
        self.dut.set("l", "w")
        self.assertEqual(self.dut.match_prefix("k"), ["k", "kk"])

    def test_match_prefix_with_max(self):
        self.dut.set("k", "v")
        self.dut.set("kk", "vv")
        self.dut.set("l", "w")
        self.assertEqual(self.dut.match_prefix("k", max=1), ["k"])
