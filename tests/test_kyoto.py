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
        self.dut.add("a", "1")
        self.dut.clear()
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.get, "a")

    def test_set(self):
        self.dut.set("a", "1")
        self.assertEqual(self.dut.get("a"), ("1", None))
        self.dut.set("a", "2")
        self.assertEqual(self.dut.get("a"), ("2", None))

    def test_add(self):
        self.dut.add("a", "1")
        self.assertEqual(self.dut.get("a"), ("1", None))

    def test_add_error_existing_record_was_detected(self):
        self.dut.add("a", "1")
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.add, "a", "2")

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
        self.assertEqual(self.dut.increment("count", 2, orig="try"), 3)

    def test_increment_with_try_error_incompatible_existing_record(self):
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.increment, "count", 1, orig="try")

    def test_increment_with_set(self):
        self.assertEqual(self.dut.increment("count", 1, orig="set"), 1)
        self.assertEqual(self.dut.increment("count", 2, orig="set"), 2)

    def test_increment_with_set_error_incompatible_existing_record(self):
        self.dut.set("count", "1")
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.increment, "count", 2, orig="set")

    def test_get(self):
        self.dut.set("a", "1")
        self.assertEqual(self.dut.get("a"), ("1", None))

    def test_get_error_no_record_was_found(self):
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.get, "a")

    def test_get_expired(self):
        self.dut.set("a", "1", 1)
        self.assertEqual(self.dut.get("a"), ("1", int(time.time() + 1)))
        time.sleep(2)
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.get, "a")

    def test_check(self):
        self.dut.set("a", "1")
        self.assertEqual(self.dut.check("a"), (1, None))

    def test_check_error_no_record_was_found(self):
        self.assertRaises(kyoto.LogicalInconsistencyError, self.dut.check, "a")

    def test_get_bulk(self):
        self.assertEqual(self.dut.get_bulk(["a", "b"]), {})
        self.dut.set("a", "1")
        self.dut.set("b", "2")
        self.assertEqual(self.dut.get_bulk(["a", "b"]), {"a": "1", "b": "2"})

    def test_match_prefix(self):
        self.assertEqual(self.dut.match_prefix("a"), [])
        self.dut.set("a", "1")
        self.dut.set("aa", "11")
        self.dut.set("b", "2")
        self.assertEqual(self.dut.match_prefix("a"), ["a", "aa"])

    def test_match_prefix_with_max(self):
        self.dut.set("a", "1")
        self.dut.set("aa", "11")
        self.dut.set("b", "2")
        self.assertEqual(self.dut.match_prefix("a", max=1), ["a"])


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
        dut.set("a", "1" * 1024)
        for i in range(1000):
            dut.get("a")
        e = time.time()
        self.assertLess(e - s, 2)
        dut.close()
