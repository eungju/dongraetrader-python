import platform
import pytest
import time

from dongraetrader import kyoto


pytestmark = pytest.mark.skipif(platform.python_implementation() == 'PyPy', reason="pypy+coverage has an issue")


def test_connect_and_close():
    s = time.time()
    for i in range(100):
        dut = kyoto.KyotoTycoonConnection('localhost', 1978)
        dut.close()
    e = time.time()
    assert e - s < 1


def test_get():
    s = time.time()
    dut = kyoto.KyotoTycoonConnection('localhost', 1978)
    dut.set("k", "v" * 1024)
    for i in range(1000):
        dut.get("k")
    e = time.time()
    assert e - s < 2
    dut.close()
