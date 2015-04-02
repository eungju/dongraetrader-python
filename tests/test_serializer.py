# -*- coding: utf-8 -*-

from dongraetrader.serializer import BytesSerializer, TextSerializer, StrSerializer


def test_bytes_serializer():
    dut = BytesSerializer()
    assert dut.serialize(b'\xea\xb0\x80') == b'\xea\xb0\x80'
    assert dut.deserialize(b'\xea\xb0\x80') == b'\xea\xb0\x80'


def test_text_serializer():
    dut = TextSerializer()
    assert dut.serialize(u'가') == b'\xea\xb0\x80'
    assert dut.deserialize(b'\xea\xb0\x80') == u'가'


def test_str_serializer():
    dut = StrSerializer()
    assert dut.serialize('가') == b'\xea\xb0\x80'
    assert dut.deserialize(b'\xea\xb0\x80') == '가'
