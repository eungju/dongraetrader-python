import sys


class Serializer(object):
    def serialize(self, v):
        raise NotImplementedError

    def deserialize(self, b):
        raise NotImplementedError


class BytesSerializer(Serializer):
    def serialize(self, v):
        return v

    def deserialize(self, b):
        return b


class TextSerializer(Serializer):
    def __init__(self, text_encoding='utf-8'):
        self.text_encoding = text_encoding

    def serialize(self, v):
        return v.encode(self.text_encoding)

    def deserialize(self, b):
        return b.decode(self.text_encoding)


if sys.version < '3':
    StrSerializer = BytesSerializer
else:
    StrSerializer = TextSerializer
