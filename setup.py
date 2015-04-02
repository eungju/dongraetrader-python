"""
Dongrae Trader
--------------
"""

from setuptools import setup

tests_require = [
    'pytest >= 2.5.0'
]

extras_require = {
    'tests': tests_require,
}

config = {
    'name': 'dongraetrader',
    'version': '0.1',
    'description': 'Python client for Kyoto Tycoon',
    'long_description': __doc__,
    'url': 'https://github.com/eungju/dongraetrader-python',
    'author': 'Park Eungju',
    'author_email': 'eungju@gmail.com',
    'packages': ['dongraetrader'],
    'tests_require': tests_require,
    'extras_require': extras_require
}

setup(**config)
