"""
Dongrae Trader
--------------
"""

from setuptools import setup

config = {
    'name': 'dongraetrader',
    'version': '0.1',
    'description': 'Python client for Kyoto Tycoon',
    'long_description': __doc__,
    'url': 'http://github.com/eungju/dongraetrader-python',
    'author': 'Park Eungju',
    'author_email': 'eungju@gmail.com',
    'packages': ['dongraetrader'],
    'tests_require': ['pytest>=2.5.0']
}

setup(**config)
