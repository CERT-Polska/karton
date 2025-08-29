import sys

if sys.version_info < (3, 11, 0):
    raise ImportError("karton.gateway is only compatible with Python 3.11+")
