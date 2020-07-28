import sys
import os

from utils.string_help import StringHelp


a = [1, 3, 4]
try:
    print(a[9])
except Exception as e:
    error_str = repr(e)
    content = StringHelp.error(error_str)
    print(content)
