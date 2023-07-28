import re

from sparkdantic import __version__


def test_version():
    match = re.search(r'^\d+\.\d+\.\d+$', __version__)
    assert match is not None
