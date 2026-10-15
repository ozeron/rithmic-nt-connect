"""Repository-wide pytest setup.

Loads the credential output guard for every test run, including the wheel
smoke job that runs with ``-o pythonpath=`` (pytest puts this file's directory
on ``sys.path`` before importing it).
"""

from _pytest_redact import (  # noqa: F401 - re-exported pytest hooks
    pytest_configure,
    pytest_itemcollected,
    pytest_make_collect_report,
    pytest_runtest_makereport,
    pytest_sessionstart,
)
