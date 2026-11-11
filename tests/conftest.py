"""Repository-wide pytest setup.

Loads the credential output guard for every test run, including the wheel
smoke job that runs with ``-o pythonpath=`` (pytest puts this file's directory
on ``sys.path`` before importing it).
"""

import gc
from collections.abc import Iterator

import pytest
from _pytest_redact import (  # noqa: F401 - re-exported pytest hooks
    pytest_configure,
    pytest_itemcollected,
    pytest_make_collect_report,
    pytest_runtest_makereport,
    pytest_sessionstart,
)


@pytest.fixture(autouse=True)
def _fresh_exposure_hub() -> Iterator[None]:
    """The exposure hub is process-wide: no test inherits another's responder."""
    from rithmic_nt_connect.exposure import EXPOSURE_HUB

    EXPOSURE_HUB.reset()
    yield
    EXPOSURE_HUB.reset()


@pytest.fixture(autouse=True)
def _collect_on_this_thread() -> Iterator[None]:
    """Free each test's garbage on the thread that made it.

    NautilusTrader 2 objects (loggers, clocks, caches) must be dropped on their
    creating thread. A node test leaves reference cycles; a later automatic
    collection on a worker thread would drop them there instead.
    """
    yield
    gc.collect()
