"""Scrub live credentials at pytest output boundaries, including --showlocals.

Safe object repr is the first defense. Raw strings, protobufs, assertion
rewriting and captured child output still need a final guard. Only explicit
RITHMIC_TEST_DOTENV and process secrets are consulted; never the repo .env.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import pytest
from _pytest._io.saferepr import saferepr
from rithmic_nt_connect._redact import redact_text, secret_key
from rithmic_nt_connect.config import _parse_dotenv

_REDACT = pytest.StashKey[Callable[[str], str]]()


def pytest_configure(config: pytest.Config) -> None:
    secrets = [value for key, value in os.environ.items() if secret_key(key)]
    source = os.environ.get("RITHMIC_TEST_DOTENV")
    if source:
        try:
            values = _parse_dotenv(os.path.expanduser(source))
        except (OSError, UnicodeError):
            # The source loader reports invalid files; do not echo file contents.
            values = {}
        secrets.extend(value for key, value in values.items() if secret_key(key))
    # --showlocals truncates long raw strings before reports reach these hooks.
    # Scrub pytest's shortened representation as well as the full credential.
    secrets.extend(saferepr(str(value))[1:-1] for value in tuple(secrets))

    def scrub(text: str) -> str:
        return redact_text(text, secrets)

    config.stash[_REDACT] = scrub


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session: pytest.Session) -> None:
    config = session.config
    scrub = config.stash[_REDACT]
    reporter = config.pluginmanager.getplugin("terminalreporter")
    if reporter is not None:
        # pytest has no public final-terminal-text hook. Wrapping its writer
        # also covers live logging, warnings, collection errors and progress IDs.
        writer: Any = reporter._tw
        write = writer.write

        def safe_write(text: str, **markup: bool) -> None:
            write(scrub(text), **markup)

        writer.write = safe_write
        config.add_cleanup(lambda: setattr(writer, "write", write))


def _scrub_report(report: Any, config: pytest.Config) -> None:
    scrub = config.stash[_REDACT]
    report.nodeid = scrub(report.nodeid)
    if report.longrepr is not None:
        text = str(report.longrepr)
        cleaned = scrub(text)
        if cleaned != text:
            report.longrepr = cleaned
    report.sections = [(scrub(name), scrub(text)) for name, text in report.sections]
    if hasattr(report, "user_properties"):
        report.user_properties = [
            (scrub(name), scrub(str(value))) for name, value in report.user_properties
        ]


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    report = yield
    _scrub_report(report, item.config)
    return report


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_make_collect_report(collector: pytest.Collector):
    report = yield
    _scrub_report(report, collector.config)
    return report


def pytest_itemcollected(item: pytest.Item) -> None:
    # JUnit's testcase names and terminal progress are derived from nodeid.
    item._nodeid = item.config.stash[_REDACT](item.nodeid)
