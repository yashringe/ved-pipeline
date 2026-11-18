"""Shared fixtures.

The Spark session is created once per test run. Spark tests skip (with the reason in
the summary) when pyspark or a Java runtime is missing, so the fast unit suites stay
usable on any laptop; CI installs both and runs everything.
"""

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Iterator
from typing import Any

import pytest


@pytest.fixture(scope="session")
def spark() -> Iterator[Any]:
    pytest.importorskip("pyspark", reason="pyspark not installed (pip install -e .[spark])")
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        pytest.skip("no Java runtime: Spark tests need Java 17")
    # Workers must use this interpreter, where vedpipe is installed.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    from vedpipe.spark.job import build_session

    session = build_session("local[2]")
    yield session
    session.stop()
