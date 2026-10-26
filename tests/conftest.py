"""Fixtures communes : un projet de demo copie dans un dossier temporaire.

Les outils se lancent depuis la racine du projet, comme en vrai : chaque
test copie tests/demo/ dans tmp_path, y cree un lien vers tools/, et lance
les outils en sous-processus avec ce dossier comme dossier courant.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
TOOLS = ROOT / "tools"
DEMO = Path(__file__).resolve().parent / "demo"
FAKELIB = Path(__file__).resolve().parent / "fakelib"

sys.path.insert(0, str(TOOLS))


def pytest_collection_modifyitems(config, items):
    # sans pyspark, les tests Spark sont sautes plutot qu'en echec
    if importlib.util.find_spec("pyspark") is None:
        skip = pytest.mark.skip(reason="pyspark absent")
        for item in items:
            if "spark" in item.keywords:
                item.add_marker(skip)


class Project:
    """Un projet de demo dans un dossier temporaire."""

    def __init__(self, path: Path):
        self.path = path

    def run(self, tool: str, *args: str, check: bool = True):
        """Lance tools/<tool> depuis la racine du projet, avec le faux
        spark_foundation dans le PYTHONPATH."""
        env = dict(
            os.environ,
            PYTHONPATH=str(FAKELIB),
            entrees="data/input",
            sorties="data/output",
        )
        res = subprocess.run(
            [sys.executable, str(TOOLS / tool), *args],
            cwd=self.path,
            env=env,
            capture_output=True,
            text=True,
        )
        if check and res.returncode:
            raise AssertionError(
                f"{tool} {' '.join(args)} : code "
                f"{res.returncode}\n{res.stdout}\n{res.stderr}"
            )
        return res

    def __truediv__(self, other):
        return self.path / other


@pytest.fixture
def project(tmp_path) -> Project:
    path = tmp_path / "demo"
    shutil.copytree(DEMO, path)
    return Project(path)
