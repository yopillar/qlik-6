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
# doublure du socle spark_foundation, que le code genere importe
FAKELIB = Path(__file__).resolve().parent / "fakelib"
# le job genere trouve le socle (doublure) et core/, commons/ qu'elle utilise
JOB_PATH = os.pathsep.join([str(FAKELIB), str(ROOT)])

sys.path.insert(0, str(TOOLS))


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Saute les tests Spark quand pyspark n'est pas installe."""
    # sans pyspark, les tests Spark sont sautes plutot qu'en echec
    if importlib.util.find_spec("pyspark") is None:
        skip = pytest.mark.skip(reason="pyspark absent")
        for item in items:
            if "spark" in item.keywords:
                item.add_marker(skip)


class Project:
    """Un projet de demo dans un dossier temporaire."""

    def __init__(self, path: Path) -> None:
        """Projet copie dans path."""
        self.path = path

    def run(
        self, tool: str, *args: str, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        """Lance tools/<tool> depuis la racine du projet, avec la doublure
        du socle dans le PYTHONPATH (le job genere l'importe)."""
        env = dict(
            os.environ,
            PYTHONPATH=JOB_PATH,
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

    def run_job(self, *args: str) -> subprocess.CompletedProcess[str]:
        """Lance le job genere src/demo/job.py.

        Args:
            args: ses arguments (--only ventes...).

        Returns:
            Le resultat du processus ; une erreur s'il a echoue.
        """
        env = dict(
            os.environ,
            PYTHONPATH=JOB_PATH,
            entrees="data/input",
            sorties="data/output",
        )
        res = subprocess.run(
            [sys.executable, "src/demo/job.py", *args],
            cwd=self.path,
            env=env,
            capture_output=True,
            text=True,
        )
        if res.returncode:
            raise AssertionError(f"job.py {' '.join(args)}\n{res.stderr[-3000:]}")
        return res

    def __truediv__(self, other: str) -> Path:
        """Chemin d'un fichier du projet : project / "conf/io.yml"."""
        return self.path / other


@pytest.fixture
def project(tmp_path: Path) -> Project:
    """Le projet de demo, copie dans un dossier temporaire."""
    path = tmp_path / "demo"
    shutil.copytree(DEMO, path)
    return Project(path)
