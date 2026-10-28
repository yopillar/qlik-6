"""Doublure de spark_foundation/runner.py : le decorateur du job et
l'enchainement des scripts."""

from core.pipeline import Pipeline, pipeline
from core.steps import run_scripts

__all__ = ["Pipeline", "pipeline", "run_scripts"]
