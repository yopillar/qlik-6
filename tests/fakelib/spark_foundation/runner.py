"""Doublure de spark_foundation/runner.py : le decorateur du job et
l'enchainement des scripts."""

from core.runner import (
    Pipeline,
    parse_options,
    pipeline,
    run_scripts,
    script_name,
    select,
)

__all__ = [
    "Pipeline",
    "parse_options",
    "pipeline",
    "run_scripts",
    "script_name",
    "select",
]
