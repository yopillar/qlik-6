"""Doublure de spark_foundation/runner.py : le decorateur du job et celui
de chaque script."""

from core.runner import Pipeline, parse_options, pipeline, script

__all__ = ["Pipeline", "parse_options", "pipeline", "script"]
