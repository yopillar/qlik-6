"""Doublure de spark_foundation/tables.py : lecture, calcul et ecriture
d'une table."""

from core.datasets import read_input, write_output
from core.steps import run_sql

__all__ = ["read_input", "run_sql", "write_output"]
