# Validation

Before publishing this branch, the framework was validated locally with:

```text
python -m py_compile scripts/run_monthly_alpha_research.py src/graph_alpha_lab/*.py
python -m pytest -q
python scripts/run_monthly_alpha_research.py --help
```

Result:

```text
4 passed
CLI help rendered successfully
```

CI repeats compilation, tests and CLI validation on Ubuntu and Windows using Python 3.11 and 3.13.
