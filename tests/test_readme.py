from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def test_readme_covers_the_promises():
    text = (ROOT / "README.md").read_text()
    for needle in (
        "Energy-Charts (Fraunhofer ISE), CC BY 4.0",
        "Bundesnetzagentur | SMARD.de",
        "Free Edition",
        "trusted domains",
        "24 hours",
        "GRANT USE CATALOG",
        "DATABRICKS_HOST",
        "Serverless Starter Warehouse",
        "uv run inv ingest",
        "RationisLabsEnergyLakehouse/1.0 (+https://github.com/rationislabs/databricks-data-pipeline-app)",
        "databricks bundle run explorer",
    ):
        assert needle in text, needle
