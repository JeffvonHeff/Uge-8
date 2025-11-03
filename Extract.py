"""Very small helper that reads the raw CSV files for the ETL demo."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

# The CSV files already live in the repository, so we only need to read them.
CSV_FILES: Dict[str, Path] = {
    "orders": Path("orders.csv"),
    "order_items": Path("order_items.csv"),
    "customers": Path("customers.csv"),
    "brands": Path("Data opsætning") / "Data CSV" / "brands.csv",
    "categories": Path("Data opsætning") / "Data CSV" / "categories.csv",
    "products": Path("Data opsætning") / "Data CSV" / "products.csv",
    "stores": Path("Data opsætning") / "Data CSV" / "stores.csv",
    "staffs": Path("Data opsætning") / "Data CSV" / "staffs.csv",
    "stocks": Path("Data opsætning") / "Data CSV" / "stocks.csv",
}


def extract_data(base_path: Path | str = ".") -> Dict[str, pd.DataFrame]:
    """Load every CSV file into a pandas DataFrame.

    Parameters
    ----------
    base_path:
        Folder that contains the CSV files. The default value (``"."``)
        points to the project root directory.

    Returns
    -------
    Dict[str, pandas.DataFrame]
        A dictionary keyed by table name. It contains the API sourced data
        (``orders``, ``order_items``, ``customers``) as well as the supporting
        lookup CSV files required by the relational schema.
    """

    data: Dict[str, pd.DataFrame] = {}
    base = Path(base_path)
    for name, filename in CSV_FILES.items():
        csv_path = base / filename
        data[name] = pd.read_csv(csv_path, na_values=["NULL"])
        print(f"Read {csv_path}")
    return data


__all__ = ["extract_data", "CSV_FILES"]
