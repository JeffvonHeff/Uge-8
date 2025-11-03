"""Tiny transform helpers for the ETL demo."""

from __future__ import annotations

from typing import Dict

import pandas as pd


def prepare_relational_tables(raw: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Clean and align the raw CSV files with the PostgreSQL schema."""

    tables: Dict[str, pd.DataFrame] = {}

    tables["brands"] = raw["brands"][["brand_id", "brand_name"]].copy()
    tables["brands"]["brand_id"] = tables["brands"]["brand_id"].astype(int)

    tables["categories"] = raw["categories"][["category_id", "category_name"]].copy()
    tables["categories"]["category_id"] = tables["categories"]["category_id"].astype(
        int
    )

    stores = raw["stores"].copy()
    stores = stores.rename(columns={"name": "store_name"})
    stores.insert(0, "store_id", range(1, len(stores) + 1))
    tables["stores"] = stores[
        [
            "store_id",
            "store_name",
            "phone",
            "email",
            "street",
            "city",
            "state",
            "zip_code",
        ]
    ].copy()
    tables["stores"]["store_id"] = tables["stores"]["store_id"].astype(int)

    store_lookup = dict(
        zip(tables["stores"]["store_name"], tables["stores"]["store_id"])
    )

    customers = raw["customers"].copy()
    tables["customers"] = customers[
        [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "street",
            "city",
            "state",
            "zip_code",
        ]
    ].copy()
    tables["customers"]["customer_id"] = tables["customers"]["customer_id"].astype(int)

    products = raw["products"].copy()
    tables["products"] = products[
        [
            "product_id",
            "product_name",
            "brand_id",
            "category_id",
            "model_year",
            "list_price",
        ]
    ].copy()
    tables["products"]["product_id"] = tables["products"]["product_id"].astype(int)
    tables["products"]["brand_id"] = tables["products"]["brand_id"].astype(int)
    tables["products"]["category_id"] = tables["products"]["category_id"].astype(int)
    tables["products"]["model_year"] = tables["products"]["model_year"].astype(int)
    tables["products"]["list_price"] = tables["products"]["list_price"].astype(float)

    staffs = raw["staffs"].copy()
    staffs = staffs.rename(
        columns={
            "name": "first_name",
            "last_name": "last_name",
            "store_name": "store_name",
        }
    )
    staffs.insert(0, "staff_id", range(1, len(staffs) + 1))
    staffs["manager_id"] = pd.to_numeric(staffs["manager_id"], errors="coerce")
    staffs["active"] = staffs["active"].fillna(0).astype(int).astype(bool)
    staffs["store_id"] = staffs["store_name"].map(store_lookup)
    tables["staffs"] = staffs[
        [
            "staff_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "active",
            "street",
            "store_id",
            "manager_id",
        ]
    ].copy()
    tables["staffs"]["manager_id"] = tables["staffs"]["manager_id"].astype("Int64")

    staff_lookup = dict(
        zip(tables["staffs"]["first_name"], tables["staffs"]["staff_id"])
    )

    stocks = raw["stocks"].copy()
    stocks["store_id"] = stocks["store_name"].map(store_lookup)
    tables["stocks"] = stocks[["store_id", "product_id", "quantity"]].copy()
    tables["stocks"]["store_id"] = tables["stocks"]["store_id"].astype(int)
    tables["stocks"]["product_id"] = tables["stocks"]["product_id"].astype(int)
    tables["stocks"]["quantity"] = tables["stocks"]["quantity"].astype(int)

    orders = raw["orders"].copy()
    orders["order_date"] = pd.to_datetime(
        orders["order_date"], format="%d/%m/%Y", errors="coerce"
    )
    orders["required_date"] = pd.to_datetime(
        orders["required_date"], format="%d/%m/%Y", errors="coerce"
    )
    orders["shipped_date"] = pd.to_datetime(
        orders["shipped_date"], format="%d/%m/%Y", errors="coerce"
    )
    orders["store_id"] = orders["store"].map(store_lookup)
    orders["staff_id"] = orders["staff_name"].map(staff_lookup)
    orders["order_status"] = orders["order_status"].astype(int)
    orders["order_id"] = orders["order_id"].astype(int)
    orders["customer_id"] = orders["customer_id"].astype(int)
    orders["store_id"] = orders["store_id"].astype(int)
    orders["staff_id"] = orders["staff_id"].astype(int)
    tables["orders"] = orders[
        [
            "order_id",
            "customer_id",
            "store_id",
            "staff_id",
            "order_status",
            "order_date",
            "required_date",
            "shipped_date",
        ]
    ].copy()

    items = raw["order_items"].copy()
    tables["order_items"] = items[
        [
            "order_id",
            "item_id",
            "product_id",
            "quantity",
            "list_price",
            "discount",
        ]
    ].copy()
    tables["order_items"]["order_id"] = tables["order_items"]["order_id"].astype(int)
    tables["order_items"]["item_id"] = tables["order_items"]["item_id"].astype(int)
    tables["order_items"]["product_id"] = tables["order_items"]["product_id"].astype(
        int
    )
    tables["order_items"]["quantity"] = tables["order_items"]["quantity"].astype(int)
    tables["order_items"]["list_price"] = tables["order_items"]["list_price"].astype(
        float
    )
    tables["order_items"]["discount"] = tables["order_items"]["discount"].astype(float)

    return tables


def build_order_summary(tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create a summary table ready for loading into PostgreSQL."""

    orders = tables["orders"].copy()
    items = tables["order_items"].copy()
    customers = tables["customers"].copy()

    # Work out how much each order is worth.
    items["line_total"] = (
        items["quantity"].astype(float)
        * items["list_price"].astype(float)
        * (1 - items["discount"].astype(float))
    )
    totals = (
        items.groupby("order_id", as_index=False)["line_total"]
        .sum()
        .rename({"line_total": "order_total"}, axis=1)
    )

    # Add the customer names to the orders table.
    customers["customer_name"] = customers["first_name"] + " " + customers["last_name"]
    customer_details = customers[["customer_id", "customer_name"]]

    summary = (
        orders.merge(totals, on="order_id", how="left")
        .merge(customer_details, on="customer_id", how="left")
        .fillna({"order_total": 0})
    )

    summary["order_total"] = summary["order_total"].astype(float)

    return summary[
        ["order_id", "order_date", "customer_id", "customer_name", "order_total"]
    ]


__all__ = ["build_order_summary", "prepare_relational_tables"]
