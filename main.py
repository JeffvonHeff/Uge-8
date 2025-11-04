"""Interactive entry-point for running the ETL pipeline or database utilities."""

from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Sequence, Set

from getpass import getpass
import psycopg2
from psycopg2.extensions import connection as PGConnection

from Extract import extract_data
from Transform import build_order_summary, prepare_relational_tables
from Load import create_connection, load_core_tables, load_order_summary


# CLI permissions are a simplified view of what each database role can do in the
# interactive helper. Actual database privileges are managed in schema.sql.
ROLE_PERMISSIONS: Dict[str, Set[str]] = {
    "admin": {"load_data", "create", "read", "update", "delete"},
    "customer": {"read"},
    "warehouse": {"read", "update"},
    "analytics": {"read"},
    "store": {"read"},
    "hr": {"read"},
}

ROLE_CREDENTIALS: Dict[str, str] = {
    "admin": "admin123",
    "customer": "customer123",
    "warehouse": "warehouse123",
    "analytics": "analytics123",
    "store": "store123",
    "hr": "hr123",
}

ROLE_DATA_ACCESS: Dict[str, Sequence[str]] = {
    "admin": ("All tables",),
    "customer": (
        "Customers – own profile",
        "Orders – own orders",
        "Order items – items from own orders",
    ),
    "warehouse": (
        "Orders",
        "Stocks",
        "Products",
        "Brands",
        "Categories",
    ),
    "analytics": (
        "Orders",
        "Order items",
        "Customers",
        "Brands",
        "Products",
        "Categories",
    ),
    "store": (
        "Orders – for the store",
        "Customers",
        "Stocks – for the store",
        "Staffs – for the store",
    ),
    "hr": (
        "Staffs",
        "Stores",
    ),
}

ACTION_DESCRIPTIONS: Dict[str, str] = {
    "load_data": "Run the full ETL pipeline (extract, transform, load).",
    "create": "Create a new customer record.",
    "read": "Read customer records.",
    "update": "Update a customer record.",
    "delete": "Delete a customer record.",
    "exit": "Exit the program.",
}

CUSTOMER_FIELDS: List[str] = [
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


def run_pipeline() -> None:
    """Prompt for a role and allow role-specific operations."""

    print("Welcome to the ETL and customer management tool.")
    role = _login()
    permissions = ROLE_PERMISSIONS[role]
    allowed_actions = ", ".join(sorted(permissions | {"exit"}))
    print(f"Logged in as '{role}'. Available actions: {allowed_actions}.")

    accessible_data = ROLE_DATA_ACCESS.get(role, ())
    if accessible_data:
        print("Data access granted to:")
        for item in accessible_data:
            print(f" - {item}")

    action_handlers: Dict[str, Callable[[], None]] = {
        "load_data": _handle_load_data,
        "create": lambda: _with_connection(_handle_create_customer),
        "read": lambda: _with_connection(
            lambda connection: _handle_read_customer(connection, role)
        ),
        "update": lambda: _with_connection(_handle_update_customer),
        "delete": lambda: _with_connection(_handle_delete_customer),
    }

    while True:
        action = _prompt_for_action(permissions)
        if action == "exit":
            print("Goodbye!")
            return
        handler = action_handlers[action]
        handler()


def _login() -> str:
    attempts_remaining = 3
    valid_roles = ", ".join(sorted(ROLE_PERMISSIONS))
    while attempts_remaining:
        role = input(f"Enter your role ({valid_roles}): ").strip().lower()
        password = getpass("Enter password: ")
        if role in ROLE_CREDENTIALS and ROLE_CREDENTIALS[role] == password:
            return role
        attempts_remaining -= 1
        if attempts_remaining == 0:
            raise SystemExit("Too many failed login attempts. Exiting.")
        print(
            "Invalid credentials. Attempts remaining:",
            attempts_remaining,
        )


def _prompt_for_action(permissions: Set[str]) -> str:
    allowed_actions = sorted(permissions | {"exit"})
    print("\nWhat would you like to do?")
    for name in allowed_actions:
        description = ACTION_DESCRIPTIONS.get(name, "")
        print(f" - {name}: {description}")

    while True:
        action = input("Select an action: ").strip().lower()
        if action in allowed_actions:
            return action
        print("Invalid action. Please choose one of:", ", ".join(allowed_actions))


def _handle_load_data() -> None:
    confirmation = (
        input("Run the full ETL pipeline and load data into the database? (yes/no): ")
        .strip()
        .lower()
    )
    if confirmation not in {"y", "yes"}:
        print("Skipping data load.")
        return

    print("Step 1: Extracting the CSV files")
    data = extract_data()

    print("Step 2: Preparing relational tables and the order summary")
    tables = prepare_relational_tables(data)
    summary = build_order_summary(tables)

    print("Step 3: Saving everything to PostgreSQL...")
    _with_connection(lambda connection: _load_all(connection, tables, summary))
    print("All done! You can now explore the order_summary table in PostgreSQL.")


def _load_all(connection: PGConnection, tables, summary) -> None:
    load_core_tables(connection, tables)
    load_order_summary(connection, summary)


def _with_connection(func: Callable[[PGConnection], None]) -> None:
    try:
        connection = create_connection()
    except psycopg2.Error as exc:  # pragma: no cover - defensive
        print(f"Could not connect to the database: {exc}")
        return

    try:
        func(connection)
    finally:
        connection.close()


def _handle_create_customer(connection: PGConnection) -> None:
    print("Creating a new customer. Leave a field blank to cancel.")
    values = {}
    for field in CUSTOMER_FIELDS:
        value = input(f"{field.replace('_', ' ').title()}: ").strip()
        if not value:
            print("Creation cancelled.")
            return
        if field == "customer_id":
            try:
                values[field] = int(value)
            except ValueError:
                print("Customer ID must be an integer. Creation cancelled.")
                return
        else:
            values[field] = value

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO customers (
                        customer_id, first_name, last_name, email, phone,
                        street, city, state, zip_code
                    ) VALUES (%(customer_id)s, %(first_name)s, %(last_name)s,
                        %(email)s, %(phone)s, %(street)s, %(city)s,
                        %(state)s, %(zip_code)s)
                    """,
                    values,
                )
        print("Customer created successfully.")
    except psycopg2.Error as exc:
        print(f"Failed to create customer: {exc}")


def _handle_read_customer(connection: PGConnection, role: str | None = None) -> None:
    if role == "customer":
        _handle_customer_self_service(connection)
        return

    identifier = input(
        "Enter a customer ID to look up or press enter to list all customers: "
    ).strip()
    query = "SELECT customer_id, first_name, last_name, email FROM customers"
    params: Iterable[str | int] = ()
    if identifier:
        try:
            customer_id = int(identifier)
        except ValueError:
            print("Customer ID must be an integer.")
            return
        query += " WHERE customer_id = %s"
        params = (customer_id,)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
    except psycopg2.Error as exc:
        print(f"Failed to read customer data: {exc}")
        return

    if not rows:
        print("No customer records found.")
        return

    print("Customer records:")
    for row in rows:
        cust_id, first, last, email = row
        print(f" - {cust_id}: {first} {last} <{email}>")


def _handle_customer_self_service(connection: PGConnection) -> None:
    identifier = input("Enter your customer ID to view your data: ").strip()
    if not identifier:
        print("A customer ID is required to view data.")
        return

    try:
        customer_id = int(identifier)
    except ValueError:
        print("Customer ID must be an integer.")
        return

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT customer_id, first_name, last_name, email, phone,
                       street, city, state, zip_code
                FROM customers
                WHERE customer_id = %s
                """,
                (customer_id,),
            )
            customer = cursor.fetchone()
            if customer is None:
                print("No data found for that customer ID.")
                return

            cursor.execute(
                """
                SELECT order_id, order_status, order_date, required_date, shipped_date
                FROM orders
                WHERE customer_id = %s
                ORDER BY order_date
                """,
                (customer_id,),
            )
            orders = cursor.fetchall()

            cursor.execute(
                """
                SELECT oi.order_id, oi.item_id, p.product_name, oi.quantity,
                       oi.list_price, oi.discount
                FROM order_items AS oi
                JOIN orders AS o ON o.order_id = oi.order_id
                JOIN products AS p ON p.product_id = oi.product_id
                WHERE o.customer_id = %s
                ORDER BY oi.order_id, oi.item_id
                """,
                (customer_id,),
            )
            order_items = cursor.fetchall()
    except psycopg2.Error as exc:
        print(f"Failed to read customer data: {exc}")
        return

    print("\nCustomer profile:")
    (
        cust_id,
        first_name,
        last_name,
        email,
        phone,
        street,
        city,
        state,
        zip_code,
    ) = customer
    print(f" - ID: {cust_id}")
    print(f" - Name: {first_name} {last_name}")
    print(f" - Email: {email}")
    if phone:
        print(f" - Phone: {phone}")
    print(f" - Address: {street}, {city}, {state} {zip_code}")

    if not orders:
        print("\nNo orders found for this customer.")
        return

    items_by_order: Dict[int, List[tuple]] = {}
    for item in order_items:
        order_id = item[0]
        items_by_order.setdefault(order_id, []).append(item)

    print("\nOrders:")
    for order in orders:
        order_id, status, order_date, required_date, shipped_date = order
        print(f" - Order {order_id} (status {status})")
        print(f"   Placed: {order_date}  Required: {required_date}")
        if shipped_date:
            print(f"   Shipped: {shipped_date}")
        order_items_list = items_by_order.get(order_id, [])
        if not order_items_list:
            continue
        print("   Items:")
        for (_, item_id, product_name, quantity, list_price, discount) in order_items_list:
            print(
                "     "
                f"#{item_id} {product_name} - qty {quantity}, price {list_price},"
                f" discount {discount}"
            )


def _handle_update_customer(connection: PGConnection) -> None:
    customer_id_input = input("Enter the customer ID to update: ").strip()
    if not customer_id_input:
        print("Update cancelled.")
        return
    try:
        customer_id = int(customer_id_input)
    except ValueError:
        print("Customer ID must be an integer. Update cancelled.")
        return

    updatable_fields = [field for field in CUSTOMER_FIELDS if field != "customer_id"]
    print("Which field would you like to update?")
    for field in updatable_fields:
        print(f" - {field}")

    field_name = input("Field name: ").strip().lower()
    if field_name not in updatable_fields:
        print("Invalid field. Update cancelled.")
        return

    new_value = input("New value: ").strip()
    if not new_value:
        print("No value provided. Update cancelled.")
        return

    sql = f"UPDATE customers SET {field_name} = %s WHERE customer_id = %s"
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, (new_value, customer_id))
                if cursor.rowcount == 0:
                    print("Customer not found.")
                    return
        print("Customer updated successfully.")
    except psycopg2.Error as exc:
        print(f"Failed to update customer: {exc}")


def _handle_delete_customer(connection: PGConnection) -> None:
    customer_id_input = input("Enter the customer ID to delete: ").strip()
    if not customer_id_input:
        print("Delete cancelled.")
        return

    try:
        customer_id = int(customer_id_input)
    except ValueError:
        print("Customer ID must be an integer. Delete cancelled.")
        return

    confirmation = input(f"Delete customer {customer_id}? (yes/no): ").strip().lower()
    if confirmation not in {"y", "yes"}:
        print("Delete cancelled.")
        return

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM customers WHERE customer_id = %s", (customer_id,)
                )
                if cursor.rowcount == 0:
                    print("Customer not found.")
                    return
        print("Customer deleted successfully.")
    except psycopg2.Error as exc:
        print(f"Failed to delete customer: {exc}")


if __name__ == "__main__":
    run_pipeline()
