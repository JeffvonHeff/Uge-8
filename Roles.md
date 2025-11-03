Principles
----------
**Least privilege**

Cybersecurity concept that mandates granting users and processes only the minimum access and permissions necessary to perform their required tasks

**Seperation of duties**

Dividing tasks and permission among mutilple individuals to prevent fraid and error


All Roles listed
================
- Admin
- Customer
- Warehouse
- Analytics
- Store
- HR



# Admin
All privileges in the database

- Create
- Read
- Update
- Delete

Can see everything


- All

# Customer
Limited privileges
- Read

Only see orders and own data in customers   

From customer id
- Orders (only for id)
- Customers (only own id)
- Order_items (only own order)

# Warehouse
All privileges for accessed area
- Create
- Read
- Update
- Delete

See data for each store
- Orders
- Stocks
- Products
- Brands
- Categories


# Analytics
See all data regarding sales

Extract data for analysis
- Orders
- Order_items
- Customers
- Brands
- Products
- Categories

# Store
Only see data for own store
- Orders (only for own name)
- Customers
- Stocks (only for own name)
- Staff (only for own name)

# HR
Only see internal data
- Staff
- Store





