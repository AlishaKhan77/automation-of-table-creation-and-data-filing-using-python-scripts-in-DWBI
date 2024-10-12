import pandas as pd
import pyodbc

# Define the connection to your SQL Server database
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ALISHA_SHAHID\\ALISHA_KHAN;'  # e.g., 'localhost' or 'SQLServerInstance'
                      'Database=lab03DB;'  # e.g., 'Northwind_DW'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()


# Define the path to your CSV file
file_path = r'C:\Users\alish\Desktop\DW LAB\Lab_03_DW&BI\Lab_03_DW&BI\northwind\Categories.csv'

# Read CSV file, without headers if necessary
df = pd.read_csv(file_path)

# Rename columns to match the database schema
df.columns = ['CategoryID', 'CategoryName', 'Description']

# Loop through each row and insert data
for index, row in df.iterrows():
    # Check if the CategoryID already exists
    cursor.execute('''
    IF NOT EXISTS (SELECT * FROM Categories WHERE CategoryID = ?)
    INSERT INTO Categories (CategoryID, CategoryName, Description) 
    VALUES (?, ?, ?)
    ''', row['CategoryID'], row['CategoryID'], row['CategoryName'], row['Description'])

# Commit and close the connection
conn.commit()


print("Categories table populated successfully without duplicates!")


# Read Customers CSV
df_customers = pd.read_csv(file_path)

# Rename columns to match database
df_customers.columns = ['CustomerID', 'CustomerName', 'ContactName', 'Address', 'City', 'PostalCode', 'Country']

# Insert or update Customers table
for index, row in df_customers.iterrows():
    cursor.execute('''
    IF EXISTS (SELECT * FROM Customers WHERE CustomerID = ?)
    UPDATE Customers
    SET CustomerName = ?, ContactName = ?, Address = ?, City = ?, PostalCode = ?, Country = ?
    WHERE CustomerID = ?
    ELSE
    INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', row['CustomerID'], row['CustomerName'], row['ContactName'], row['Address'], row['City'], row['PostalCode'], row['Country'],
         row['CustomerID'], row['CustomerName'], row['ContactName'], row['Address'], row['City'], row['PostalCode'], row['Country'])

conn.commit()


print("Customers table populated successfully without duplicates!")
# Read OrderDetails CSV
df_orderdetails = pd.read_csv(file_path)

# Rename columns to match database
df_orderdetails.columns = ['OrderDetailID', 'OrderID', 'ProductID', 'Quantity']

# Insert or update OrderDetails table
for index, row in df_orderdetails.iterrows():
    cursor.execute('''
    IF EXISTS (SELECT * FROM OrderDetails WHERE OrderDetailID = ?)
    UPDATE OrderDetails
    SET OrderID = ?, ProductID = ?, Quantity = ?
    WHERE OrderDetailID = ?
    ELSE
    INSERT INTO OrderDetails (OrderDetailID, OrderID, ProductID, Quantity)
    VALUES (?, ?, ?, ?)
    ''', row['OrderDetailID'], row['OrderID'], row['ProductID'], row['Quantity'], row['OrderDetailID'],
         row['OrderDetailID'], row['OrderID'], row['ProductID'], row['Quantity'])

conn.commit()



print("OrderDetails table populated successfully without duplicates!")




# Read Orders CSV
df_orders = pd.read_csv(file_path)

# Rename columns to match database
df_orders.columns = ['OrderID', 'CustomerID', 'EmployeeID', 'OrderDate', 'ShipperID']

# Insert or update Orders table
for index, row in df_orders.iterrows():
    cursor.execute('''
    IF EXISTS (SELECT * FROM Orders WHERE OrderID = ?)
    UPDATE Orders
    SET CustomerID = ?, EmployeeID = ?, OrderDate = ?, ShipperID = ?
    WHERE OrderID = ?
    ELSE
    INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID)
    VALUES (?, ?, ?, ?, ?)
    ''', row['OrderID'], row['CustomerID'], row['EmployeeID'], row['OrderDate'], row['ShipperID'], row['OrderID'],
         row['OrderID'], row['CustomerID'], row['EmployeeID'], row['OrderDate'], row['ShipperID'])

conn.commit()



print("Orders table populated successfully without duplicates!")



# Read Products CSV
df_products = pd.read_csv(file_path)

# Rename columns to match database
df_products.columns = ['ProductID', 'ProductName', 'SupplierID', 'CategoryID', 'Unit', 'Price']

# Insert or update Products table
for index, row in df_products.iterrows():
    cursor.execute('''
    IF EXISTS (SELECT * FROM Products WHERE ProductID = ?)
    UPDATE Products
    SET ProductName = ?, SupplierID = ?, CategoryID = ?, Unit = ?, Price = ?
    WHERE ProductID = ?
    ELSE
    INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, Unit, Price)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', row['ProductID'], row['ProductName'], row['SupplierID'], row['CategoryID'], row['Unit'], row['Price'], row['ProductID'],
         row['ProductID'], row['ProductName'], row['SupplierID'], row['CategoryID'], row['Unit'], row['Price'])

conn.commit()



print("Products table populated successfully without duplicates!")


# Read Shippers CSV
df_shippers = pd.read_csv(file_path)

# Rename columns to match database
df_shippers.columns = ['ShipperID', 'CompanyName', 'Phone']

# Insert or update Shippers table
for index, row in df_shippers.iterrows():
    cursor.execute('''
    IF EXISTS (SELECT * FROM Shippers WHERE ShipperID = ?)
    UPDATE Shippers
    SET CompanyName = ?, Phone = ?
    WHERE ShipperID = ?
    ELSE
    INSERT INTO Shippers (ShipperID, CompanyName, Phone)
    VALUES (?, ?, ?)
    ''', row['ShipperID'], row['CompanyName'], row['Phone'], row['ShipperID'],
         row['ShipperID'], row['CompanyName'], row['Phone'])

conn.commit()



print("Shippers table populated successfully without duplicates!")



# Read Suppliers CSV
df_suppliers = pd.read_csv(file_path)

# Rename columns to match database
df_suppliers.columns = ['SupplierID', 'SupplierName', 'ContactName', 'Address', 'City', 'PostalCode', 'Country', 'Phone']

# Insert or update Suppliers table
for index, row in df_suppliers.iterrows():
    cursor.execute('''
    IF EXISTS (SELECT * FROM Suppliers WHERE SupplierID = ?)
    UPDATE Suppliers
    SET SupplierName = ?, ContactName = ?, Address = ?, City = ?, PostalCode = ?, Country = ?, Phone = ?
    WHERE SupplierID = ?
    ELSE
    INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Address, City, PostalCode, Country, Phone)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', row['SupplierID'], row['SupplierName'], row['ContactName'], row['Address'], row['City'], row['PostalCode'], row['Country'], row['Phone'], row['SupplierID'],
         row['SupplierID'], row['SupplierName'], row['ContactName'], row['Address'], row['City'], row['PostalCode'], row['Country'], row['Phone'])

conn.commit()



print("Suppliers table populated successfully without duplicates!")





# Read Customers CSV
df_customers = pd.read_csv(file_path)

# Rename columns to match database
df_customers.columns = ['CustomerID', 'CustomerName', 'ContactName', 'Address', 'City', 'PostalCode', 'Country']

# Insert or update Customers table
for index, row in df_customers.iterrows():
    cursor.execute('''
    IF EXISTS (SELECT * FROM Customers WHERE CustomerID = ?)
    UPDATE Customers
    SET CustomerName = ?, ContactName = ?, Address = ?, City = ?, PostalCode = ?, Country = ?
    WHERE CustomerID = ?
    ELSE
    INSERT INTO Customers (CustomerID, CustomerName, ContactName, Address, City, PostalCode, Country)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', row['CustomerID'], row['CustomerName'], row['ContactName'], row['Address'], row['City'], row['PostalCode'], row['Country'],
         row['CustomerID'], row['CustomerName'], row['ContactName'], row['Address'], row['City'], row['PostalCode'], row['Country'])

conn.commit()
conn.close()


print("Customers table populated successfully without duplicates!")

