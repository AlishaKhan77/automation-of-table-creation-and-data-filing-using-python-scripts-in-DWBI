import pyodbc

# Connect to the SQL Server database
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ALISHA_SHAHID\\ALISHA_KHAN;"  # Update with your server name
    "Database=db0;"  # Replace with your database name
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Create Products table if it doesn't exist
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Products' AND xtype='U')
CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit VARCHAR(255),
    UnitPrice DECIMAL(10, 2),
    UnitsInStock SMALLINT DEFAULT 0,
    UnitsOnOrder SMALLINT DEFAULT 0,
    ReorderLevel SMALLINT DEFAULT 0,
    Discontinued BIT DEFAULT 0,
    FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID),
    FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);
''')

# List of products data to insert
products_data = [
    (1, 'Chai', 1, 1, '10 boxes x 20 bags', 18.00, 39, 0, 10, 0),
    (2, 'Chang', 1, 1, '24 - 12 oz bottles', 19.00, 17, 40, 25, 0),
    (3, 'Aniseed Syrup', 1, 2, '12 - 550 ml bottles', 10.00, 13, 70, 25, 0),
    (4, 'Chef Anton\'s Cajun Seasoning', 2, 2, '48 - 6 oz jars', 22.00, 53, 0, 0, 0),
    (5, 'Chef Anton\'s Gumbo Mix', 2, 2, '36 boxes', 21.35, 0, 0, 0, 1),
    (6, 'Grandma\'s Boysenberry Spread', 3, 2, '12 - 8 oz jars', 25.00, 120, 0, 0, 0),
    (7, 'Uncle Bob\'s Organic Dried Pears', 3, 7, '12 - 1 lb pkgs.', 30.00, 15, 0, 10, 0),
    (8, 'Northwoods Cranberry Sauce', 3, 8, '12 - 12 oz jars', 40.00, 6, 0, 0, 0),
    (9, 'Mishi Kobe Niku', 4, 6, '18 - 500 g pkgs.', 97.00, 29, 0, 0, 0),
    (10, 'Ikura', 4, 8, '12 - 200 g jars', 31.00, 31, 0, 0, 0),
    # Add more product tuples as needed
]

# Function to populate Products table
def populate_products(data):
    for product in data:
        try:
            cursor.execute(
                "INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                product
            )
            print(f"Inserted ProductID={product[0]}, ProductName={product[1]}")
        except Exception as e:
            print(f"Error inserting data for ProductID={product[0]}: {e}")

# Populate the Products table
populate_products(products_data)

# Commit the transactions
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Products data inserted successfully!")
