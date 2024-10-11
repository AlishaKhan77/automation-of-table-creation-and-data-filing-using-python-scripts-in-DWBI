import pyodbc

# Define the connection to your SQL Server database
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ALISHA_SHAHID\\ALISHA_KHAN;'  # e.g., 'localhost' or 'SQLServerInstance'
                      'Database=NWBD2;'  # e.g., 'Northwind_DW'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

# Table creation queries with IF NOT EXISTS

# Create Shippers table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Shippers' AND xtype='U')
CREATE TABLE Shippers (
    ShipperID INT PRIMARY KEY,
    CompanyName VARCHAR(255),
    Phone VARCHAR(24)
);
''')

# Create Customers table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Customers' AND xtype='U')
CREATE TABLE Customers (
    CustomerID VARCHAR(50) PRIMARY KEY,
    CompanyName VARCHAR(255),
    ContactName VARCHAR(30),
    ContactTitle VARCHAR(30),
    Address VARCHAR(255),
    City VARCHAR(50),
    Region VARCHAR(50),
    PostalCode VARCHAR(20),
    Country VARCHAR(50),
    Phone VARCHAR(24),
    Fax VARCHAR(24)
);
''')

# Create Suppliers table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Suppliers' AND xtype='U')
CREATE TABLE Suppliers (
    SupplierID INT PRIMARY KEY,
    CompanyName VARCHAR(255),
    ContactName VARCHAR(30),
    ContactTitle VARCHAR(30),
    Address VARCHAR(255),
    City VARCHAR(50),
    Region VARCHAR(50),
    PostalCode VARCHAR(20),
    Country VARCHAR(50),
    Phone VARCHAR(24),
    Fax VARCHAR(24),
    Homepage TEXT
);
''')

# Create Categories table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Categories' AND xtype='U')
CREATE TABLE Categories (
    CategoryID INT PRIMARY KEY,
    CategoryName VARCHAR(255),
    Description TEXT,
    Picture IMAGE
);
''')

# Create Products table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Products' AND xtype='U')
CREATE TABLE Products (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit VARCHAR(255),
    UnitPrice DECIMAL(10, 2),
    UnitsInStock SMALLINT,
    UnitsOnOrder SMALLINT,
    ReorderLevel SMALLINT,
    Discontinued BIT,
    FOREIGN KEY (SupplierID) REFERENCES Suppliers(SupplierID),
    FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);
''')

# Create Orders table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Orders' AND xtype='U')
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY,
    CustomerID VARCHAR(50),
    EmployeeID INT,
    OrderDate DATE,
    RequiredDate DATE,
    ShippedDate DATE,
    ShipVia INT,
    Freight DECIMAL(10, 2),
    ShipName VARCHAR(255),
    ShipAddress VARCHAR(255),
    ShipCity VARCHAR(50),
    ShipRegion VARCHAR(50),
    ShipPostalCode VARCHAR(20),
    ShipCountry VARCHAR(50),
    FOREIGN KEY (CustomerID) REFERENCES Customers(CustomerID),
    FOREIGN KEY (ShipVia) REFERENCES Shippers(ShipperID)
);
''')

# Create OrderDetails table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='OrderDetails' AND xtype='U')
CREATE TABLE OrderDetails (
    OrderID INT,
    ProductID INT,
    UnitPrice DECIMAL(10, 2),
    Quantity SMALLINT,
    Discount DECIMAL(5, 2),
    PRIMARY KEY (OrderID, ProductID),
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);
''')

# Create Employees table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Employees' AND xtype='U')
CREATE TABLE Employees (
    EmployeeID INT PRIMARY KEY,
    LastName VARCHAR(255),
    FirstName VARCHAR(255),
    Title VARCHAR(50),
    TitleOfCourtesy VARCHAR(25),
    BirthDate DATE,
    HireDate DATE,
    Address VARCHAR(255),
    City VARCHAR(50),
    Region VARCHAR(50),
    PostalCode VARCHAR(20),
    Country VARCHAR(50),
    HomePhone VARCHAR(24),
    Extension VARCHAR(4),
    Photo IMAGE,
    Notes TEXT,
    ReportsTo INT,
    PhotoPath VARCHAR(255),
    FOREIGN KEY (ReportsTo) REFERENCES Employees(EmployeeID)
);
''')

# Create Regions table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Regions' AND xtype='U')
CREATE TABLE Regions (
    RegionID INT PRIMARY KEY,
    RegionDescription VARCHAR(50)
);
''')

# Create Territories table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Territories' AND xtype='U')
CREATE TABLE Territories (
    TerritoryID VARCHAR(20) PRIMARY KEY,
    TerritoryDescription VARCHAR(50),
    RegionID INT,
    FOREIGN KEY (RegionID) REFERENCES Regions(RegionID)
);
''')

# Create EmployeeTerritories table
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='EmployeeTerritories' AND xtype='U')
CREATE TABLE EmployeeTerritories (
    EmployeeID INT,
    TerritoryID VARCHAR(20),
    PRIMARY KEY (EmployeeID, TerritoryID),
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID),
    FOREIGN KEY (TerritoryID) REFERENCES Territories(TerritoryID)
);
''')

# Commit the changes
conn.commit()

# Close the connection
conn.close()

print("All tables created successfully (if they didn't exist already)!")
