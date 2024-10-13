import pyodbc

# Connect to the SQL Server database
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ALISHA_SHAHID\\ALISHA_KHAN;"  # Update with your server name
    "Database=db0;"  # Replace with your database name
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Create Suppliers table if it doesn't exist
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Suppliers' AND xtype='U')
CREATE TABLE Suppliers (
    SupplierID INT PRIMARY KEY,
    CompanyName VARCHAR(255),
    ContactName VARCHAR(30),
    ContactTitle VARCHAR(30),  -- You may not have data for this, can be left NULL
    Address VARCHAR(255),
    City VARCHAR(50),
    Region VARCHAR(50),        -- Can be left NULL as well
    PostalCode VARCHAR(20),
    Country VARCHAR(50),
    Phone VARCHAR(24),
    Fax VARCHAR(24),           -- Optional, can be left NULL
    Homepage TEXT              -- Optional, can be left NULL
);
''')

# List of suppliers data to insert
suppliers_data = [
    (1, 'Exotic Liquids', 'Charlotte Cooper', None, '49 Gilbert St.', 'London', None, 'EC1 4SD', 'UK', '(171) 555-2222', None, None),
    (2, 'New Orleans Cajun Delights', 'Shelley Burke', None, 'P.O. Box 78934', 'New Orleans', None, '70117', 'USA', '(100) 555-4822', None, None),
    (3, "Grandma Kelly's Homestead", 'Regina Murphy', None, '707 Oxford Rd', 'Ann Arbor', None, '48104', 'USA', '(313) 555-5735', None, None),
    (4, 'Tokyo Traders', 'Yoshi Nagase', None, '9-8 Sekimai Musashino-shi', 'Tokyo', None, '100', 'Japan', '(03) 3555-5011', None, None),
    (5, "Cooperativa de Quesos 'Las Cabras'", 'Antonio del Valle', None, 'Calle del Rosal 4', 'Oviedo', None, '33007', 'Spain', '(98) 598 76 54', None, None),
    (6, 'African Spice Traders', 'Ayo Adeyemi', None, '14 Market Street', 'Lagos', None, '10101', 'Nigeria', '(234) 1 234 5678', None, None),
    (7, 'Blue Mountain Coffee', 'James Kingston', None, '254 Blue Mountain Rd', 'Kingston', None, '12000', 'Jamaica', '(876) 555-1234', None, None),
    (8, 'Dolce Sweets', 'Carla Ferraro', None, 'Viale della Libertà 10', 'Rome', None, '192', 'Italy', '(39) 06 555 7890', None, None),
    (9, 'Fruitful Farms', 'Katherine Lee', None, '123 Orchard Way', 'San Francisco', None, '94107', 'USA', '(415) 555-3456', None, None),
    (10, 'Herbs and Spices', 'Satish Gupta', None, '45 Spice Street', 'Mumbai', None, '400001', 'India', '(91) 22 5555 8970', None, None),
    (11, 'Ice Cream Dream', 'Laura Kent', None, '55 Creamery Lane', 'Chicago', None, '60601', 'USA', '(312) 555-6789', None, None),
    (12, 'Jasmine Tea Co.', 'Li Wei', None, '33 Tea Road', 'Hangzhou', None, '310000', 'China', '(86) 571 555 7788', None, None),
    (13, 'Kingfish Seafood', 'Tony Chen', None, '4 Fisherman\'s Wharf', 'New Orleans', None, '70116', 'USA', '(504) 555-8899', None, None),
    (14, 'Luxury Chocolates', 'Nilay Tiwari', None, '10 Sweet Street', 'Mumbai', None, '400099', 'India', '(91) 22 5555 1234', None, None),
    (15, 'Mediterranean Bites', 'Elena Rossi', None, '20 Via dei Pini', 'Milan', None, '20100', 'Italy', '(39) 02 555 0101', None, None),
    (16, 'Natural Nut Company', 'Bubbles Johnson', None, '76 Nut Street', 'Los Angeles', None, '90001', 'USA', '(213) 555-0011', None, None),
    (17, 'Old World Olive Oil', 'Josie Cartwright', None, '212 Olive Ave', 'Barcelona', None, '8001', 'Spain', '(34) 93 555 3344', None, None),
    (18, 'Peanut Butter Paradise', 'Jimmy Chen', None, '8 Nutty Way', 'Orlando', None, '32801', 'USA', '(407) 555-7888', None, None),
    (19, 'Sweet Honey Farms', 'Bobby Bee', None, '89 Honeycomb Dr', 'Memphis', None, '38103', 'USA', '(901) 555-4567', None, None),
    (20, 'Urban Artisan Bakery', 'Maya Singh', None, '404 Artisan Row', 'London', None, 'SE1 1PL', 'UK', '(44) 20 5555 8910', None, None),
    (21, 'Vintage Vineyards', 'Rafael Garcia', None, '12 Vineyard Rd', 'Valencia', None, '46001', 'Spain', '(34) 96 555 2233', None, None),
    (22, 'Yummy Cupcakes', 'Isabella Martinez', None, '75 Dessert Blvd', 'San Diego', None, '92101', 'USA', '(619) 555-6611', None, None),
]

# Function to populate Suppliers table
def populate_suppliers(data):
    for supplier in data:
        try:
            cursor.execute(
                "INSERT INTO Suppliers (SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, Homepage) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                supplier
            )
            print(f"Inserted SupplierID={supplier[0]}, CompanyName={supplier[1]}")
        except Exception as e:
            print(f"Error inserting data for SupplierID={supplier[0]}: {e}")

# Populate the Suppliers table
populate_suppliers(suppliers_data)

# Commit the transactions
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Suppliers data inserted successfully!")
