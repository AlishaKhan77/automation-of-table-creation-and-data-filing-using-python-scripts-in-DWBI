import pyodbc

# Connect to the SQL Server database
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ALISHA_SHAHID\\ALISHA_KHAN;"  # Update with your server name
    "Database=db0;"  # Replace with your database name
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Create Shippers table if it doesn't exist
cursor.execute('''
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Shippers' AND xtype='U')
CREATE TABLE Shippers (
    ShipperID INT PRIMARY KEY,
    CompanyName VARCHAR(255),
    Phone VARCHAR(24)
);
''')

# List of shippers data to insert
shippers_data = [
    (1, 'Speedy Express', '(503) 555-9831'),
    (2, 'United Package', '(503) 555-3199'),
    (3, 'Federal Shipping', '(503) 555-9931'),
    (4, 'Fast Freight', '(503) 555-2244'),
    (5, 'Express Delivery', '(503) 555-8765'),
    (6, 'Rapid Transport', '(503) 555-6789'),
    (7, 'Global Shipments', '(503) 555-1234'),
    (8, 'Quick Logistics', '(503) 555-5678'),
    (9, 'Parcel Solutions', '(503) 555-2345'),
    (10, 'Premier Carriers', '(503) 555-3456'),
    (11, 'Velocity Shipping', '(503) 555-7890'),
    (12, 'Direct Dispatch', '(503) 555-4567'),
    (13, 'Next Day Delivery', '(503) 555-9101'),
    (14, 'ABC Shipping', '(503) 555-1122'),
    (15, 'Coastal Couriers', '(503) 555-2233'),
    (16, 'Swift Transport', '(503) 555-3344'),
    (17, 'Metro Shippers', '(503) 555-4455'),
    (18, 'Reliable Freight', '(503) 555-5566'),
    (19, 'Eco-Friendly Shipping', '(503) 555-6677'),
    (20, 'On-Time Express', '(503) 555-7788'),
    (21, 'Trusted Couriers', '(503) 555-8899'),
    (22, 'ShipRight', '(503) 555-9900'),
]

# Function to populate Shippers table
def populate_shippers(data):
    for shipper in data:
        try:
            cursor.execute(
                "INSERT INTO Shippers (ShipperID, CompanyName, Phone) VALUES (?, ?, ?)",
                shipper
            )
            print(f"Inserted ShipperID={shipper[0]}, CompanyName={shipper[1]}, Phone={shipper[2]}")
        except Exception as e:
            print(f"Error inserting data for ShipperID={shipper[0]}: {e}")

# Populate the Shippers table
populate_shippers(shippers_data)

# Commit the transactions
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Shippers data inserted successfully!")
