import pyodbc
from faker import Faker
import random
from datetime import datetime, timedelta

# Create a Faker instance
fake = Faker()

# Connect to the SQL Server database
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ALISHA_SHAHID\\ALISHA_KHAN;"  # Update with your server name
    "Database=db0;"  # Replace with your database name
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Function to populate Orders table with fake data
def populate_orders(num_records):
    for _ in range(num_records):
        OrderID = _ + 10248  # Starting OrderID from your given data
        CustomerID = fake.random_int(min=1, max=100, step=1)  # Assuming CustomerID is between 1 and 100
        EmployeeID = fake.random_int(min=1, max=10, step=1)  # Assuming EmployeeID is between 1 and 10
        OrderDate = fake.date_between(start_date='-1y', end_date='today')
        RequiredDate = OrderDate + timedelta(days=random.randint(1, 30))  # Random required date
        ShippedDate = OrderDate + timedelta(days=random.randint(1, 10))  # Random shipped date
        ShipVia = random.choice([1, 2, 3])  # Assuming valid ShipperIDs
        Freight = round(fake.random_number(digits=5) / 100, 2)  # Random freight cost
        ShipName = fake.company()
        ShipAddress = fake.street_address()
        ShipCity = fake.city()
        ShipRegion = fake.state()
        ShipPostalCode = fake.zipcode()
        ShipCountry = fake.country()

        try:
            cursor.execute('''
                INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry))
            print(f"Inserted OrderID={OrderID} for CustomerID={CustomerID}")
        except Exception as e:
            print(f"Error inserting data for OrderID={OrderID}: {e}")

# Call the function to populate 100 records
populate_orders(100)

# Commit the transactions
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("Orders data inserted successfully!")
