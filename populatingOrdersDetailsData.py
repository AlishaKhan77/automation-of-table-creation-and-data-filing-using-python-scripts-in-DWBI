import pyodbc
from faker import Faker
import random

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

# Function to populate OrderDetails table with fake data
def populate_order_details(num_records):
    for _ in range(num_records):
        # Randomly choose OrderID that exists in the Orders table
        OrderID = random.randint(10248, 10300)  # Adjust range based on existing OrderIDs
        ProductID = random.randint(1, 80)  # Assuming ProductIDs range from 1 to 80
        Quantity = random.randint(1, 20)  # Random quantity between 1 and 20
        UnitPrice = round(random.uniform(5.0, 100.0), 2)  # Random unit price between 5 and 100
        Discount = round(random.uniform(0.0, 0.5), 2)  # Random discount between 0.00 and 0.50

        try:
            cursor.execute('''
                INSERT INTO OrderDetails (OrderID, ProductID, UnitPrice, Quantity, Discount)
                VALUES (?, ?, ?, ?, ?)
            ''', (OrderID, ProductID, UnitPrice, Quantity, Discount))
            print(f"Inserted OrderID={OrderID}, ProductID={ProductID}, Quantity={Quantity}, UnitPrice={UnitPrice}, Discount={Discount}")
        except Exception as e:
            print(f"Error inserting data for OrderID={OrderID}, ProductID={ProductID}: {e}")

# Call the function to populate 100 records
populate_order_details(100)

# Commit the transactions
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("OrderDetails data inserted successfully!")
