import pyodbc
from faker import Faker
import random

# Connect to the SQL Server database
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ALISHA_SHAHID\\ALISHA_KHAN;"  # Update with your server name
    "Database=db0;"  # Replace with your database name
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Initialize Faker
fake = Faker()

# Function to populate EmployeeTerritories
def populate_employee_territories(n):
    # Get available EmployeeIDs and TerritoryIDs
    cursor.execute("SELECT EmployeeID FROM Employees")
    employee_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT TerritoryID FROM Territories")
    territory_ids = [row[0] for row in cursor.fetchall()]

    for _ in range(n):
        employee_id = random.choice(employee_ids)  # Randomly pick an EmployeeID
        territory_id = random.choice(territory_ids)  # Randomly pick a TerritoryID
        
        try:
            cursor.execute(
                "INSERT INTO EmployeeTerritories (EmployeeID, TerritoryID) VALUES (?, ?)",
                employee_id, territory_id
            )
            print(f"Inserted EmployeeID={employee_id}, TerritoryID={territory_id}")
        except Exception as e:
            print(f"Error inserting data: {e}")

# Populate the EmployeeTerritories table
populate_employee_territories(30)  # Adjust the number of records as needed

# Commit the transactions
conn.commit()

# Close the connection
cursor.close()
conn.close()

print("EmployeeTerritories data inserted successfully!")
