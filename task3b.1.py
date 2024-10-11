import pyodbc
import pandas as pd
from faker import Faker

# Define the connection to your SQL Server database
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ALISHA_SHAHID\\ALISHA_KHAN;'
                      'Database=NWBD2;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

# Define the path for the CSV files
csv_path = r'C:\Users\alish\Desktop\DW LAB\Lab_03_DW&BI\Lab_03_DW&BI\northwind'

# Define file paths for each CSV
csv_files = {
    'Categories': f'{csv_path}\\Categories.csv',
    'Customers': f'{csv_path}\\Customers.csv',
    'OrderDetails': f'{csv_path}\\Order_details.csv',
    'Products': f'{csv_path}\\Products.csv',
    'Shippers': f'{csv_path}\\Shippers.csv',
    'Suppliers': f'{csv_path}\\Suppliers.csv',
    'Orders': f'{csv_path}\\Orders.csv'
}

# Initialize Faker
fake = Faker()

# Function to insert data from CSV into the respective table
def insert_data_from_csv(table_name, csv_file):
    df = pd.read_csv(csv_file, on_bad_lines='warn')  # Change here
    for index, row in df.iterrows():
        try:
            placeholders = ', '.join(['?'] * len(row))
            columns = ', '.join(row.index)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            row_data = []
            for value in row:
                if pd.isna(value):
                    row_data.append(None)
                elif isinstance(value, str):
                    row_data.append(value.strip())
                else:
                    row_data.append(value)
            
            cursor.execute(query, tuple(row_data))
        except Exception as e:
            print(f"Error inserting row {index} into {table_name}: {e}")
    
    conn.commit()
    print(f"Data inserted into {table_name} table successfully!")

# Insert data for each table from the respective CSV
for table, csv_file in csv_files.items():
    insert_data_from_csv(table, csv_file)

# Function to insert fake data into the remaining tables
def insert_fake_data(table_name, num_records):
    for _ in range(num_records):
        try:
            if table_name == 'Customers':
                query = "INSERT INTO Customers (CustomerID, CompanyName, ContactName, Country) VALUES (?, ?, ?, ?)"
                cursor.execute(query, (fake.uuid4(), fake.company(), fake.name(), fake.country()))
            elif table_name == 'Products':
                query = "INSERT INTO Products (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice) VALUES (?, ?, ?, ?, ?, ?)"
                cursor.execute(query, (fake.uuid4(), fake.word(), fake.random_int(min=1, max=20), fake.random_int(min=1, max=10), fake.random_int(min=1, max=10), fake.random_number(digits=2)))
            elif table_name == 'Shippers':
                query = "INSERT INTO Shippers (ShipperID, ShipperName, Phone) VALUES (?, ?, ?)"
                cursor.execute(query, (fake.uuid4(), fake.company(), fake.phone_number()))
            elif table_name == 'Suppliers':
                query = "INSERT INTO Suppliers (SupplierID, SupplierName, ContactName, Country) VALUES (?, ?, ?, ?)"
                cursor.execute(query, (fake.uuid4(), fake.company(), fake.name(), fake.country()))
            elif table_name == 'Orders':
                query = "INSERT INTO Orders (OrderID, CustomerID, EmployeeID, OrderDate, ShipperID) VALUES (?, ?, ?, ?, ?)"
                cursor.execute(query, (fake.uuid4(), fake.random_int(min=1, max=20), fake.random_int(min=1, max=10), fake.date_time_this_decade(), fake.random_int(min=1, max=5)))
            # Add more conditions for other tables as needed
            
        except Exception as e:
            print(f"Error inserting fake data into {table_name}: {e}")
    
    conn.commit()
    print(f"Fake data inserted into {table_name} table successfully!")

# Insert fake data into the remaining tables
remaining_tables = ['Customers', 'Products', 'Shippers', 'Suppliers', 'Orders']
num_records_per_table = 10  # Adjust the number of records to insert as needed

for table in remaining_tables:
    insert_fake_data(table, num_records_per_table)

# Close the connection
conn.close()
