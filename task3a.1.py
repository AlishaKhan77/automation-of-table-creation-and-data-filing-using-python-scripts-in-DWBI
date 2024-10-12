import pyodbc
import pandas as pd

# Define the connection to your SQL Server database
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ALISHA_SHAHID\\ALISHA_KHAN;'
                      'Database=lab03DB;'
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


# Function to insert data from CSV into the respective table
def insert_data_from_csv(table_name, csv_file):
    # Use on_bad_lines to skip problematic lines
    df = pd.read_csv(csv_file, on_bad_lines='skip')

    #df = pd.read_csv(csv_file, on_bad_lines='warn')  # Change here
    for index, row in df.iterrows():
        try:
            # Create placeholders for the query and retrieve column names from the CSV
            placeholders = ', '.join(['?'] * len(row))
            columns = ', '.join(row.index)
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Handle data type conversion and cleaning
            row_data = []
            for value in row:
                if pd.isna(value):  # Handle NULL values
                    row_data.append(None)
                elif isinstance(value, str):
                    row_data.append(value.strip())  # Remove extra spaces
                else:
                    row_data.append(value)
            
            cursor.execute(query, tuple(row_data))  # Execute insert query
        except Exception as e:
            print(f"Error inserting row {index} into {table_name}: {e}")
    
    conn.commit()
    print(f"Data inserted into {table_name} table successfully!")
# Insert data for each table from the respective CSV
for table, csv_file in csv_files.items():
    insert_data_from_csv(table, csv_file)

# Close the connection
conn.close()
