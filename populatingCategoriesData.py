import pandas as pd
import pyodbc

# Database connection
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ALISHA_SHAHID\\ALISHA_KHAN;'
                      'Database=db0;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

# Read CSV file into pandas dataframe
df_categories = pd.read_csv('C:/Users/alish/Desktop/DW LAB/Lab_03_DW&BI/Lab_03_DW&BI/northwind/Categories.csv')

# Strip extra spaces from column names
df_categories.columns = df_categories.columns.str.strip()

# Insert data into Categories table, avoiding duplicates
for index, row in df_categories.iterrows():
    # Check if the CategoryID already exists in the database
    cursor.execute('SELECT COUNT(*) FROM Categories WHERE CategoryID = ?', row['CategoryID'])
    count = cursor.fetchone()[0]
    
    if count == 0:
        # If the CategoryID does not exist, insert the row
        cursor.execute('''
            INSERT INTO Categories (CategoryID, CategoryName, Description) 
            VALUES (?, ?, ?)
        ''', row['CategoryID'], row['CategoryName'], row.get('Description', 'No Description'))
    else:
        print(f"Skipping duplicate CategoryID: {row['CategoryID']}")

# Commit the transaction
conn.commit()

# Close the connection
conn.close()

print("Categories data inserted successfully!")
