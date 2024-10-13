import pyodbc
import time
import matplotlib.pyplot as plt

# Database connection
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ALISHA_SHAHID\\ALISHA_KHAN;"  # Replace with your actual server name
    "Database=db0;"               # Replace with your database name
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Define the queries
queries = {
    # PK queries
    "Retrieve Order by OrderID": "SELECT * FROM Orders WHERE OrderID = 10248",
    "Retrieve Customer by CustomerID": "SELECT * FROM Customers WHERE CustomerID = 'ALFKI'",
    "Retrieve Supplier by SupplierID": "SELECT * FROM Suppliers WHERE SupplierID = 1",
    "Retrieve Product by ProductID": "SELECT * FROM Products WHERE ProductID = 1",
    "Retrieve Employee by EmployeeID": "SELECT * FROM Employees WHERE EmployeeID = 1",

    # Non-PK queries
    "Retrieve Customer by ContactName": "SELECT * FROM Customers WHERE ContactName = 'Alfred Schmidt'",  # Ensure this matches your DB schema
    "Retrieve Shipper by CompanyName": "SELECT * FROM Shippers WHERE CompanyName = 'Speedy Express'",
    "Retrieve Orders by OrderDate": "SELECT * FROM Orders WHERE OrderDate = '2023-01-01'",
    "Retrieve Orders by CustomerName": "SELECT * FROM Orders WHERE CustomerID IN (SELECT CustomerID FROM Customers WHERE ContactName = 'Alfred Schmidt')",  # Update accordingly
    "Retrieve Orders by ShipperName": "SELECT * FROM Orders WHERE ShipVia IN (SELECT ShipperID FROM Shippers WHERE CompanyName = 'Speedy Express')"
}

# Number of times to run each query
num_runs = 5
execution_times = {query_name: 0 for query_name in queries.keys()}

# Execute queries and measure execution time
for query_name, query in queries.items():
    for _ in range(num_runs):
        start_time = time.time()
        cursor.execute(query)
        cursor.fetchall()  # Ensure the query fully executes
        end_time = time.time()
        execution_times[query_name] += (end_time - start_time)

# Average execution times (scaled)
execution_times = {query_name: (time / num_runs) * 100 for query_name, time in execution_times.items()}

# Close the connection
conn.close()

# Separate PK and Non-PK queries for graphing
pk_queries = {name: time for name, time in execution_times.items() if "Retrieve Order by OrderID" in name or 
                                                    "CustomerID" in name or 
                                                    "SupplierID" in name or 
                                                    "ProductID" in name or 
                                                    "EmployeeID" in name}

non_pk_queries = {name: time for name, time in execution_times.items() if "Customer" in name or 
                                                    "Shipper" in name or 
                                                    "OrderDate" in name}

# Create a bar graph to display execution times
plt.figure(figsize=(10, 6))
plt.bar(pk_queries.keys(), pk_queries.values(), label='PK Queries', alpha=0.7)
plt.bar(non_pk_queries.keys(), non_pk_queries.values(), label='Non-PK Queries', alpha=0.7)
plt.xlabel('Queries')
plt.ylabel('Execution Time (scaled, x100)')
plt.title('Query Execution Times')
plt.xticks(rotation=45, ha='right')
plt.legend()
plt.tight_layout()  # Adjust layout to prevent clipping of tick-labels
plt.show()
