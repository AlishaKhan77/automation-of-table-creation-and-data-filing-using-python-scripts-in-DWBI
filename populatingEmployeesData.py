import pyodbc
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Connect to the SQL Server database
conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=ALISHA_SHAHID\\ALISHA_KHAN;"
    "Database=db0;"
    "Trusted_Connection=yes;"
)

cursor = conn.cursor()

# Function to insert data into the Employees table
def insert_employee_data(cursor):
    for _ in range(10):  # Generating 10 employee records, adjust as needed
        EmployeeID = random.randint(100, 999)  # Generate random EmployeeID
        LastName = fake.last_name()
        FirstName = fake.first_name()
        Title = fake.job()
        TitleOfCourtesy = random.choice(['Mr.', 'Mrs.', 'Ms.', 'Dr.'])
        BirthDate = fake.date_of_birth(minimum_age=18, maximum_age=65)
        HireDate = fake.date_this_decade()
        Address = fake.address().replace('\n', ', ')
        City = fake.city()
        Region = fake.state_abbr()
        PostalCode = fake.postcode()
        Country = fake.country()
        HomePhone = fake.phone_number()
        Extension = str(random.randint(1000, 9999))
        Photo = None  # Placeholder, as it's an image field
        Notes = fake.text(max_nb_chars=200)
        ReportsTo = None  # Null, or set as an EmployeeID from a previous insert
        PhotoPath = fake.image_url()

        # Insert data into Employees table
        cursor.execute('''
            INSERT INTO Employees (EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate, 
                                   Address, City, Region, PostalCode, Country, HomePhone, Extension, Photo, Notes, 
                                   ReportsTo, PhotoPath)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate, Address, City, 
              Region, PostalCode, Country, HomePhone, Extension, Photo, Notes, ReportsTo, PhotoPath))

        print(f"Inserted EmployeeID={EmployeeID}, Name={FirstName} {LastName}, Title={Title}")

    conn.commit()  # Save the changes

# Insert employee data
insert_employee_data(cursor)

# Close the connection
conn.close()
