import pandas as pd
import pyodbc

# === Step 1: Load Excel File and Both Sheets ===
excel_file = "online_retail_II.xlsx"  # Ensure this file is in the same folder
sheet_names = ["Year 2009-2010", "Year 2010-2011"]

# Read and concatenate both sheets
try:
    df_list = [pd.read_excel(excel_file, sheet_name=sheet) for sheet in sheet_names]
    df = pd.concat(df_list, ignore_index=True)
    print("Both Excel sheets loaded and merged successfully.")
except Exception as e:
    print(f"Failed to load Excel sheets: {e}")
    exit()

# Replace NaN with None (SQL-compatible nulls)
df = df.where(pd.notnull(df), None)

# === Step 2: Connect to SQL Server ===
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'                 # Change if needed
        'DATABASE=OnlineRetailDB;'          # Your database name
        'Trusted_Connection=yes;'           # Or use UID and PWD
    )
    cursor = conn.cursor()
    print("Connected to SQL Server.")
except Exception as e:
    print(f"Database connection failed: {e}")
    exit()

# === Step 3: Create Table If Not Exists ===
create_table_query = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='OnlineRetail' AND xtype='U')
CREATE TABLE OnlineRetail (
    Invoice NVARCHAR(55),
    StockCode NVARCHAR(55),
    Description NVARCHAR(255),
    Quantity INT,
    InvoiceDate DATETIME,
    Price FLOAT,
    Customer_ID INT,
    Country NVARCHAR(100)
)
"""

try:
    cursor.execute(create_table_query)
    conn.commit()
    print("Table checked/created.")
except Exception as e:
    print(f"Table creation failed: {e}")
    conn.close()
    exit()

# === Step 4: Insert Data Into SQL Table ===
insert_query = """
INSERT INTO OnlineRetail (
    Invoice, StockCode, Description, Quantity,
    InvoiceDate, Price, Customer_ID, Country
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

try:
    print(df['Customer ID'].unique()[:10])  # Arată primele 10 valori unice
    print(df['Customer ID'].dtype)          # Vezi tipul de date

    for index, row in df.iterrows():
        # try:
            invoice_no = str(row['Invoice']) if row['Invoice'] else None
            stock_code = str(row['StockCode']) if row['StockCode'] else None
            description = str(row['Description']) if row['Description'] else None
            quantity = int(row['Quantity']) if pd.notnull(row['Quantity']) else None
            invoice_date = pd.to_datetime(row['InvoiceDate']) if pd.notnull(row['InvoiceDate']) else None
            price = float(row['Price']) if pd.notnull(row['Price']) else None
            customer_id = int(row['Customer ID']) if pd.notnull(row['Customer ID']) else None
            country = str(row['Country']) if row['Country'] else None

            cursor.execute(insert_query, (
                invoice_no, stock_code, description, quantity,
                invoice_date, price, customer_id, country
            ))

            if index % 1000 == 0:
                print(f"Inserted {index} rows...")
        # except Exception as row_error:
        #     print(f"Skipping row {index} due to error: {row_error}")

    conn.commit()
    print(f"All {len(df)} rows inserted successfully.")
    
except Exception as e:
    print(f"Data insertion failed: {e}")
    conn.rollback()

# === Step 5: Close Connection ===
cursor.close()
conn.close()
print("Database connection closed.")
