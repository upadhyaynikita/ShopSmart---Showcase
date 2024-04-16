import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.graph_objects as go

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
 
# Initialize Azure Key Vault client
vault_uri = "https://akv-invoices.vault.azure.net/"
credential = DefaultAzureCredential()
secret_client = SecretClient(vault_uri, credential)


# Snowflake Connection Details
snowflake_user = secret_client.get_secret("sf-user").value
snowflake_password = secret_client.get_secret("sf-password").value
snowflake_account = secret_client.get_secret("sf-account-name").value
snowflake_warehouse = secret_client.get_secret("sf-warehouse").value
snowflake_database = secret_client.get_secret("sf-database").value
snowflake_schema = secret_client.get_secret("sf-schema").value
snowflake_table = secret_client.get_secret("sf-table").value

# Function to connect to Snowflake
def connect_snowflake():
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )
    return conn

# Function to fetch data from Snowflake table
def fetch_SF_data():
    conn = connect_snowflake()
    cursor = conn.cursor()
    
    # First business rule: Count of duplicate records based on INVOICE_ID and INVOICE_DATE
    cursor.execute("SELECT COUNT(*) FROM STG_INVOICES GROUP BY INVOICE_ID, INVOICE_DATE HAVING COUNT(*) > 1")
    duplicate_records = cursor.fetchall()
    total_duplicate_records = sum([record[0] for record in duplicate_records])
    
    # Second business rule: Count of records where AMOUNT is null or empty string
    cursor.execute("SELECT COUNT(*) FROM STG_INVOICES WHERE AMOUNT IS NULL OR TRIM(AMOUNT) = ''")
    null_amount_records = cursor.fetchone()[0]
    
    # Third business rule: Count of records where CUSTOMER_NAME is duplicate
    cursor.execute("SELECT COUNT(*) FROM (SELECT CUSTOMER_NAME FROM STG_INVOICES GROUP BY CUSTOMER_NAME HAVING COUNT(*) > 1)")
    duplicate_customer_name_records = cursor.fetchone()[0]
    
    # Fourth business rule: Count of records where TOTAL_TAX is greater than 10% of SUB_TOTAL or less than 8% of SUB_TOTAL
    cursor.execute("""
        SELECT COUNT(*) FROM STG_INVOICES 
        WHERE 
        TRY_CAST(TRIM(TOTAL_TAX) AS FLOAT) > TRY_CAST(TRIM(SUB_TOTAL) AS FLOAT) * 0.10
        OR 
        TRY_CAST(TRIM(TOTAL_TAX) AS FLOAT) < TRY_CAST(TRIM(SUB_TOTAL) AS FLOAT) * 0.08
    """)
    total_tax_out_of_range_records = cursor.fetchone()[0]
    
    # Total number of records in the table
    cursor.execute("SELECT COUNT(*) FROM STG_INVOICES")
    total_records = cursor.fetchone()[0]
    
    conn.close()
    return total_duplicate_records, null_amount_records, duplicate_customer_name_records, total_tax_out_of_range_records, total_records

# # Fetch latest data from Snowflake
# total_duplicate_records, null_amount_records, duplicate_customer_name_records, total_tax_out_of_range_records, total_records = fetch_SF_data()

# # Create DataFrame for double bar graph
# df = pd.DataFrame({
#     'Business Rule': ['Duplicate Records (Invoice ID and Invoice Date)', 'Null Amount Records', 'Duplicate Customer Names', 'Total Tax Out of Range Records', 'Total Records'],
#     'Count': [total_duplicate_records, null_amount_records, duplicate_customer_name_records, total_tax_out_of_range_records, total_records],
#     'Color': ['blue', 'blue', 'blue', 'blue', 'orange']
# })

# # Create double bar graph
# fig = go.Figure()

# for index, row in df.iterrows():
#     fig.add_trace(go.Bar(
#         y=[row['Business Rule']],
#         x=[row['Count']],
#         orientation='h',
#         marker=dict(color=row['Color']),
#         name=row['Business Rule'],
#         width=0.3  # Set the width of the bars
#     ))

# # Update layout
# fig.update_layout(
#     barmode='group',
#     title='Invoices Discrepancies Summary',
#     yaxis_title='Business Rule',
#     xaxis_title='Records Count',
# )

# # Display the graph
# st.plotly_chart(fig, use_container_width=True)
