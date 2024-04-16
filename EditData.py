import streamlit as st
import snowflake.connector
import pandas as pd
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
import os
# # Initialize Azure Key Vault client
# vault_uri = "https://akv-invoices.vault.azure.net/"
# credential = DefaultAzureCredential()
# secret_client = SecretClient(vault_uri, credential)


# Snowflake Connection Details
# snowflake_user = os.environ.get("SF_USER")
# snowflake_password = os.environ.get("SF_PASSWORD")
# snowflake_account = os.environ.get("SF_ACCOUNT")
# snowflake_warehouse = os.environ.get("SF_WAREHOUSE")
# snowflake_database = os.environ.get("SF_DATABASE")
# snowflake_schema = os.environ.get("SF_SCHEMA")
# snowflake_table = os.environ.get("SF_TABLE")

# Connect to Snowflake
try:
    # conn = snowflake.connector.connect(
    #     user=snowflake_user,
    #     password=snowflake_password,
    #     account=snowflake_account,
    #     warehouse=snowflake_warehouse,
    #     database=snowflake_database,
    #     schema=snowflake_schema
    # )
    conn = st.connection("snowflake")
    st.success("Snowflake Connection Established Successfully!")
except Exception as e:
    st.error(f"Error connecting to Snowflake: {str(e)}")

# Function to fetch data from Snowflake table
def fetch_data():
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM STG_INVOICES")
        columns = [col[0] for col in cursor.description]
        data = cursor.fetchall()
        cursor.close()
        return columns, data
    except Exception as e:
        st.error(f"Error fetching data from Snowflake: {str(e)}")

# Function to update data in Snowflake table
def update_data(column_name, unique_identifier, unique_identifier_value, new_value):
    try:
        cursor = conn.cursor()
        query = f"UPDATE STG_INVOICES SET {column_name} = %s WHERE {unique_identifier} = %s"
        cursor.execute(query, (new_value, unique_identifier_value))
        conn.commit()
        st.success("Data Updated Successfully!")
    except Exception as e:
        st.error(f"Error updating data in Snowflake: {str(e)}")

# Main Streamlit App
# def main():
#     st.title("Snowflake Data Editor")

#     # Fetch data from Snowflake
#     columns, data = fetch_data()

#     # Display data as DataFrame
#     df = pd.DataFrame(data, columns=columns)
#     st.write("### Snowflake Table Data")
#     st.write(df)

#     # Dropdown to select column name
#     selected_column = st.selectbox("Select Column to Edit", columns)

#     # Input for unique identifier column
#     unique_identifier_column = st.selectbox("Select Unique Identifier Column", columns)

#     # Input for unique identifier value
#     unique_identifier_value = st.text_input("Enter Unique Identifier Value")

#     # Text input for new value
#     new_value = st.text_input("Enter New Value")

#     # Submit button to update data
#     if st.button("Submit"):
#         if new_value and unique_identifier_value:
#             update_data(selected_column, unique_identifier_column, unique_identifier_value, new_value)
#         else:
#             st.warning("Please enter both unique identifier value and new value.")

# # Run the app
# if __name__ == "__main__":
#     main()
