import streamlit as st
import snowflake.connector
import pandas as pd


# Snowflake Connection Details
snowflake_user = "diwakarnahata4aws"
snowflake_password = "Info@999"
snowflake_account = "kechpqy-arb81606"
snowflake_warehouse = "COMPUTE_WH"
snowflake_database = "DFS"
snowflake_schema = "DEMO"
snowflake_table = "STG_INVOICES"

# Connect to Snowflake
try:
    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )
    st.success("Snowflake Connection Established Successfully!")
except Exception as e:
    st.error(f"Error connecting to Snowflake: {str(e)}")

# Function to fetch data from Snowflake table
def fetch_data():
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {snowflake_table}")
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
        query = f"UPDATE {snowflake_table} SET {column_name} = %s WHERE {unique_identifier} = %s"
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
