import streamlit as st
import snowflake.connector
import os

# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
 
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

# Function to copy records from STG_INVOICES to WH_INVOICES and truncate STG_INVOICES
def publish_data():
    try:
        conn = st.connection("snowflake")
        cursor = conn.cursor()

        # Copy records from STG_INVOICES to WH_INVOICES
        cursor.execute("INSERT INTO WH_INVOICES SELECT * FROM STG_INVOICES")
        st.session_state.publish_success = True

        # Truncate STG_INVOICES
        cursor.execute("TRUNCATE TABLE STG_INVOICES")

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")

# Streamlit UI
# def main():
#     # Button to trigger data publishing process
#     if st.button("Publish", help="Click to publish data", key="publish_button"):
#         publish_data()

#     # Injecting CSS to position the button in the bottom right corner and change its color
#     button_style = """
#     <style>
#     .stButton button {
#         position: fixed;
#         bottom: 20px;
#         right: 20px;
#         background-color: #2E8B57; /* Sea green */
#     }
#     .stButton button:hover {
#         background-color: #3CB371; /* Light sea green */
#     }
#     </style>
#     """
#     st.markdown(button_style, unsafe_allow_html=True)

#     # Show success message below the button when the process is completed successfully
#     if st.session_state.get("publish_success", False):
#         success_message_style = """
#         <style>
#         .success-message {
#             position: fixed;
#             bottom: 50px; /* Adjusted position */
#             right: 20px;
#             color: #2E8B57;
#         }
#         </style>
#         """
#         st.markdown(success_message_style, unsafe_allow_html=True)
#         st.markdown('<p class="success-message">Process completed successfully!</p>', unsafe_allow_html=True)

# if __name__ == "__main__":
#     main()
