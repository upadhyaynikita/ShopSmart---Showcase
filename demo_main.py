from openai import OpenAI
import re
import streamlit as st
from prompts import get_system_prompt

import os
import requests
from PIL import Image
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, BlobPrefix
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
import random
import plotly.graph_objects as go

from streamlit_option_menu import option_menu


from streamlit_float import *
from streamlit import session_state

from plot import fetch_SF_data, connect_snowflake
from EditData import fetch_data, update_data
from publish import publish_data

def start_session():
    st.session_state.session_started = True

def stop_session():
    st.session_state.session_started = False


# # Snowflake Connection Details
# snowflake_user=st.secrets.snowflake_user
# snowflake_password=st.secrets.snowflake_password
# snowflake_account=st.secrets.snowflake_account
# snowflake_warehouse=st.secrets.snowflake_warehouse
# snowflake_database=st.secrets.snowflake_database
# snowflake_schema=st.secrets.snowflake_schema
# snowflake_table=st.secrets.snowflake_table

# Connect to Snowflake
# try:
#     conn = snowflake.connector.connect(
#         user=snowflake_user,
#         password=snowflake_password,
#         account=snowflake_account,
#         warehouse=snowflake_warehouse,
#         database=snowflake_database,
#         schema=snowflake_schema
#     )
#     #st.success("Snowflake Connection Established Successfully!")
# except Exception as e:
#     st.error(f"Error connecting to Snowflake: {str(e)}")

# # Function to fetch data from Snowflake table
# def fetch_data():
#     try:
#         cursor = conn.cursor()
#         cursor.execute(f"SELECT * FROM {snowflake_table}")
#         columns = [col[0] for col in cursor.description]
#         data = cursor.fetchall()
#         cursor.close()
#         return columns, data
#     except Exception as e:
#         st.error(f"Error fetching data from Snowflake: {str(e)}")

# # Connect to Snowflake

# def connect_snowflake():
#     conn = snowflake.connector.connect(
#         user=snowflake_user,
#         password=snowflake_password,
#         account=snowflake_account,
#         warehouse=snowflake_warehouse,
#         database=snowflake_database,
#         schema=snowflake_schema
#     )
#     return conn

# Fetch data from Snowflake table
# Function to fetch data from Snowflake table
# def fetch_SF_data():
#     conn = connect_snowflake()
#     cursor = conn.cursor()
    
#     # First business rule: Count of duplicate records based on INVOICE_ID and INVOICE_DATE
#     cursor.execute("SELECT COUNT(*) FROM STG_INVOICES GROUP BY INVOICE_ID, INVOICE_DATE HAVING COUNT(*) > 1")
#     duplicate_records = cursor.fetchall()
#     total_duplicate_records = sum([record[0] for record in duplicate_records])
    
#     # Second business rule: Count of records where AMOUNT is null or empty string
#     cursor.execute("SELECT COUNT(*) FROM STG_INVOICES WHERE TOTAL_TAX IS NULL OR sub_total is null")
#     null_amount_records = cursor.fetchone()[0]
    
#     # Third business rule: Count of records where CUSTOMER_NAME is duplicate
#     cursor.execute("SELECT COUNT(*) FROM (SELECT VENDOR_NAME FROM STG_INVOICES GROUP BY VENDOR_NAME HAVING COUNT(*) > 1)")
#     duplicate_vendor_name_records = cursor.fetchone()[0]
    
    # # Fourth business rule: Count of records where TOTAL_TAX is greater than 10% of SUB_TOTAL or less than 8% of SUB_TOTAL
    # cursor.execute("""
    #     SELECT COUNT(*) FROM STG_INVOICES 
    #     WHERE 
    #     TRY_CAST(TRIM(TOTAL_TAX) AS FLOAT) > TRY_CAST(TRIM(SUB_TOTAL) AS FLOAT) * 0.10
    #     OR 
    #     TRY_CAST(TRIM(TOTAL_TAX) AS FLOAT) < TRY_CAST(TRIM(SUB_TOTAL) AS FLOAT) * 0.08
    # """)
    # total_tax_out_of_range_records = cursor.fetchone()[0]
    
    # # Total number of records in the table
    # cursor.execute("SELECT COUNT(*) FROM STG_INVOICES")
    # total_records = cursor.fetchone()[0]
    
    # conn.close()
    # return total_duplicate_records, null_amount_records, duplicate_vendor_name_records, total_tax_out_of_range_records, total_records





# Function to update data in Snowflake table
# def update_data(column_name, unique_identifier, unique_identifier_value, new_value):
#     try:
#         cursor = conn.cursor()
#         query = f"UPDATE STG_INVOICES SET {column_name} = %s WHERE {unique_identifier} = %s"
#         cursor.execute(query, (new_value, unique_identifier_value))
#         conn.commit()
#         st.success("Data Updated Successfully!, Click on button above to Refresh. ")
#     except Exception as e:
#         st.error(f"Error updating data in Snowflake: {str(e)}")





#file to upload in azure container
def upload_to_azure_container(account_name, account_key, container_name, file_path, blob_name=None):
    """
    Uploads a file to an Azure Blob Storage container using account access keys.

    Args:
        account_name (str): Azure Storage account name.
        account_key (str): Access key for the Azure Storage account.
        container_name (str): Name of the container to upload the file to.
        file_path (str): Local path of the file to upload.
        blob_name (str): Name to be used for the blob. If None, the name of the file will be used.

    Returns:
        str: URL of the uploaded blob.
    """
    try:
        # Create BlobServiceClient using the account name and account key
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)

        # Get a container client
        container_client = blob_service_client.get_container_client(container_name)

        # Get the blob name from file_path if not provided
        if not blob_name:
            blob_name = os.path.basename(file_path)

        # Create a blob client
        blob_client = container_client.get_blob_client(blob_name)

        # Upload the file
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)

        # Get the URL of the uploaded blob
        blob_url = blob_client.url
        return blob_url

    except Exception as e:
        print(f"Error uploading file to Azure Blob Storage: {e}")
        return None
    
#Function to delete files from input folder
def delete_files_from_container():
    try:
        # Define your Azure Storage account credentials and container name
        account_name = "invoicepool"
        account_key = "tR58WG8ynd3NVIdeNFMgmt0JNw10SXTd9AACrPPDlNxsWfYFUYOJJgXV8whXh+DYbchMkcfA4IQd+AStKxh5fA=="
        container_name = "input"

        # Connect to the Azure Storage account
        blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
        
        # Get a list of blob names in the container
        blob_list = blob_service_client.get_container_client(container_name).list_blobs()
        
        # Delete each blob in the container
        for blob in blob_list:
            blob_service_client.get_blob_client(container=container_name, blob=blob.name).delete_blob()
        
        #st.success("All files deleted successfully.")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")

def show_modal(content):
            # Display the modal content at the bottom of the page
            st.markdown(f'<div style="position: fixed; bottom: 0; right:0; width: 100%;  background-color: #f0f0f0; padding: 20px; border-top: 1px solid #ddd;">{content}</div>', unsafe_allow_html=True)



def main():
    st.markdown("""
    <style>
    /* Reduce the size of file uploader */
    .stFileUploader > div {
        padding: 0 ;
        height: 0.5px;
    }

    /* Reduce the size of camera input */
    .stCameraInput > div {
        padding: 0.1rem;
    }

    /* Reduce the size of buttons */
    .stButton > button {
        font-size: 10px;
        padding: 0.1rem 0.3rem;
    }
    </style>
    """, unsafe_allow_html=True)


    st.markdown("""
    <style>
    .title {
        text-align: center;
        padding: 0px;
        margin: 0px;
        background-color: lighblue;
        height : 0px;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <style>
    /* Reduce the size of option menu */
    .OptionMenu > div {
        font-size: 10px; /* Adjust font size */
        padding: 0.1rem 0.3rem; /* Adjust padding */
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<h2 class='title'>ShopSmart Showcase</h2>", unsafe_allow_html=True)
    st.markdown("")
    st.markdown("")
    approve = st.markdown("<p style='text-align: center;'>File Upload..........Processing & Fraud Detection..........Fraud Correction</p>", unsafe_allow_html=True)
    st.markdown("--------------------------------------------------------------------------------")

    #approve = st.markdown("Status Bar : Step1: Upload File_____Step2: Processing & Fraud Detection_____Step3: Fraud Correction")
    #approve = st.markdown("<p style='text-align: center;'>Invoice Processing: File Upload..........Processing & Fraud Detection..........Fraud Correction</p>", unsafe_allow_html=True)
    col1, col2 , col3= st.columns([2,0.5, 3])

    with col1:
        # Option menu to select upload method
        # st.markdown("**Test your own Document**")
        st.markdown("<h4 style='text-align: center;'>Test your own Document</h4>", unsafe_allow_html=True)
    
        selected = option_menu(
            menu_title = None, 
            options = ["📁File Uploader", '📷 Camera'], 
            menu_icon="cast", 
            #default_index= 0,
            orientation= "horizontal",
        )

        if selected == "📁File Uploader":
            uploaded_files = st.file_uploader("", type=['txt', 'csv', 'pdf', 'jpg'], accept_multiple_files=True)
            if uploaded_files is not None:
                total_files = len(uploaded_files)
                uploaded_count = 0  # Counter for successfully uploaded files

                uploaded_filenames = [file.name for file in uploaded_files]
                # Check if any files have the same name
                if len(uploaded_filenames) != len(set(uploaded_filenames)):
                    st.warning("Warning: You have uploaded files with the same name. Please rename the files and try again.")
                else:
                    # Button to upload files
                    if st.button("Click to Upload File"):
                        delete_files_from_container()
                        if not uploaded_files:
                            st.warning("No files uploaded. Please upload files before hitting the button.")
                        else:
                            account_name = "invoicepool"
                            account_key = "tR58WG8ynd3NVIdeNFMgmt0JNw10SXTd9AACrPPDlNxsWfYFUYOJJgXV8whXh+DYbchMkcfA4IQd+AStKxh5fA=="
                            container_name = "input"

                            # Display in progress message
                            progress_text = st.empty()
                            progress_text.text("Upload in progress...")

                            # Upload files to Azure container
                            for uploaded_file in uploaded_files:
                                if isinstance(uploaded_file, str):  # If it's a file path
                                    file_path = uploaded_file
                                else:  # If it's a camera image
                                    file_path = f"./{uploaded_file.name}"
                                    with open(file_path, "wb") as f:
                                        f.write(uploaded_file.read())

                                uploaded_url = upload_to_azure_container(account_name, account_key, container_name, file_path)
                                if uploaded_url:
                                    uploaded_count += 1
                                    print("File uploaded successfully. URL:", uploaded_url)
                                else:
                                    print("Failed to upload file.")

                            # Display message indicating the number of files uploaded
                            progress_text.markdown(f"<span style='color:green'>{uploaded_count} out of {total_files} file(s) uploaded successfully.</span>", unsafe_allow_html=True)
                            image_url = "https://www.bing.com/th?id=OIP.Cs_MQcODAFqjh_mqsHuJ5wHaHa&w=179&h=185&c=8&rs=1&qlt=90&o=6&pid=3.1&rm=2"
                            
                            approve.markdown('<div style="text-align: center;" class="image-container">Upload File<img src="' + image_url + '" width="3%" />Processing & Fraud Detection..........Fraud Correction</div>', unsafe_allow_html=True)
                            
        elif selected == "📷 Camera":
            uploaded_files =st.empty()
            picture = st.camera_input("Capture an Image", key="camera_input")
        
            if picture:
                uploaded_files = [picture] 

            if st.button("Upload Image"):
                # Upload the captured image
                account_name = "invoicepool"
                account_key = "tR58WG8ynd3NVIdeNFMgmt0JNw10SXTd9AACrPPDlNxsWfYFUYOJJgXV8whXh+DYbchMkcfA4IQd+AStKxh5fA=="
                container_name = "input"
                for uploaded_file in uploaded_files:
                    file_path = f"./{uploaded_file.name}"
                    with open(file_path, "wb") as f:
                        f.write(uploaded_file.read())
                uploaded_url = upload_to_azure_container(account_name, account_key, container_name, file_path)
                if uploaded_url:
                    st.success("Image uploaded successfully!")
                else:
                    st.error("Failed to upload image.")
                image_url = "https://www.bing.com/th?id=OIP.Cs_MQcODAFqjh_mqsHuJ5wHaHa&w=179&h=185&c=8&rs=1&qlt=90&o=6&pid=3.1&rm=2"
                        
                approve.markdown('<div style="text-align: center;" class="image-container">Upload File<img src="' + image_url + '" width="3%" />Processing & Fraud Detection..........Fraud Correction</div>', unsafe_allow_html=True)

        st.markdown("------------------------------------------------------------------------")
        # Location 2: Display container if session is started
        if st.session_state.get("session_started", False):
            # Display scrollable container
            with st.container(height = 500):
                
            #Code for Chatbot

            ###
                st.title("☃️ Devika")

                # Initialize the chat messages history
                client = OpenAI(api_key=st.secrets.OPENAI_API_KEY)
                if "messages" not in st.session_state:
                    # system prompt includes table information, rules, and prompts the LLM to produce
                    # a welcome message to the user.
                    st.session_state.messages = [{"role": "system", "content": get_system_prompt()}]


                # display the existing chat messages
                for message in st.session_state.messages:
                    if message["role"] == "system":
                        continue
                    with st.chat_message(message["role"]):
                        st.write(message["content"])
                        if "results" in message:
                            st.dataframe(message["results"])

                # If last message is not from assistant, we need to generate a new response
                if st.session_state.messages[-1]["role"] != "assistant":
                    with st.chat_message("assistant"):
                        response = ""
                        resp_container = st.empty()
                        for delta in client.chat.completions.create(
                            model="gpt-4",
                            messages=[{"role": m["role"], "content": m["content"]} for m in st.session_state.messages],
                            stream=True,
                        ):
                            response += (delta.choices[0].delta.content or "")
                            resp_container.markdown(response)

                        message = {"role": "assistant", "content": response}
                        # Parse the response for a SQL query and execute if available
                        sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)
                        if sql_match:
                            sql = sql_match.group(1)
                            conn = st.connection("snowflake")
                            message["results"] = conn.query(sql)
                            st.dataframe(message["results"])
                        st.session_state.messages.append(message)
                # Prompt for user input and save
                if prompt := st.chat_input():
                    st.session_state.messages.append({"role": "user", "content": prompt})        
###            
                
                
                
                
                
        
            # Add a button to stop the session
            if st.button("Close ChatBOT", key="stop_button"):
                stop_session()


    with col3:
        # st.markdown("**Invoice Discrepancies Summary**")
        st.markdown("<h4 style='text-align: center;'>Invoice Discrepancies Summary</h4>", unsafe_allow_html=True)

        st.markdown("This section will get you a Summary of Invoice Inconsistencies and an editable table to handle those inconsistences. Click on the below button to refresh the Summary Data and Table Preview " )
 
        st.markdown("")
        fraudlent = st.button("Click to Refresh invoice discrepancies")

        selecteds = option_menu(
            menu_title = None, 
            options = ["Invoice Discrepancies Summary", "Table Preview of Invoice discrepancies"], 
            menu_icon="cast", 
            #default_index= 0,
            orientation= "horizontal",
        )
        if selecteds == "Invoice Discrepancies Summary":
            if fraudlent:
                api_endpoint = 'https://invoice-processing-app.azurewebsites.net/api/InvoiceProcessing'
                if not uploaded_files:
                        #st.warning("No files uploaded. Please upload files before hitting the button.")
                        st.warning("")
                else:
                    response = requests.get(url=api_endpoint, verify=False)
                    if response.status_code == 200:
                        st.markdown(f"<span style='color:green'>Your RESULTS are ready!</span>", unsafe_allow_html=True)
                    
                    else:
                        st.error(f"API call failed with status code: {response.status_code}") 

                image_url = "https://www.bing.com/th?id=OIP.Cs_MQcODAFqjh_mqsHuJ5wHaHa&w=179&h=185&c=8&rs=1&qlt=90&o=6&pid=3.1&rm=2"
                        
                approve.markdown('<div style="text-align: center;" class="image-container">Upload File<img src="' + image_url + '" width="3%" />Processing & Fraud Detection<img src="' + image_url + '" width="3%" />Fraud Correction</div>', unsafe_allow_html=True)

            # Fetch latest data from Snowflake
            total_duplicate_records, null_amount_records, duplicate_vendor_name_records, total_tax_out_of_range_records, total_records = fetch_SF_data()

            # Create DataFrame for double bar graph
            df = pd.DataFrame({
                'Business Rule': ['Duplicate Records (Invoice ID and Invoice Date)', 'Null Amount Records', 'Duplicate Vendor Names', 'Total Tax Out of Range Records', 'Total Records'],
                'Count': [total_duplicate_records, null_amount_records, duplicate_vendor_name_records, total_tax_out_of_range_records, total_records],
                'Color': ['blue', 'blue', 'blue', 'blue', 'orange']
            })

            # Create double bar graph
            fig = go.Figure()

            for index, row in df.iterrows():
                fig.add_trace(go.Bar(
                    y=[row['Business Rule']],
                    x=[row['Count']],
                    orientation='h',
                    marker=dict(color=row['Color']),
                    name=row['Business Rule'],
                    width=0.3  # Set the width of the bars
                ))

            # Update layout
            fig.update_layout(
                barmode='group',
                title='Invoices Discrepancies Summary',
                yaxis_title='Business Rule',
                xaxis_title='Records Count',
            )

            # Display the graph
            st.plotly_chart(fig, use_container_width=True)


#2nd tab
        if selecteds == "Table Preview of Invoice discrepancies":
            columns, data = fetch_data()
            # Display data as DataFrame
            df = pd.DataFrame(data, columns=columns)
        
            st.write(df)
        
            #if st.button("Modify/Update the Incorrect Record"):
            if "show" not in st.session_state:
                st.session_state.show = False  
            buttons_container = st.container()
            with buttons_container:
                if st.button("Edit Table  ", key="change_button", help="Options to update table"):
                    st.session_state.show = not st.session_state.show       
                 # Dropdown to select column name

            if st.session_state.show:
                with st.form(key="update_form"):
                  selected_column = st.selectbox("Select Column to Edit", columns)
                  unique_identifier_column = st.selectbox("Select Unique Identifier Column", columns)
                  unique_identifier_value = st.text_input("Enter Unique Identifier Value")
                  new_value = st.text_input("Enter New Value")
                  st.session_state.show = True
        # Submit button to update data
                  if st.form_submit_button("Update table"):
                      
                       if new_value and unique_identifier_value:
                            update_data(selected_column, unique_identifier_column, unique_identifier_value, new_value)
                            #st.success("Data updated successfully!")
                            image_url = "https://www.bing.com/th?id=OIP.Cs_MQcODAFqjh_mqsHuJ5wHaHa&w=179&h=185&c=8&rs=1&qlt=90&o=6&pid=3.1&rm=2"
                        
                            approve.markdown('<div style="text-align: center;" class="image-container">Upload File<img src="' + image_url + '" width="3%" />Processing & Fraud Detection<img src="' + image_url + '" width="3%" />Fraud Correction<img src="' + image_url + '" width="3%" /></div>', unsafe_allow_html=True)
            
                       else:
                            st.warning("Please enter both unique identifier value and new value.")
  
        st.markdown("-------------------------------------------------------------------------")
        st.markdown("Get help from ChatBOT to apply more fraud detection rules")

# Location 1: Button to start the session
        if st.button("Activate ChatBOT", key="start_button"):
            start_session()



if __name__ == "__main__":
    main()
    
st.markdown("------------------------------------------------------------------------------------------------------------------------------------")

