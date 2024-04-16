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
import plotly.graph_objects as go
from streamlit_option_menu import option_menu
from streamlit_float import *
from streamlit import session_state

# from chatbot import chatbot
from plot import fetch_SF_data, connect_snowflake
from EditData import fetch_data, update_data
from publish import publish_data

from streamlit_extras.stylable_container import stylable_container

# def execute_sql_query(sql):
#     # Execute SQL query using Snowflake connection
#     try:
#         conn = st.connection("snowflake")
#         st.code(sql, language='sql')
#         results = conn.query(sql)
#         return results
#     except Exception as e:
#         st.error(f"Failed to execute SQL query: {e}")
#         return None

# def handle_response(response):
#     # Parse response for SQL query and execute if available
#     sql_match = re.search(r"```sql\n(.*)\n```", response, re.DOTALL)
#     if sql_match:
#         sql = sql_match.group(1).strip()
#         results = execute_sql_query(sql)
#         if results is not None:
#             st.dataframe(results)

# def generate_response(client, messages):
#     response = ""
#     try:
#         for delta in client.chat.completions.create(
#             model="gpt-4",
#             messages=[{"role": m["role"], "content": m["content"]} for m in messages],
#             stream=True,
#         ):
#             response += (delta.choices[0].delta.content or "")
#         return response
#     except Exception as e:
#         st.error(f"Failed to generate response: {e}")
#         return None

def start_chatbot_session():
    if "chatbot_session" not in st.session_state:
        st.session_state.chatbot_session = False

    if st.button("Analyze Fraud in Detail", key="start_chatbot_button"):
         st.session_state.chatbot_session = True
         st.experimental_rerun()

def stop_chatbot_session():
    if "chatbot_session" not in st.session_state:
         st.session_state.chatbot_session = False

    if st.button("Stop ChatBOT", key="stop_chatbot_button", help = "Click on button to close the BOT"):
         st.session_state.chatbot_session = False
         st.experimental_rerun()

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
        account_name = "aimlb54a"
        account_key = "74Fl0ShFyDmQRsP6rOP2MSPzxKt0xQC0tawDVYxhucGRIwkHwYy52DhdKOac0QD7DpEykxIyunba+AStMgoQSg=="
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
    # st.set_page_config(layout="wide")
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
        st.markdown("<h4 style='text-align: center;'>Test Your Invoice Documents</h4>", unsafe_allow_html=True)
    
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
                            account_name = "aimlb54a"
                            account_key = "74Fl0ShFyDmQRsP6rOP2MSPzxKt0xQC0tawDVYxhucGRIwkHwYy52DhdKOac0QD7DpEykxIyunba+AStMgoQSg=="
                            container_name = "input"
                            api_endpoint = 'https://invoice-processing-app9.azurewebsites.net/api/InvoiceProcessing'

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

                            response = requests.get(url=api_endpoint, verify=False)
                            if response.status_code == 200:
                                st.markdown(f"<span style='color:green'>Your RESULTS are ready!</span>", unsafe_allow_html=True)
                    
                            else:
                                st.error(f"API call failed with status code: {response.status_code}") 

                            image_url = "https://www.bing.com/th?id=OIP.Cs_MQcODAFqjh_mqsHuJ5wHaHa&w=179&h=185&c=8&rs=1&qlt=90&o=6&pid=3.1&rm=2"
                            
                            approve.markdown('<div style="text-align: center;" class="image-container">Upload File<img src="' + image_url + '" width="3%" />Processing & Fraud Detection..........Fraud Correction</div>', unsafe_allow_html=True)
                            
        elif selected == "📷 Camera":
            uploaded_files =st.empty()
            picture = st.camera_input("Capture an Image", key="camera_input")
        
            if picture:
                uploaded_files = [picture] 

            if st.button("Upload Image"):
                delete_files_from_container()
                # Upload the captured image
                account_name = "aimlb54a"
                account_key = "74Fl0ShFyDmQRsP6rOP2MSPzxKt0xQC0tawDVYxhucGRIwkHwYy52DhdKOac0QD7DpEykxIyunba+AStMgoQSg=="
                container_name = "input"
                api_endpoint = 'https://invoice-processing-app9.azurewebsites.net/api/InvoiceProcessing'
                for uploaded_file in uploaded_files:
                    file_path = f"./{uploaded_file.name}"
                    with open(file_path, "wb") as f:
                        f.write(uploaded_file.read())
                uploaded_url = upload_to_azure_container(account_name, account_key, container_name, file_path)
                if uploaded_url:
                    st.success("Image uploaded successfully!")
                else:
                    st.error("Failed to upload image.")

                response = requests.get(url=api_endpoint, verify=False)
                if response.status_code == 200:
                    st.markdown(f"<span style='color:green'>Your RESULTS are ready!</span>", unsafe_allow_html=True)
                    
                else:
                    st.error(f"API call failed with status code: {response.status_code}") 

                image_url = "https://www.bing.com/th?id=OIP.Cs_MQcODAFqjh_mqsHuJ5wHaHa&w=179&h=185&c=8&rs=1&qlt=90&o=6&pid=3.1&rm=2"
                        
                approve.markdown('<div style="text-align: center;" class="image-container">Upload File<img src="' + image_url + '" width="3%" />Processing & Fraud Detection..........Fraud Correction</div>', unsafe_allow_html=True)

        st.markdown("------------------------------------------------------------------------")
        # Location 2: Display container if session is started
        
        if st.session_state.get("chatbot_session", False):
        
            # Display scrollable container
            with st.container(height = 500):
                # st.title("☃️ Devika")

                # if "messages" not in st.session_state:
                #     st.session_state.messages = [{"role": "system", "content": get_system_prompt()}]
            
                # for message in st.session_state.messages:
                #     if message["role"] == "system":
                #         continue
                #     with st.chat_message(message["role"]):
                #         st.write(message["content"])
            
                # if st.session_state.messages[-1]["role"] != "assistant":
                #     with st.chat_message("assistant"):
                #         response = generate_response(OpenAI(api_key=st.secrets.OPENAI_API_KEY), st.session_state.messages)
                #         if response is not None:
                #             st.markdown(response)
                #             handle_response(response)
            
                # if prompt := st.chat_input():
                #     st.session_state.messages.append({"role": "user", "content": prompt})
                            
            # Code for Chatbot

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
            stop_chatbot_session()
           


    with col3:
        # st.markdown("**Invoice Discrepancies Summary**")
        st.markdown("<h4 style='text-align: center;'>Invoice Fraud Summary</h4>", unsafe_allow_html=True)

        st.markdown("This section will get you a Summary of Invoice Inconsistencies and an editable table to handle those inconsistences. Click on the below button to refresh the Summary Data and Table Preview " )
 
        st.markdown("")
        fraudlent = st.button("Click to refresh Fraud Rules and their Details")

        selecteds = option_menu(
            menu_title = None, 
            options = ["Invoice Discrepancies Summary", "Table Preview of Invoice discrepancies"], 
            menu_icon="cast", 
            #default_index= 0,
            orientation= "horizontal",
        )
        if selecteds == "Invoice Discrepancies Summary":
            if fraudlent:
                fetch_data()
                fetch_SF_data()

                image_url = "https://www.bing.com/th?id=OIP.Cs_MQcODAFqjh_mqsHuJ5wHaHa&w=179&h=185&c=8&rs=1&qlt=90&o=6&pid=3.1&rm=2"
                        
                approve.markdown('<div style="text-align: center;" class="image-container">Upload File<img src="' + image_url + '" width="3%" />Processing & Fraud Detection<img src="' + image_url + '" width="3%" />Fraud Correction</div>', unsafe_allow_html=True)

            # Fetch latest data from Snowflake
            total_duplicate_records, null_sub_total_records, duplicate_vendor_name_records, total_tax_out_of_range_records, total_records = fetch_SF_data()

            # Create DataFrame for double bar graph
            df = pd.DataFrame({
                'Business Rule': ['Duplicate Records (Invoice ID and Invoice Date)', 'Null Amount Records', 'Duplicate Vendor Names', 'Total Tax Out of Range Records', 'Total Records'],
                'Count': [total_duplicate_records, null_sub_total_records, duplicate_vendor_name_records, total_tax_out_of_range_records, total_records],
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

        start_chatbot_session()

        # Publish Button
        with stylable_container(
        key="publish_button",
        css_styles="""
            button {
                
                background-color: red;
                color: white;
                border-radius: 5px;
                padding:
            }
            """,
    ):
            if st.button("Publish all changes to Final Table", help="Click to publish data", key="publish_button"):
                publish_data()

        if st.session_state.get("publish_success", False):
            success_message_style = """
            <style>
            .success-message {
             
                color: #2E8B57;
            }
            </style>
            """
            st.markdown(success_message_style, unsafe_allow_html=True)
            st.markdown('<p class="success-message">Published all records successfully!</p>', unsafe_allow_html=True)





if __name__ == "__main__":
    main()
    
st.markdown("------------------------------------------------------------------------------------------------------------------------------------")

