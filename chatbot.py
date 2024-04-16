from openai import OpenAI
import snowflake.connector
import os
import re
import streamlit as st
from prompts import get_system_prompt

from streamlit import session_state
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
 
# # Initialize Azure Key Vault client
# vault_uri = "https://akv-invoices.vault.azure.net/"
# credential = DefaultAzureCredential()
# secret_client = SecretClient(vault_uri, credential)


# Snowflake Connection Details
# process.env.{ENVIRONMENT_VARIABLE_NAME} )
# snowflake_user = os.environ.get("SF_USER")
# snowflake_password = os.environ.get("SF_PASSWORD")
# snowflake_account = os.environ.get("SF_ACCOUNT")
# snowflake_warehouse = os.environ.get("SF_WAREHOUSE")
# snowflake_database = os.environ.get("SF_DATABASE")
# snowflake_schema = os.environ.get("SF_SCHEMA")
# snowflake_table = os.environ.get("SF_TABLE")




def chatbot():
    #Code for Chatbot

            ###
                st.title("☃️ Devika")
                client = OpenAI(api_key=st.secrets.OPENAI_API_KEY)
                # client = OpenAI(api_key= os.environ.get("OPEN_AI_KEY"))
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
def main():
     chatbot()

if __name__ == "__main__":
     main()
