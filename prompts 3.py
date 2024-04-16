import streamlit as st

SCHEMA_PATH = st.secrets.get("SCHEMA_PATH", "DFS.DEMO")
QUALIFIED_TABLE_NAME = f"{SCHEMA_PATH}.VW_INVOICES "
TABLE_DESCRIPTION = """
This table has various metrics for financial entities (also referred to as banks) since 1983.
The user may describe the entities interchangeably as banks, financial institutions, or financial entities.
"""
# This query is optional if running Frosty on your own table, especially a wide table.
# Since this is a deep table, it's useful to tell Frosty what variables are available.
# Similarly, if you have a table with semi-structured data (like JSON), it could be used to provide hints on available keys.
# If altering, you may also need to modify the formatting logic in get_table_context() below.
METADATA_QUERY = f"SELECT COLUMN_NAME, DEFINITION FROM {SCHEMA_PATH}.TABLE_METADATA;"

GEN_SQL = """
You will be acting as an AI Snowflake SQL Expert named Devika.
Your goal is to give correct, executable sql query to users, or create charts if requested.
You will be replying to users who will be confused if you don't respond in the character of Devika.
You are given one table, the table name is in <tableName> tag, the columns are in <columns> tag.
The user will ask questions, for each question you should respond and include a sql query based on the question and the table. 

{context}

Here are 6 critical rules for the interaction you must abide:
<rules>
1. You MUST MUST wrap the generated sql code within ``` sql code markdown in this format e.g
```sql
(select 1) union (select 2)
```
2. If I don't tell you to find a limited set of results in the sql query or question, you MUST limit the number of responses to 10.
3. Text / string where clauses must be fuzzy match e.g ilike %keyword%
4. Make sure to generate a single snowflake sql code, not multiple. 
5. You should only use the table columns given in <columns>, and the table given in <tableName>, you MUST NOT hallucinate about the table names
6. DO NOT put numerical at the very front of sql variable.
</rules>

Don't forget to use "ilike %keyword%" for fuzzy match queries
and wrap the generated sql code with ``` sql code markdown in this format e.g:
```sql
(select 1) union (select 2)
```
Be strict with the column names, take them from TABLE_METADATA table. I repeat, dont make mistakes in puling the column names in SQL, else the SQL will fail.
For each question from the user, make sure to include a query in your response.

If asked to create chart then please create the request chart instead of SQL or table. 

Now to get started, please briefly introduce yourself, describe the table at a high level, and share the available metrics in 2-3 sentences.
Then show only below 2 example questions for reference without changin gthe text, don't run them, and dont show the SQL while showing the example.  I repeat, show only the below 2 example -
1. Identify duplicate invoices based on a combination of InvoiceId and Invoice Total
2. Verify VAT compliance by checking if the VAT Amount is correctly calculated within the expected VAT rates (8%, 12%, 18%)
"""

@st.cache_data(show_spinner="Loading Devika's context...")
def get_table_context(table_name: str, table_description: str, metadata_query: str = None):
    table = table_name.split(".")
    conn = st.connection("snowflake")
    columns = conn.query(f"""
        SELECT COLUMN_NAME, DATA_TYPE FROM {table[0].upper()}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{table[1].upper()}' AND TABLE_NAME = 'STG_INVOICES'
        """, show_spinner=False,
    )
    columns = "\n".join(
        [
            f"- **{columns['COLUMN_NAME'][i]}**: {columns['DATA_TYPE'][i]}"
            for i in range(len(columns["COLUMN_NAME"]))
        ]
    )
    context = f"""
Here is the table name <tableName> {'.'.join(table)} </tableName>

<tableDescription>{table_description}</tableDescription>

Here are the columns of the {'.'.join(table)}

<columns>\n\n{columns}\n\n</columns>
    """
    if metadata_query:
        metadata = conn.query(metadata_query, show_spinner=False)
        metadata = "\n".join(
            [
                f"- **{metadata['COLUMN_NAME'][i]}**: {metadata['DEFINITION'][i]}"
                for i in range(len(metadata["COLUMN_NAME"]))
            ]
        )
        context = context + f"\n\nAvailable variables by COLUMN_NAME:\n\n{metadata}"
    return context

def get_system_prompt():
    table_context = get_table_context(
        table_name=QUALIFIED_TABLE_NAME,
        table_description=TABLE_DESCRIPTION,
        metadata_query=METADATA_QUERY
    )
    return GEN_SQL.format(context=table_context)

# do `streamlit run prompts.py` to view the initial system prompt in a Streamlit app
if __name__ == "__main__":
    st.header("System prompt for Devika")
    st.markdown(get_system_prompt())