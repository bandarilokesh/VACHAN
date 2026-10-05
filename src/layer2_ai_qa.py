"""
Layer 2: AI Q&A (Talk to Data) - Beginner-Friendly Implementation
===================================================================
This module provides a Streamlit web app that allows users to ask
plain English questions about the Golden Customer 360 data.
It translates natural language to DuckDB SQL using an LLM.
"""

import streamlit as st
import duckdb
import pandas as pd

# ==========================================
# 1. SETUP & CONFIGURATION
# ==========================================
# Pointing to the DuckDB file created in Layer 1
DB_PATH = "d:/VACHAN/collections_c360/maple.duckdb"

# We define the schema of our Gold tables so the AI knows what it can query.
# This acts as the "Brain" for our Text-to-SQL translation.
GOLD_SCHEMA_PROMPT = """
You are a banking data analyst. Write a DuckDB SQL query to answer the user's question.
Here is the schema of the Golden Customer 360 database:

Table: gold_c360_customer
- golden_id (VARCHAR): Unique customer ID (e.g., G-0000001)
- primary_crm_id (VARCHAR): Original CRM ID
- first_name (VARCHAR)
- last_name (VARCHAR)
- email (VARCHAR)
- phone (VARCHAR)
- consent_call (BOOLEAN): Can we call them?
- vulnerability_flag (BOOLEAN): Are they vulnerable?
- deceased_flag (BOOLEAN): Are they deceased?
- bankruptcy_flag (BOOLEAN): Have they filed for bankruptcy?

Table: gold_c360_accounts
- golden_id (VARCHAR): Joins to gold_c360_customer
- product_category (VARCHAR): 'card', 'loan', or 'deposit'
- account_id (VARCHAR): The specific account number
- current_balance (DOUBLE): Current balance of the account
- past_due_amount (DOUBLE): Amount past due
- dpd (INTEGER): Days past due

Table: gold_c360_cases
- golden_id (VARCHAR): Joins to gold_c360_customer
- case_id (VARCHAR): Collections case ID
- case_status (VARCHAR): e.g., 'open', 'closed'
- total_overdue (DOUBLE): Total overdue amount for this case

Return ONLY the raw SQL query. No markdown formatting or explanations.
"""

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

@st.cache_resource
def get_db_connection():
    """Connects to our DuckDB database built in Layer 1."""
    return duckdb.connect(DB_PATH, read_only=True)

def generate_sql_from_question(question: str) -> str:
    """
    Translates a plain English question into SQL using an AI model.
    NOTE: In this starter template, we use a mock for demonstration.
    To make it real, you would plug in OpenAI (gpt-4) or Google (Gemini) here.
    """
    # ---------------------------------------------------------
    # TODO: Replace with real LLM call!
    # Example using hypothetical google-genai:
    # response = client.models.generate_content(
    #     model='gemini-2.5-flash', 
    #     contents=[GOLD_SCHEMA_PROMPT, question]
    # )
    # return response.text
    # ---------------------------------------------------------
    
    # --- MOCK LOGIC FOR BEGINNER DEMO ---
    question = question.lower()
    if "how many customers" in question and "deceased" in question:
        return "SELECT COUNT(*) AS total_deceased FROM gold_c360_customer WHERE deceased_flag = true;"
    elif "total past due" in question or "overdue" in question:
        return "SELECT SUM(past_due_amount) AS total_past_due FROM gold_c360_accounts WHERE dpd > 0;"
    elif "vulnerable" in question:
        return "SELECT COUNT(*) AS vulnerable_count FROM gold_c360_customer WHERE vulnerability_flag = true;"
    else:
        return "SELECT * FROM gold_c360_customer LIMIT 5;"

def execute_sql(query: str) -> pd.DataFrame:
    """Runs the AI-generated SQL query against our DuckDB and returns a Pandas DataFrame."""
    con = get_db_connection()
    try:
        # Run query and fetch as dataframe
        df = con.execute(query).df()
        return df
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})

# ==========================================
# 3. STREAMLIT WEB APP (THE UI)
# ==========================================

def main():
    st.set_page_config(page_title="Maple Bank: Layer 2 AI Q&A", page_icon="🏦", layout="wide")
    
    st.title("🗣️ Layer 2: AI Q&A (Talk to Data)")
    st.markdown("""
    Welcome to **Layer 2**! 
    This app sits on top of our Golden Customer 360 database (Layer 1). 
    Instead of writing complex SQL queries, you can ask questions in **Plain English**.
    """)
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
        
    # Display chat history
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "sql" in msg:
                with st.expander("🔍 View generated SQL"):
                    st.code(msg["sql"], language="sql")
            if "data" in msg:
                st.dataframe(msg["data"])

    # Chat input
    if prompt := st.chat_input("Ask a question (e.g., 'How many customers are deceased?')"):
        # User message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
            
        # Assistant response
        with st.chat_message("assistant"):
            st.markdown(f"Thinking about: *{prompt}*...")
            
            # 1. Translate AI to SQL
            sql_query = generate_sql_from_question(prompt)
            
            # 2. Show the SQL to the user
            with st.expander("🔍 View generated SQL", expanded=True):
                st.code(sql_query, language="sql")
                
            # 3. Execute the SQL against DuckDB
            data_result = execute_sql(sql_query)
            
            # 4. Show the results
            st.dataframe(data_result)
            
            # Save to history
            st.session_state.messages.append({
                "role": "assistant",
                "content": "Here is the data I found based on your question:",
                "sql": sql_query,
                "data": data_result
            })

if __name__ == "__main__":
    main()
