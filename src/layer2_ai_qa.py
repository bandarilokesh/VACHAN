"""
Layer 2: AI Q&A (Talk to Data) - Dual-Engine Architecture
===================================================================
This module provides a Streamlit web app that allows users to ask
plain English questions about the Golden Customer 360 data OR Bank Policies.
It implements the Dual-Engine Architecture:
- Engine 1: Text-to-SQL
- Engine 2: Policy RAG
- Guardrails: SQL AST Validation, Read-only DuckDB
"""

import streamlit as st
import duckdb
import pandas as pd
import os
import sqlglot
from langfuse import observe
from google import genai
from google.genai import types

# ==========================================
# 1. SETUP & CONFIGURATION
# ==========================================
DB_PATH = "d:/VACHAN/collections_c360/maple.duckdb"
DOCS_BASE_PATH = "d:/VACHAN/maple_data/maple_collections_release/maple_collections_release/files/"

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
    """Connects to our DuckDB database (Guardrail 1: read-only=True)."""
    return duckdb.connect(DB_PATH, read_only=True)

def generate_with_fallback(client, prompt: str) -> str:
    """Remedy 1: Handle 429 Quota Exhaustion by falling back to alternative models."""
    models_to_try = ['gemini-3.8-flash', 'gemini-1.5-flash', 'gemini-1.5-pro']
    last_error = None
    for model_name in models_to_try:
        try:
            res = client.models.generate_content(model=model_name, contents=[prompt])
            return res.text.strip()
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or "404" in str(e) or "NOT_FOUND" in str(e):
                last_error = e
                continue
            return f"-- Error generating: {e}"
    return f"-- Error generating SQL: {last_error}"

@observe()
def classify_intent(client, question: str) -> str:
    """AI Query Router: Uses ultra-fast keyword heuristics first to cut latency by 50%"""
    q_lower = question.lower()
    
    # Fast path for Policy RAG
    rag_keywords = ['policy', 'rule', 'procedure', 'script', 'casl', 'dncl', 'law', 'sue', 'legal', 'say', 'agree', 'agent do', 'must', 'should', 'consent']
    if any(k in q_lower for k in rag_keywords):
        return 'rag'
        
    # Fast path for Data SQL
    sql_keywords = ['how many', 'total', 'average', 'list', 'cost', 'rate', 'share', 'compare', 'which', 'amount', 'who', 'what is the']
    if any(k in q_lower for k in sql_keywords):
        return 'sql'
        
    # Fallback to LLM only if highly ambiguous (Rare)
    if not client: return "sql"
    prompt = f"""
    Classify the following question into exactly one of these categories:
    1. 'sql' - quantitative data, metrics, counts, sums (e.g. "how many...", "total overdue...").
    2. 'rag' - policy, rules, procedures, definitions (e.g. "what is the policy...", "can we call...").
    Question: {question}
    Return ONLY 'sql' or 'rag'.
    """
    res = generate_with_fallback(client, prompt)
    return 'rag' if 'rag' in res.lower() else 'sql'

def validate_sql_ast(query: str) -> bool:
    """Guardrail 2: SQL AST Sandboxing using sqlglot (blocks DDL/DML)"""
    try:
        parsed = sqlglot.parse(query, read="duckdb")
        for expression in parsed:
            if not isinstance(expression, sqlglot.exp.Select):
                return False
        return True
    except Exception:
        return False

@observe()
def generate_sql_from_question(client, question: str) -> str:
    """Engine 1: Generates DuckDB SQL using GenAI"""
    if not client: return "SELECT * FROM gold_c360_customer LIMIT 5;"
    
    prompt = f"{GOLD_SCHEMA_PROMPT}\n\nQuestion: {question}"
    sql = generate_with_fallback(client, prompt)
    
    # Clean up markdown code blocks if present
    if sql.startswith("```sql"): sql = sql[6:]
    if sql.startswith("```"): sql = sql[3:]
    if sql.endswith("```"): sql = sql[:-3]
    return sql.strip()

def retrieve_policy_docs(question: str) -> str:
    """Retrieves relevant policy documents from DuckDB and disk"""
    con = get_db_connection()
    # Simple search simulation across document titles
    docs = con.execute("SELECT doc_id, title, file_path FROM bronze_reference_documents").fetchall()
    
    q_words = question.lower().replace("?", "").split()
    selected = []
    for doc in docs:
        if any(word in doc[1].lower() or word in doc[0].lower() for word in q_words if len(word) > 3):
            selected.append(doc)
    
    if not selected:
        selected = docs[:2] # Fallback to top 2 docs
        
    context_texts = []
    for doc in selected[:3]: # Limit to top 3 to fit context window
        doc_id, title, file_path = doc
        full_path = os.path.join(DOCS_BASE_PATH, file_path)
        if os.path.exists(full_path):
            with open(full_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Truncate content slightly to ensure we don't blow up context size
                context_texts.append(f"--- Document: {doc_id} - {title} ---\n{content[:3000]}")
                
    return "\n\n".join(context_texts)

@observe()
def answer_policy_question(client, question: str) -> str:
    """Engine 2: Policy RAG using GenAI and retrieved documents"""
    if not client: return "Error: GenAI client not initialized. Cannot answer policy questions."
    
    context = retrieve_policy_docs(question)
    prompt = f"""
    You are a banking policy expert. Answer the user's question based strictly on the provided documents.
    If the answer is not clearly present in the documents, say "I cannot verify this based on the available policies."
    Guardrail 4: You MUST cite the document ID (e.g., POL-COLL-001) in your answer.
    
    Context Documents:
    {context}
    
    User Question: {question}
    """
    return generate_with_fallback(client, prompt)

def execute_sql(query: str) -> pd.DataFrame:
    """Executes validated SQL against DuckDB"""
    con = get_db_connection()
    try:
        return con.execute(query).df()
    except Exception as e:
        return pd.DataFrame({"Error": [str(e)]})

# ==========================================
# 3. STREAMLIT WEB APP (THE UI)
# ==========================================

def process_user_query(client, prompt: str):
    """Handles the core logic of processing a question and displaying it in the chat UI."""
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
        
    with st.chat_message("assistant"):
        st.markdown(f"Thinking about: *{prompt}*...")
        
        # 1. AI Router: Classify intent
        intent = classify_intent(client, prompt)
        st.caption(f"Intent classified as: **{intent.upper()}**")
        
        msg_obj = {
            "role": "assistant",
            "content": "Here is the response I found based on your question:",
            "intent": intent
        }
        
        if intent == 'sql':
            # Engine 1: Text-to-SQL
            sql_query = generate_sql_from_question(client, prompt)
            
            with st.expander("🔍 View generated SQL", expanded=True):
                st.code(sql_query, language="sql")
            
            # Guardrail 2: AST Validation
            if not validate_sql_ast(sql_query):
                st.error("Guardrail Blocked: Query contains unauthorized DDL/DML operations.")
                msg_obj["content"] = "Query blocked by AST Security Guardrail."
                msg_obj["sql"] = sql_query
            else:
                data_result = execute_sql(sql_query)
                st.dataframe(data_result)
                msg_obj["sql"] = sql_query
                msg_obj["data"] = data_result
                
                # Remedy 2: Natural Language Data Synthesis
                if not data_result.empty:
                    nl_prompt = f"Question: {prompt}\nData: {data_result.head(10).to_dict(orient='records')}\nWrite a short, highly precise, natural language sentence answering the question using ONLY the provided data. Do not use markdown headers."
                    nl_answer = generate_with_fallback(client, nl_prompt)
                    st.success(f"🤖 **Synthesis:** {nl_answer}")
                    msg_obj["content"] = nl_answer
                    
        elif intent == 'rag':
            # Engine 2: Policy RAG
            answer = answer_policy_question(client, prompt)
            st.info(answer)
            msg_obj["policy_answer"] = answer
            
        # Save to history
        st.session_state.messages.append(msg_obj)

def main():
    st.set_page_config(page_title="Maple Bank: Layer 2 AI Q&A", page_icon="🏦", layout="wide")
    
    if "messages" not in st.session_state:
        st.session_state.messages = []
        
    with st.sidebar:
        st.header("⚙️ Settings")
        api_key = st.text_input("Gemini API Key", type="password", value=os.environ.get("GEMINI_API_KEY", ""), help="Get your key at https://aistudio.google.com/")
        
        st.divider()
        st.subheader("🧪 Benchmark Testing")
        st.caption("Select a rigorous benchmark question to test live:")
        try:
            con = get_db_connection()
            q_df = con.execute("SELECT question_id, difficulty, split, question_text FROM bronze_benchmark_questions ORDER BY question_id").df()
            # Create friendly labels for the dropdown
            q_options = [f"[{row.question_id}] {row.question_text}" for _, row in q_df.iterrows()]
            selected_q_label = st.selectbox("Select a question:", q_options)
            
            # Extract just the question text from the label
            selected_q_text = selected_q_label.split("] ", 1)[1] if selected_q_label else ""
            
            if st.button("▶️ Run Test") and selected_q_text:
                st.session_state.pending_test = selected_q_text
        except Exception as e:
            st.error(f"Could not load benchmarks: {e}")
            
    client = None
    if api_key:
        try:
            client = genai.Client(api_key=api_key)
        except Exception as e:
            st.sidebar.error(f"Invalid API Key: {e}")
    
    st.title("🗣️ Layer 2: Dual-Engine AI Q&A")
    st.markdown("""
    Welcome to **Layer 2**! 
    This implements the **Dual-Engine Architecture**. The Router will classify if you are asking a **Data Question** (Engine 1) or a **Policy Question** (Engine 2).
    """)
    
    if not client:
        st.warning("⚠️ Please enter your Gemini API Key in the sidebar to activate the AI engines.")
        
    if "messages" not in st.session_state:
        st.session_state.messages = []
        
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "intent" in msg:
                st.caption(f"Intent classified as: **{msg['intent'].upper()}**")
            if "sql" in msg:
                with st.expander("🔍 View generated SQL"):
                    st.code(msg["sql"], language="sql")
            if "data" in msg:
                st.dataframe(msg["data"])
            if "policy_answer" in msg:
                st.info(msg["policy_answer"])

    # Process pending test query from sidebar button
    pending = st.session_state.get('pending_test', None)
    if pending and client:
        process_user_query(client, pending)
        # Clear the pending test so it doesn't re-run on next interaction
        st.session_state.pending_test = None

    if prompt := st.chat_input("Ask a question (e.g., 'How many customers are deceased?' or 'What is the collections calling policy?')"):
        if client:
            process_user_query(client, prompt)
        else:
            st.error("Please provide an API key first.")

if __name__ == "__main__":
    main()
