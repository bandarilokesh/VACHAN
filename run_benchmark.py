import duckdb
import pandas as pd
import sys
import os

# Add src to path to import our modules
sys.path.append('d:/VACHAN/src')
from layer2_ai_qa import classify_intent, generate_sql_from_question, execute_sql, answer_policy_question
from google import genai

# Setup client using the key from env
API_KEY = os.environ.get("GEMINI_API_KEY", "")
client = genai.Client(api_key=API_KEY) if API_KEY else None

# Load questions and answers
answers_df = pd.read_csv(r'd:\VACHAN\maple_data\maple_collections_release\maple_collections_release\labels\benchmark_dev_answers.csv')
con = duckdb.connect('d:/VACHAN/collections_c360/maple.duckdb', read_only=True)
q_df = con.execute("SELECT question_id, split, question_text FROM bronze_benchmark_questions ORDER BY question_id").df()
merged = pd.merge(q_df, answers_df, on='question_id', how='inner')

# Process all questions
test_set = merged

output_md = "# Benchmark Evaluation Report\n\n"
output_md += "Comparison of Actual vs Expected Outputs for the Dual-Engine Architecture.\n\n"

print(f"Running evaluation on {len(test_set)} questions...")

for idx, row in test_set.iterrows():
    qid = row['question_id']
    question = row['question_text']
    expected_answer = row['gold_answer']
    expected_sql = row['gold_sql_reference']
    
    print(f"Processing {qid}...")
    
    intent = classify_intent(client, question)
    
    actual_sql = "N/A"
    actual_answer = "N/A"
    
    if intent == 'sql':
        actual_sql = generate_sql_from_question(client, question)
        # Execute it to get a peek at the result
        try:
            df_res = execute_sql(actual_sql)
            if not df_res.empty:
                raw_data = df_res.head(10).to_dict(orient='records')
                from layer2_ai_qa import generate_with_fallback
                nl_prompt = f"Question: {question}\nData: {raw_data}\nWrite a short, highly precise, natural language sentence answering the question using ONLY the provided data. Do not use markdown headers."
                actual_answer = generate_with_fallback(client, nl_prompt)
            else:
                actual_answer = "Empty Result"
        except Exception as e:
            actual_answer = f"Error: {e}"
            
    elif intent == 'rag':
        actual_answer = answer_policy_question(client, question)
        
    output_md += f"## {qid}: {question}\n"
    output_md += f"**Intent:** `{intent.upper()}`\n\n"
    
    if intent == 'sql':
        output_md += "### Expected SQL\n```sql\n" + str(expected_sql) + "\n```\n"
        output_md += "### Actual SQL Generated\n```sql\n" + str(actual_sql) + "\n```\n"
        output_md += f"**Data Returned:** {actual_answer}\n\n"
    else:
        output_md += f"**Expected Answer:** {expected_answer}\n\n"
        output_md += f"**Actual Answer:** {actual_answer}\n\n"
        
    output_md += "---\n"

with open(r'C:\Users\Lokesh Vardhan\.gemini\antigravity-ide\brain\69face51-0804-4572-8390-6759bea595ce\benchmark_evaluation_results.md', 'w', encoding='utf-8') as f:
    f.write(output_md)
    
print("Evaluation complete. Results written to artifact.")
