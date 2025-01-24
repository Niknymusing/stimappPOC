import os
import openai
import streamlit as st
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

##############################################################################
# 1. CONFIGURATION
##############################################################################

# Make sure you have the openai package installed: pip install openai
# And streamlit: pip install streamlit
# Then run with: streamlit run streamlit_app.py

# Set your OpenAI API key (recommended: set as environment variable)
load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY") 
# If you have not set the key as an env variable,
# replace "YOUR_OPENAI_KEY_HERE" with your actual key, or do:
# openai.api_key = "sk-XXXX..."

##############################################################################
# 2. DEMO DATABASE SETUP (SQLite in-memory)
##############################################################################

def create_demo_db():
    """
    Creates an in-memory SQLite database with mock STIM-like tables:
      - works
      - contributors
      - royalties
      - (optional) work_contributors for multi-writer splits

    Returns a SQLAlchemy engine connected to this in-memory DB.
    """
    # Create in-memory SQLite engine
    engine = create_engine("sqlite://", echo=False)

    # Create tables
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE works (
                work_id INTEGER PRIMARY KEY,
                title TEXT,
                created_year INTEGER
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE contributors (
                contributor_id INTEGER PRIMARY KEY,
                name TEXT,
                is_publisher BOOLEAN
            )
        """))
        
        # Many-to-many table for works <-> contributors (to handle co-writers, etc.)
        conn.execute(text("""
            CREATE TABLE work_contributors (
                work_id INTEGER,
                contributor_id INTEGER,
                share_percentage REAL,
                FOREIGN KEY(work_id) REFERENCES works(work_id),
                FOREIGN KEY(contributor_id) REFERENCES contributors(contributor_id)
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE royalties (
                royalty_id INTEGER PRIMARY KEY,
                work_id INTEGER,
                amount NUMERIC,
                period_start TEXT,
                period_end TEXT,
                FOREIGN KEY(work_id) REFERENCES works(work_id)
            )
        """))

        # Insert some demo data
        # Works
        conn.execute(text("""
            INSERT INTO works (work_id, title, created_year)
            VALUES
            (1, 'Dancing Queen', 1976),
            (2, 'Mamma Mia', 1975),
            (3, 'Fernando', 1976),
            (4, 'Waterloo', 1974)
        """))
        
        # Contributors
        conn.execute(text("""
            INSERT INTO contributors (contributor_id, name, is_publisher)
            VALUES
            (101, 'Benny Andersson', 0),
            (102, 'Björn Ulvaeus', 0),
            (103, 'Polar Music', 1),
            (104, 'ABBA Manager', 1)
        """))
        
        # Work-Contributors (splits)
        conn.execute(text("""
            INSERT INTO work_contributors (work_id, contributor_id, share_percentage)
            VALUES
            (1, 101, 50.0),
            (1, 102, 50.0),
            (2, 101, 40.0),
            (2, 102, 40.0),
            (2, 103, 20.0),
            (3, 101, 33.33),
            (3, 102, 33.33),
            (3, 103, 33.34),
            (4, 101, 25.0),
            (4, 102, 25.0),
            (4, 103, 25.0),
            (4, 104, 25.0)
        """))
        
        # Royalties
        conn.execute(text("""
            INSERT INTO royalties (royalty_id, work_id, amount, period_start, period_end)
            VALUES
            (1001, 1, 1500.00, '2022-01-01', '2022-06-30'),
            (1002, 1, 1700.00, '2022-07-01', '2022-12-31'),
            (1003, 2, 2200.00, '2022-01-01', '2022-12-31'),
            (1004, 3, 1800.00, '2022-01-01', '2022-06-30'),
            (1005, 3, 1900.00, '2022-07-01', '2022-12-31'),
            (1006, 4, 3000.00, '2022-01-01', '2022-12-31')
        """))

    return engine

##############################################################################
# 3. LLM (OpenAI) HELPER: GENERATE SQL FROM USER QUERY
##############################################################################
from openai import OpenAI

def generate_sql_query(user_question: str, schema_description: str, api_key: str) -> str:
    client = OpenAI(api_key=api_key)
    
    prompt = f"""
You are an expert SQL generator. Generate a query following this structure:

WITH
-- Common Table Expressions (CTEs) for complex subqueries
base_data AS (
    -- Initial data gathering
),
transformed_data AS (
    -- Data transformations
)

-- Main query
SELECT 
    -- Columns with clear aliases
    column AS meaningful_name
FROM base_data
-- JOINs in logical order
LEFT JOIN other_table ON conditions
-- Filtering
WHERE conditions
-- Grouping
GROUP BY columns
-- Having clauses for group filtering
HAVING conditions
-- Final ordering
ORDER BY columns;

Schema:
{schema_description}

User question: {user_question}

Return ONLY the raw SQL query, no markdown formatting, no ```sql tags, no backticks."""

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are an expert SQL query generator that produces clean, structured queries without any markdown formatting."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )
    
    sql_code = response.choices[0].message.content.strip()
    # Remove any markdown formatting if present
    sql_code = sql_code.replace('```sql', '').replace('```', '')
    return sql_code.strip()



##############################################################################
# 4. MAIN STREAMLIT APP
##############################################################################
def main():
    st.title("STIM Query Assistant")
    
    api_key = openai.api_key
    if not api_key:
        api_key = st.text_input("Enter OpenAI API Key:", type="password")
        if not api_key:
            st.warning("Please enter an OpenAI API key to continue.")
            return

    engine = create_demo_db()
    
    schema_text = """
Tables:

1) works(
    work_id INTEGER PRIMARY KEY,
    title TEXT,
    created_year INTEGER
)

2) contributors(
    contributor_id INTEGER PRIMARY KEY,
    name TEXT,
    is_publisher BOOLEAN
)

3) work_contributors(
    work_id INTEGER,
    contributor_id INTEGER,
    share_percentage REAL
)

4) royalties(
    royalty_id INTEGER PRIMARY KEY,
    work_id INTEGER,
    amount NUMERIC,
    period_start TEXT,
    period_end TEXT
)

Relationships:
- works <-> work_contributors: 1-to-many (one work can have multiple entries in work_contributors)
- contributors <-> work_contributors: 1-to-many (one contributor can appear on multiple works)
- works <-> royalties: 1-to-many (one work can have multiple royalty entries)
"""

    st.markdown("""
**Instructions**:  
1. Type a question about the catalog (e.g., "Show total royalties for each work in 2022").  
2. Click "Run Query" to generate and execute SQL automatically.  
3. Review the results below.
    """)

    user_query = st.text_input("Ask a question about the catalog:")

    if st.button("Run Query"):
        if not user_query.strip():
            st.warning("Please enter a question before running the query.")
        else:
            with st.spinner("Generating SQL via OpenAI..."):
                sql_code = generate_sql_query(user_query, schema_text, api_key)
            
            st.subheader("Generated SQL Query")
            st.code(sql_code, language="sql")
            
            try:
                with engine.connect() as conn:
                    df = pd.read_sql(text(sql_code), conn)
                st.subheader("Query Results")
                if df.empty:
                    st.write("No rows returned.")
                else:
                    st.dataframe(df)
                    numeric_cols = df.select_dtypes(include=['int64','float64']).columns
                    if len(numeric_cols) > 0:
                        st.bar_chart(df[numeric_cols])
            except Exception as e:
                st.error(f"Error executing SQL: {e}")

    st.markdown("---")
    st.markdown("**Demo database**: This is only a mock in-memory DB with ABBA-themed data. In production, connect to STIM's real schema.")

if __name__ == "__main__":
    main()