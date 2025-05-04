import duckdb
from langchain_community.llms import Ollama
llm = Ollama(model="gemma2")


# Connect to the DuckDB database
con = duckdb.connect('pdf_data.db')

# Fetch all text data
result = con.execute("SELECT text FROM text_data").fetchall()
text_data = " ".join(row[0] for row in result if row[0])

summary = llm.invoke(f"Please summarize into a paragraph the following text: {text_data}")
# Print the summary
print("Summary:\n", summary)