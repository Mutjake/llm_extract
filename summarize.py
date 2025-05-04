import duckdb
import requests

# Connect to the DuckDB database
con = duckdb.connect('pdf_data.db')

# Fetch all text data
result = con.execute("SELECT text FROM text_data").fetchall()
text_data = " ".join(row[0] for row in result if row[0])

# Use Ollama's all-minilm:22m model for summarization
response = requests.post(
    "http://localhost:11434/api/generate",
    json={
        "model": "all-minilm:22m",
        "prompt": f"Summarize the following text into a single paragraph:\n\n{text_data}"
    }
)

# Check if the request was successful
if response.status_code == 200:
    summary = response.json().get("response", "No summary generated.")
    print("Summary:\n", summary)
else:
    print("Failed to generate summary. Status code:", response.status_code)
    print("Response:", response.text)