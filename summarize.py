import duckdb
from gemma import Gemma

# Connect to the DuckDB database
con = duckdb.connect('pdf_data.db')

# Fetch all text data
result = con.execute("SELECT text FROM text_data").fetchall()
text_data = " ".join(row[0] for row in result if row[0])

# Use Gemma to summarize the text
gemma = Gemma()
summary = gemma.summarize(text_data)

# Print the summary
print("Summary:\n", summary)