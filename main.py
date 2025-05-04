import pdfplumber
import duckdb
import os
import requests
import json
import pickle

con = duckdb.connect('pdf_data.db')
# Create a table to store the extracted data
con.execute('''
CREATE TABLE IF NOT EXISTS pdf_data (
    original_url TEXT,
    text TEXT,
    images BLOB,
    tables JSON,
    metadata JSON,
    annotations JSON,
    hyperlinks JSON
)
''')    

pdf_temporary_files_list = []
# Read a newline separated list of PDF file urls from sources.txt

with open('sources.txt', 'r') as file:
    pdf_url_list = file.read().splitlines()
    # Download each file into a temporary file and append the file path to the pdf_temporary_files_list
    for url in pdf_url_list:
        response = requests.get(url)
        # Save the PDF to a temporary file
        temp_file = f"temp_{url.split('/')[-1]}"
        with open(temp_file, 'wb') as temp_pdf:
            temp_pdf.write(response.content)
        pdf_temporary_files_list.append((temp_file, url,))

for pdf_tuple in pdf_temporary_files_list:
    with pdfplumber.open(pdf_tuple[0]) as pdf:
        for page in pdf.pages:
            orig_url = pdf_tuple[1]
            # Extract text from the page
            text = page.extract_text() or None
            # Serialize images as binary data
            images = pickle.dumps(page.images) if page.images else None
            # Convert tables to JSON string
            tables = json.dumps(page.extract_tables()) if page.extract_tables() else None
            # Convert metadata to JSON string
            metadata = json.dumps(pdf.metadata) if pdf.metadata else None
            # Convert annotations to JSON string
            annotations = json.dumps(page.annots) if page.annots else None
            # Convert hyperlinks to JSON string
            hyperlinks = json.dumps(page.hyperlinks) if page.hyperlinks else None
            # Insert the extracted data into the DuckDB database
            con.execute('''
            INSERT INTO pdf_data (original_url, text, images, tables, metadata, annotations, hyperlinks)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (orig_url, text, images, tables, metadata, annotations, hyperlinks))

# Close the connection
con.close()

# Delete the temporary files
for pdf_tuple in pdf_temporary_files_list:
    os.remove(pdf_tuple[0])

# Return the database file path
print("Database file created:", os.path.abspath('pdf_data.db'))