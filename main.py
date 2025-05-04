import pdfplumber
import duckdb
import os
import requests
import json

con = duckdb.connect('pdf_data.db')

# Create separate tables for each extraction type
con.execute('''
CREATE TABLE IF NOT EXISTS text_data (
    original_url TEXT,
    page_number INTEGER,
    text TEXT
)
''')
con.execute('''
CREATE TABLE IF NOT EXISTS images_data (
    original_url TEXT,
    page_number INTEGER,
    image_data JSON
)
''')
con.execute('''
CREATE TABLE IF NOT EXISTS tables_data (
    original_url TEXT,
    page_number INTEGER,
    table_data JSON
)
''')
con.execute('''
CREATE TABLE IF NOT EXISTS metadata_data (
    original_url TEXT,
    metadata JSON
)
''')
con.execute('''
CREATE TABLE IF NOT EXISTS annotations_data (
    original_url TEXT,
    page_number INTEGER,
    annotation_data JSON
)
''')
con.execute('''
CREATE TABLE IF NOT EXISTS hyperlinks_data (
    original_url TEXT,
    page_number INTEGER,
    hyperlink_data JSON
)
''')

pdf_temporary_files_list = []
# Read a newline-separated list of PDF file URLs from sources.txt
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
        orig_url = pdf_tuple[1]
        # Insert metadata (only once per PDF)
        metadata = json.dumps(pdf.metadata) if pdf.metadata else None
        if metadata:
            con.execute('''
            INSERT INTO metadata_data (original_url, metadata)
            VALUES (?, ?)
            ''', (orig_url, metadata))

        for page_number, page in enumerate(pdf.pages, start=1):
            # Extract text
            text = page.extract_text() or None
            if text:
                con.execute('''
                INSERT INTO text_data (original_url, page_number, text)
                VALUES (?, ?, ?)
                ''', (orig_url, page_number, text))

            # Extract images
            if page.images:
                for img in page.images:
                    image_data = json.dumps({"x0": img["x0"], "x1": img["x1"], "y0": img["y0"], "y1": img["y1"]})
                    con.execute('''
                    INSERT INTO images_data (original_url, page_number, image_data)
                    VALUES (?, ?, ?)
                    ''', (orig_url, page_number, image_data))

            # Extract tables
            tables = page.extract_tables()
            if tables:
                for table in tables:
                    table_data = json.dumps(table)
                    con.execute('''
                    INSERT INTO tables_data (original_url, page_number, table_data)
                    VALUES (?, ?, ?)
                    ''', (orig_url, page_number, table_data))

            # Extract annotations
            if page.annots:
                for annot in page.annots:
                    annotation_data = json.dumps(annot)
                    con.execute('''
                    INSERT INTO annotations_data (original_url, page_number, annotation_data)
                    VALUES (?, ?, ?)
                    ''', (orig_url, page_number, annotation_data))

            # Extract hyperlinks
            if page.hyperlinks:
                for hyperlink in page.hyperlinks:
                    hyperlink_data = json.dumps(hyperlink)
                    con.execute('''
                    INSERT INTO hyperlinks_data (original_url, page_number, hyperlink_data)
                    VALUES (?, ?, ?)
                    ''', (orig_url, page_number, hyperlink_data))

# Close the connection
con.close()

# Delete the temporary files
for pdf_tuple in pdf_temporary_files_list:
    os.remove(pdf_tuple[0])

# Return the database file path
print("Database file created:", os.path.abspath('pdf_data.db'))