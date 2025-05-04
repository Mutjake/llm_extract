import pdfplumber
import duckdb
import os
import requests

con = duckdb.connect('pdf_data.db')
# Create a table to store the extracted data
con.execute('''
CREATE TABLE IF NOT EXISTS pdf_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_url TEXT,
    text TEXT,
    images BLOB,
    tables BLOB,
    metadata BLOB,
    annotations BLOB,
    hyperlinks BLOB,
    outlines BLOB,
    bookmarks BLOB,
    links BLOB,
    textboxes BLOB
)
''')    

pdf_temporary_files_list = []
# Read a newline separated list of PDF file urls from sources.txt

with open('sources.txt', 'r') as file:
    pdf_url_list = file.read().splitlines()
        #download each file into temporary file and append the file path to the pdf_temporary_files_list
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
            text = page.extract_text()  
            # extract images from the page
            images = page.images
            # extract tables from the page
            tables = page.extract_tables()
            # extract metadata from the page
            metadata = pdf.metadata
            # extract annotations from the page
            annotations = page.annots
            # extract hyperlinks from the page
            hyperlinks = page.hyperlinks
            # extract outlines from the page
            outlines = pdf.outlines
            # extract bookmarks from the page
            bookmarks = pdf.bookmarks
            # extract links from the page
            links = page.links
            # extract textboxes from the page
            textboxes = page.textboxes
            # write the extracted data to duckdb database
            con.execute('''
            INSERT INTO pdf_data (original_url, text, images, tables, metadata, annotations, hyperlinks, outlines, bookmarks, links, textboxes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (orig_url, text, images, tables, metadata, annotations, hyperlinks, outlines, bookmarks, links, textboxes))
    # close the connection
con.close()
# Delete the temporary files
for pdf_tuple in pdf_temporary_files_list:
    os.remove(pdf_tuple[0])

# Return the database file path
print("Database file created:", os.path.abspath('pdf_data.db'))