CREATE OR REPLACE PROCEDURE preprocess_pdf(file_path string, file_name string, dest_stage_name string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pypdf2')
HANDLER = 'run'
AS
$$
from PyPDF2 import PdfFileReader, PdfWriter
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark import Session, FileOperation
from io import BytesIO

def run(session, file_path, file_name, dest_stage_name):
    whole_text = "Success"
    #dest_stage = "@doc_stage_split"    
    dest_stage = dest_stage_name
    
    with SnowflakeFile.open(file_path, 'rb') as file:
        f = BytesIO(file.readall())
        input_pdf = PdfFileReader(f)
        batch_size = 50
        num_batches = len(input_pdf.pages) // batch_size + 1
        for b in range(num_batches):
            writer = PdfWriter()
            # Get the start and end page numbers for this batch
            start_page = b * batch_size
            end_page = min((b+1) * batch_size, len(input_pdf.pages))
            
            # Add pages in this batch to the writer
            for i in range(start_page, end_page):
                writer.add_page(input_pdf.pages[i])

            # Save the batch to a separate PDF file
            batch_filename = f'//tmp/{file_name}{b+1}.pdf'

            with open(batch_filename, 'wb') as output_file:
                writer.write(output_file)

            FileOperation(session).put("file:///tmp/"+file_name+"*", dest_stage, auto_compress = False)        
        
    return whole_text
$$;
