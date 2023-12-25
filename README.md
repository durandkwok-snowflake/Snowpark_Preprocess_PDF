# Snowpark_Preprocess_PDF
Welcome to the Snowflake Snowpark pdf demo. This demo showcases how you can easily use Snowpark in Snowflake in ingest pdfs, parse and split the pdf in batches. This is extremely useful when want to search and keep certain parts of the pdf.

Setup
Create Doc Stage for Raw PDF
<img width="1118" alt="image" src="https://github.com/durandkwok-snowflake/Snowpark_Preprocess_PDF/assets/109616231/fccdcc53-8fb4-484c-b6d2-59f5aeadea1f">

Create Doc Stage for Split PDF
<img width="1426" alt="image" src="https://github.com/durandkwok-snowflake/Snowpark_Preprocess_PDF/assets/109616231/3e9a3614-ec22-43c5-9a77-18f171127f71">

```SQL

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
            DOC_AI_DB.DOC_AI_SCHEMA.DOC_STAGE_RAW
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

### Run the code to create the Snowpark Store Procedure preprocess_pdf

CALL preprocess_pdf(build_scoped_file_url(@doc_stage_raw, 'Snowflake2021_10K.pdf'), 'Snowflake2021_10K', '@doc_stage_split' );
```
<img width="1411" alt="image" src="https://github.com/durandkwok-snowflake/Snowpark_Preprocess_PDF/assets/109616231/0b3ca8d3-1c1e-4bd1-a706-ede3ddbd3e28">

<img width="1134" alt="image" src="https://github.com/durandkwok-snowflake/Snowpark_Preprocess_PDF/assets/109616231/c4f43467-c10b-4c64-987f-196f3b7ec2ef">
