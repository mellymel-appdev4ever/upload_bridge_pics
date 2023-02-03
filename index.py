import json
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import streamlit as st
import io
from io import StringIO
import base64
import uuid
import boto3
import os

st.set_page_config(page_title='Image Uploader',  initial_sidebar_state="auto", menu_items=None)

# Set page title, header and links to docs
st.header("Upload a Picture")
st.caption(f"Of a bridge, or not a bridge")
 
# Create the connection to Snowflake
conn = {**st.secrets["snowflake"]}

# Create a new Snowpark session (or get existing session details)
def create_session():
    if "snowpark_session" not in st.session_state:
        session = Session.builder.configs(conn).create()
        st.session_state['snowpark_session'] = session
    else:
        session = st.session_state['snowpark_session']
    return session


uploaded_file = st.file_uploader("Choose an image file", accept_multiple_files=False, label_visibility='hidden')
if uploaded_file is not None:

    with st.spinner("Uploading image and creating a metadata row about it..."):

        st.write(uploaded_file)
        #st.write(getattr(uploaded_file, "name"))
        file_to_put = getattr(uploaded_file, "name")
        st.write("'" + file_to_put + "'")
        #session.file.put(file_to_put,'@intl_db.bridges.image_files',overwrite=True,auto_compress=False)      
     
      
        s3 = boto3.client('s3', **st.secrets["s3"])
        bucket = 'uni-bridge-image-uploads'  
        s3.upload_fileobj(uploaded_file, bucket, file_to_put)
   
        
        #st.stop()
        # Call that function ^ 
        session = create_session()
        
        # Convert uploaded file to hex - base64 string into hex 
        #bytes_data_in_hex = uploaded_file.getvalue().hex()
        bytes_data_in_hex = 'not really'
        # Generate new image file name
        file_name = 'img_' + str(uuid.uuid4())

                
        # Write image data in Snowflake table
        df = pd.DataFrame({"UUID_FILE_NAME": [file_name],  "OG_FILE_NAME": [file_to_put], "IMAGE_BYTES": [bytes_data_in_hex]})
        session.write_pandas(df, "UPLOADED_IMAGES")
        
        #st.stop()
  
        
              
