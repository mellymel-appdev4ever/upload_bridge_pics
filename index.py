import json
import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import streamlit as st
import io
#from io import StringIO
#import base64
import uuid
import boto3
#import os

st.set_page_config(page_title='Image Uploader',  initial_sidebar_state="auto", menu_items=None)


# Set page title
st.header("Submit Your Favorite Bridge!")
st.caption(f"Summit 2023 - Build Your Bridge to the Snowflake Data Cloud")
 
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
 
col1, col2 = st.columns(2)

with col1:
  country_code = st.selectbox(
        "In what country is this bridge located?",
        ("US", "UK", "MX"),
        label_visibility=st.session_state.visibility,
        disabled=st.session_state.disabled,
  ) 

with col2:
   bridge_name = st.text_input('Movie title', 'Life of Brian')
   st.write('The current movie title is', title)
   )   
  
   
uploaded_file = st.file_uploader("Choose an image file", accept_multiple_files=False, label_visibility='hidden')
if uploaded_file is not None:

    with st.spinner("Uploading image and creating a metadata row about it..."):

        st.write(uploaded_file)
        file_to_put = getattr(uploaded_file, "name")
        st.write("'" + file_to_put + "'")
      
        s3 = boto3.client('s3', **st.secrets["s3"])
        bucket = 'uni-bridge-image-uploads'  
        s3.upload_fileobj(uploaded_file, bucket, file_to_put)
   
        # Create a Snowflake Snowpark Session
        session = create_session()
        
        country_code = 'UK'
        # Generate new image file name to avoid dupes
        file_name = 'img_' + str(uuid.uuid4())

                
        # Write image data in Snowflake table
        df = pd.DataFrame({"UUID_FILE_NAME": [file_name],  "OG_FILE_NAME": [file_to_put], "COUNTRY_CODE": [country_code]})
        session.write_pandas(df, "UPLOADED_IMAGES")
        
        #st.stop()
  
        
              
