import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import streamlit as st
import uuid
import boto3

st.set_page_config(page_title='Image Uploader',  initial_sidebar_state="auto", menu_items=None)

# Set page title
st.header("Submit An Image of Your Favorite Bridge!")
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

# Open a Snowflake Snowpark Session
session = create_session()
   
country_codes_df = session.sql("select iso_country_name, alpha_code_2digit from intl_db.countries.int_stds_org_3661 order by iso_country_name;").collect()
country_codes_df =  pd.DataFrame(country_codes_df)
#option = st.selectbox("Select option", country_codes_df, format_func=format_func)
#st.write(f"You selected option {option} called {format_func(option)}")

display = (country_codes_df.columns[1])
options = (country_codes_df.columns[2])
value = st.selectbox("Pick a Country", options, format_func=lambda x: display[x])

st.write(value)


col1, col2 = st.columns(2)

with col1:
  country_code = st.selectbox(
        "In what country is this bridge located?",
        country_codes_df,
        index=59
  ) 
  st.write('The country chosen is: ',country_code)
  
#CHOICES = country_codes_df
#option = st.selectbox("Select option", options=list(CHOICES.keys()), format_func=format_func)

with col2:
   bridge_name = st.text_input('Bridge Name', 'Øresund')
   st.write('The bridge name you entered is:', bridge_name)
      
  
   
uploaded_file = st.file_uploader("Choose an image file", accept_multiple_files=False, label_visibility='hidden')
if uploaded_file is not None:

    with st.spinner("Uploading image and creating a metadata row about it..."):

        st.write(uploaded_file)
        file_to_put = getattr(uploaded_file, "name")
        st.write("'" + file_to_put + "'")
      
        s3 = boto3.client('s3', **st.secrets["s3"])
        bucket = 'uni-bridge-image-uploads'  
        s3.upload_fileobj(uploaded_file, bucket, file_to_put, ExtraArgs={'ContentType': "image/png"})

        # Generate new image file name to avoid dupes
        file_name = 'img_' + str(uuid.uuid4())

                
        # Write image data in Snowflake table
        df = pd.DataFrame({"UUID_FILE_NAME": [file_name],  "OG_FILE_NAME": [file_to_put], "COUNTRY_CODE": [country_code]})
        session.write_pandas(df, "UPLOADED_IMAGES")
        
        #st.stop()
  
        
              
