import pandas as pd
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col
import streamlit as st
import uuid
import boto3
import io
from io import StringIO
import base64
from PIL import Image, ImageDraw

st.set_page_config(page_title='Image Uploader',  initial_sidebar_state="auto", menu_items=None)
uploaded_file = None
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
   
country_codes_df = session.sql("select iso_country_name, alpha_code_3digit from intl_db.countries.int_stds_org_3661 order by iso_country_name;").collect()
country_codes_df =  pd.DataFrame(country_codes_df)
#st.write(country_codes_df)

account_locator = st.text_input('Type in Your Snowflake Account Locator')

col1, col2 = st.columns(2)

with col1:
  country_name = st.selectbox(
        "In what country is this bridge located?",
        country_codes_df,
        index=59
  ) 
  #st.write('The country chosen is: ',country_name)
  country_code=country_codes_df.loc[country_codes_df['ISO_COUNTRY_NAME'] == country_name, 'ALPHA_CODE_3DIGIT'].iloc[0]
  st.write('The 3-digit ISO code for', country_name,' is ',country_code, '.')
  
with col2:
   bridge_name = st.text_input('Bridge Name', 'Øresund')
   st.write('The bridge name you entered is:', bridge_name)
   st.write("##")
   

with st.container():
  st.markdown("""---""")
  st.write('Please choose a JPG or PNG file to add to our bridge collection.')   
  uploaded_file = st.file_uploader("Choose an image file", accept_multiple_files=False, label_visibility='hidden')

  if uploaded_file is not None:
   if st.button('Upload and Process File'):
      with st.spinner("Uploading image, analyzing the contents, and creating a metadata row about it..."):

         #st.write(uploaded_file)
         file_to_put = getattr(uploaded_file, "name")     
         st.write("File to be Uploaded: " + file_to_put + ".")
         st.image(uploaded_file)

         s3 = boto3.client('s3', **st.secrets["s3"])
         bucket = 'uni-bridge-image-uploads'  
         s3.upload_fileobj(uploaded_file, bucket, file_to_put, ExtraArgs={'ContentType': "image/png"})


         # Write image data in Snowflake table
         df = pd.DataFrame({"ACCOUNT_LOCATOR": [account_locator], "BRIDGE_NAME": [bridge_name], "OG_FILE_NAME": [file_to_put], "COUNTRY_CODE": [country_code]})
         session.write_pandas(df, "UPLOADED_IMAGES")

         rek = boto3.client('rekognition', **st.secrets["s3"], region_name='us-west-2')
         rek_response = rek.detect_labels(
               Image={'S3Object':{'Bucket':bucket,'Name':file_to_put}},
               MaxLabels=10,
               Settings={"GeneralLabels": {"LabelInclusionFilters":["Bridge", "Water", "Car", "Person", "Airplane", "Truck", "Cloud"]}}
               )                                    

         st.write('The image you loaded has been examined for the presence of bridges and other items. The results are presented as percentage confidence that each object type appears in the image.')

         st.write(rek_response)
         
         #create fake df
         test_df = pd.DataFrame([['a', 'b'], ['c', 'd']],
                   index=['row 1', 'row 2'],
                   columns=['col 1', 'col 2'])
         
         #convert to json
         test_df.to_json(orient='split')
         #convert back to json
         new_json = pd.read_json(test_df, orient='split')
         st.write(new_json)
         
         #df2 = pd.read_json(rek_response, orient ='split')
         #st.write(df2)

         
         
         
         all_confidences = [label['Confidence'] for label in rek_response['Labels']]
         st.write(all_confidences)
         all_labels = [label['Name'] for label in rek_response['Labels']]
         for i in range(0, len(all_labels)):
                 all_labels[i]=all_labels[i]+": "+str(all_confidences[i])+"%"         

         st.write(str(all_labels))

         #st.write(rek_response)
         st.stop()

         uploaded_file = None
            
         
        #st.stop()
