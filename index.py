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

         #after loading the file, we'll use it to analyze and draw
         s3_img_connection = boto3.resource('s3', **st.secrets["s3"])
         s3_img_object = s3_img_connection.Object(bucket, file_to_put)
         s3_img_response = s3_img_object.get()

         stream = io.BytesIO(s3_img_response['Body'].read())
         uploaded_image = Image.open(stream)
         imgWidth, imgHeight = uploaded_image.size
         draw = ImageDraw.Draw(uploaded_image)
         #st.write(imgWidth)

         rek = boto3.client('rekognition', **st.secrets["s3"], region_name='us-west-2')
         rek_response = rek.detect_labels(
               Image={'S3Object':{'Bucket':bucket,'Name':file_to_put}},
               MaxLabels=10,
               Settings={"GeneralLabels": {"LabelInclusionFilters":["Bridge", "Water", "Dog", "Person", "Boat", "Vehicle", "Cloud"]}}
               )                                    

         st.write('The image you loaded has been examined for the presence of bridges and other items. The results are presented as percentage confidence that each object type appears in the image.')
        
         for label in rek_response['Labels']:
             st.write("Item Found: " + label['Name'])
             st.write("Confidence: " + str(label['Confidence']))
             

             for instance in label['Instances']:
                 st.write(" Bounding box")
                 st.write(" Top: " + str(instance['BoundingBox']['Top']))
                 st.write(" Left: " + str(instance['BoundingBox']['Left']))
                 st.write(" Width: " + str(instance['BoundingBox']['Width']))
                 st.write(" Height: " + str(instance['BoundingBox']['Height']))
                 
                 box = instance['BoundingBox']
                 left = imgWidth * box['Left']
                 top = imgHeight * box['Top']
                 width = imgWidth * box['Width']
                 height = imgHeight * box['Height']
                 
                 points = (
                    (left, top),
                    (left + width, top),
                    (left + width, top + height),
                    (left, top + height),
                    (left, top)
                 )
                 draw.line(points, fill='#00d400', width=2)
             st.markdown("""---""")  
         #uploaded_file.show()    
         st.image(draw)
         # Write image data in Snowflake table
         to_sf_df = pd.DataFrame({"ACCOUNT_LOCATOR": [account_locator], "BRIDGE_NAME": [bridge_name], "OG_FILE_NAME": [file_to_put], "COUNTRY_CODE": [country_code]})
         session.write_pandas(to_sf_df, "UPLOADED_IMAGES")
         
         
         st.stop()
        




        #print('Left: ' + '{0:.0f}'.format(left))
        #print('Top: ' + '{0:.0f}'.format(top))
        #print('Face Width: ' + "{0:.0f}".format(width))
        #print('Face Height: ' + "{0:.0f}".format(height))

  

        # Alternatively can draw rectangle. However you can't set line width.
        # draw.rectangle([left,top, left + width, top + height], outline='#00d400')

    

    #return len(response['FaceDetails'])


