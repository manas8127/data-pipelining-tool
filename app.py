import streamlit as st
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import sqlalchemy



#sc = SparkContext('local')
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

st.markdown("<h1 style='text-align: center; color: white;'>Data Pipelining Tool</h1>", unsafe_allow_html=True)

@st.cache 
def pipeline(file_loca, file_type):
	
	
	infer_schema = "true"
	first_row_is_header = "true"
	delimiter = ","
	
	if file_type!="xlsx":
		df=spark.read.format(file_type) \
    	.option("inferSchema", infer_schema) \
    	.option("header", first_row_is_header) \
    	.option("sep", delimiter) \
    	.option('nanValue', ' ')\
    	.option('nullValue', ' ')\
    	.load(file_loca)
		df = df.toPandas()
	else:
		df=pd.read_excel(file_loca)


	
	
	return df


multiple_files = st.file_uploader('' ,accept_multiple_files=True)
for file in multiple_files:
	st.write(file.name)
	#st.write(file.name.rpartition('.')[0])
	can=file.name.rpartition('.')[0]
	dataframe = pipeline(file.name,file.name.rpartition('.')[2])
	file.seek(0)
	st.dataframe(dataframe)


st.write(" ")
st.write(" ")

#user_input = st.text_input("What do you want to name the database?")

text_input=''
submit_button=''
db=''

with st.form(key='my_form'):
	text_input = st.text_input(label='Which database do you wish to choose?')
	submit_button = st.form_submit_button(label='Create Database')
	#st.write(submit_button)
	db='sqlite:///'+text_input+'.db'
	#st.write(db)


if submit_button==True:

	if text_input=='':
		st.error('Database field cannot be empty')
		
	else:
		database = sqlalchemy.create_engine(db)
		for file in multiple_files:
			#st.write(file.name)
			#st.write(file.name.rpartition('.')[0])
			dataframe = pipeline(file.name,file.name.rpartition('.')[2])
			#file.seek(0)
			dataframe.to_sql(file.name.rpartition('.')[0], db, if_exists="replace")
		
	if text_input!='':	
		st.success('Successfully created database '+text_input+'! please refresh the page')
	
	

	


hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)










