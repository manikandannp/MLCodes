import getpass
import requests
import jaydebeapi
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
import math
import csv
import time
import os

#change path here
path='C:/Data/Reports/Adhoc/Audience Tribe/Lotame API/'
#set working directory
os.chdir(path)

#import credentials.py, should contain dictionary with Lotame UI username and password, should be located in working directory with following format:
#lot_login = {'username':'[USERNAME INPUT]','password':'[PASS INPUT]'}
#acs_login = {'username':'[USERNAME INPUT]','password':'[PASS INPUT]'}

import credentials

#acs credentials from credentials.py
acs_payload = credentials.acs_login

#connect to ACS
dsn_database 			= "BACC_PRD_IDM_ACS"
dsn_hostname 			= "bacc-pda2-wall.portsmouth.uk.ibm.com"
dsn_port 				= "5480"
dsn_uid 				= acs_payload['username']
dsn_pwd 				= acs_payload['password']
jdbc_driver_name 		= 'org.netezza.Driver'
jdbc_driver_loc 		= path+"nzjdbc3.jar"
connection_string		='jdbc:netezza://'+dsn_hostname+':'+dsn_port+'/'+dsn_database
print("Connection String: " + connection_string)

conn = jaydebeapi.connect(jdbc_driver_name, connection_string,[dsn_uid,dsn_pwd], jars = jdbc_driver_loc)
crsr = conn.cursor()

#bring in current data
query="""select 
	LOT_ID
	,LOT_NAME
	,LOT_CPM
	,LOT_30_DAY_UNIQUES
	,TESTING_PERSONALIZATION
	,COMPANY
	,CONT_INT_AREA_DSCR
	,PRODUCT_INSTALL
	,PROJ_CD
	,PLAN_NAME
	,PROG_NAME
	,GBT_LVL_10_CD
	,GBT_LVL_17_CD
	,IND_DSCR
	,JOB_ROLE_DSCR
	,VEND_DSCR
	,AUDIENCE_TYPE
	from 
	ACS_ADGN0.LOTAME_AUDIENCE_TAX_MAP_FIXED"""

current_audience_df = pd.io.sql.read_sql_query(query, con=conn)
#fill NA with blanks
current_audience_df.fillna('',inplace=True)

#set Lotame API endpoint url and authentication url
API_URL = 'https://api.lotame.com/2/' 
AUTH_URL = 'https://crowdcontrol.lotame.com/auth/v1/tickets'
#add the username and password to the payload from credentials.py
lot_payload = credentials.lot_login
#make a post request to get the Lotame API Ticket Granting location
tg_ticket_location = requests.post(AUTH_URL, data=lot_payload).headers['location']
#take a look at Ticket Granting Ticket
print(tg_ticket_location[tg_ticket_location.rfind('/') + 1:])
#set up the sevice call using the API_URL and the audiences report request with all params, create payload
service_call = (API_URL+'audiences?includeExpired=N&only_profile=N&page_count=10000&page_num=1&sort_order=ASC&sort_attr=id&client_id=10025') 
service_payload = {'service': service_call}
#make post request to get Service Ticket, using Ticket Granting Ticket location and service_call payload
service_ticket = requests.post(tg_ticket_location, data=service_payload).text 
print (service_ticket)
#use the service_call and service ticket as a parameter to run a GET request to pull the audience report
audience_list = requests.get(service_call+'&ticket='+service_ticket)
#print GET request status_code, should be 200 if successful 
print(audience_list.status_code)
#convert audience response to dictionary using .json()
audience_dict=audience_list.json()

#create an empty dataframe to hold the audiences 
audience_all = pd.DataFrame()

#loop through list of audiences in audience_json
for d in audience_dict['Audience']:
	#create dataframe for single audience in audience list from dictionary
	df = pd.DataFrame.from_dict(d,orient='index')
	#if audience is not purchased then we insert effectiveCPM as 0
	if(df.loc['purchased'][0]=='N'): 
		df2=df.transpose()[['id','name','uniques']]
		df2.insert(2, 'effectiveCPM', 0, allow_duplicates = False)
	#else, for Purchased audiences we grab effectiveCPM column with other columns
	else:
		df2=df.transpose()[['id','name','effectiveCPM','uniques']]
	
	audience_all=pd.concat([audience_all,df2],axis=0)


audience_all.columns=['LOT_ID','LOT_NAME','LOT_CPM','LOT_30_DAY_UNIQUES']
#audience_all['LOT_DEFINITION']=''

#get common audiences
common=audience_all.merge(current_audience_df.astype(str),on='LOT_ID')

#get updated old audiences
common_final=common.drop(['LOT_NAME_y','LOT_CPM_y','LOT_30_DAY_UNIQUES_y'], axis=1)
common_final.rename(columns={"LOT_NAME_x": "LOT_NAME", "LOT_CPM_x": "LOT_CPM", "LOT_30_DAY_UNIQUES_x": "LOT_30_DAY_UNIQUES"}, inplace=True)

#get new audiences
audiences=audience_all[~audience_all['LOT_ID'].isin(common['LOT_ID'])]

##testing##########
#d={'LOT_NAME': ['3p|Account|Company|OpenText|HGD_365355',
#	'1p|Campaign|Plan Program & Project Code|Plan - Developer_352763|Program - Productivity_352781|Project Code - 000022PX_352788',
#	'1p|Campaign|Plan Program & Project Code|Plan - Developer_352763|Program - Productivity_352781|Project Code - 000022PX_352788',
#	'1p|Campaign|Plan Program & Project Code|Plan - Developer_352763|Program - Productivity_352781|Project Code - 000022PX_352788'}
#audiences=pd.DataFrame(data=d)
################

#parse name column to various new columns for mapping
#Testing and Personalization
audiences['TESTING_PERSONALIZATION']=audiences['LOT_NAME'].str.extract(r'(\|Audience Identification\|)')
audiences['TESTING_PERSONALIZATION']=audiences['TESTING_PERSONALIZATION'].apply(lambda x: 'Paid Media' if pd.isnull(x) else 'Testing and Personalization')

#AUDIENCE TYPE
audiences['AUDIENCE_TYPE']=audiences['LOT_NAME'].apply(lambda x: x[0:2].capitalize() if x[0:2].capitalize() == '3P' or x[0:2].capitalize() == '1P' else 'NA')
audiences['AUDIENCE_TYPE'].fillna('',inplace=True)

#Company
audiences['COMPANY']=audiences['LOT_NAME'].str.extract(r'3p\|Account\|Company\|(.*)\|')
audiences['COMPANY'].fillna('',inplace=True)

#Interest topic 1p
audiences['CONT_INT_AREA_DSCR']=audiences['LOT_NAME'].str.extract(r'(?:(?:1p\|Profile\|Interest Topic\|)|(?:1p\|Audience Identification\|Interest Topic\|)|(?:3p\|Profile\|Interest Topic\|)|(?:3p\|Audience Identification\|Interest Topic\|))(.*) - ')
audiences['CONT_INT_AREA_DSCR'].fillna('',inplace=True)

#Competitive Install
audiences['PRODUCT_INSTALL']=audiences['LOT_NAME'].str.extract(r'3p\|Competitive\|Install\|(.*)\|')
audiences['PRODUCT_INSTALL'].fillna('',inplace=True)

#Project Code
audiences['PROJ_CD']=audiences['LOT_NAME'].str.extract(r'\|Project Code - (.*)_')
audiences['PROJ_CD'].fillna('',inplace=True)

#Plan Name
audiences['PLAN_NAME']=audiences['LOT_NAME'].str.extract(r'\|Plan - (.*?)_')
audiences['PLAN_NAME'].fillna('',inplace=True)

#Prog name
audiences['PROG_NAME']=audiences['LOT_NAME'].str.extract(r'\|Program - (.*?)_')
audiences['PROG_NAME'].fillna('',inplace=True)

#GBT 10 CD
audiences['GBT_LVL_10_CD']=audiences['LOT_NAME'].str.extract(r'1p\|IBM\.com\|GBT\|.* - (B[A-Za-z0-9][0-9][0-9][0-9])\|')
audiences['GBT_LVL_10_CD'].fillna('',inplace=True)

#GBT 17 CD
audiences['GBT_LVL_17_CD']=audiences['LOT_NAME'].str.extract(r'1p\|IBM\.com\|GBT\|.* - (17[a-zA-Z][a-zA-Z][a-zA-Z])\|')
audiences['GBT_LVL_17_CD'].fillna('',inplace=True)

#Industry
audiences['IND_DSCR']=audiences['LOT_NAME'].str.extract(r'(?:(?:3p\|Profile\|Industry\|)|(?:1p\|Profile\|Industry\|)|(?:1p\|Audience Identification\|Industry\|)|(?:3p\|Audience Identification\|Industry\|))(.*) -')
audiences['IND_DSCR'].fillna('',inplace=True)

#Job Role
#audiences['JOB_ROLE_DSCR']=audiences['LOT_NAME'].str.extract(r'(?:(?:3p\|Audience Identification\|Job Role\|)|(?:3p\|Profile\|Job Role\|))(?:([^-]+ ?-? ?(?:[^-]+)?)(?:.*)\|)')
#audiences['JOB_ROLE_DSCR'].fillna('',inplace=True)

#Job Role
job_role_1=audiences['LOT_NAME'].str.extract(r'(?:(?:3p\|Audience Identification\|Job Role\|)|(?:3p\|Profile\|Job Role\|))(?:([^-]+ - [^-]+) - [^-]+)\|')
job_role_2=audiences['JOB_ROLE_DSCR']=audiences['LOT_NAME'].str.extract(r'(?:(?:3p\|Audience Identification\|Job Role\|)|(?:3p\|Profile\|Job Role\|))(?:([^-]+) - [^-]+)\|')
job_role_3=audiences['JOB_ROLE_DSCR']=audiences['LOT_NAME'].str.extract(r'(?:(?:3p\|Audience Identification\|Job Role\|)|(?:3p\|Profile\|Job Role\|))([^-]+)\|')
job_role_col=job_role_1.combine_first(job_role_2.combine_first(job_role_3))
audiences['JOB_ROLE_DSCR']=job_role_col
audiences['JOB_ROLE_DSCR'].fillna('',inplace=True)

#Vendor
audiences['VEND_DSCR']=audiences['LOT_NAME'].str.extract(r'\|([a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9])_')
audiences['VEND_DSCR'].fillna('',inplace=True)

####################

#NEED TO APPEND common_final to audiences
audiences_final=pd.concat([common_final,audiences],axis=0)

#replace TAB in 'name' field with space, this helps us avoid errors when loading into Netezza
audiences_final['LOT_NAME']=audiences_final['LOT_NAME'].replace("\t"," ", regex=True)

#Add timestamp
audiences_final['UPDATE_TS']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")

#drop lot definition
#audiences_final.drop(['LOT_DEFINITION'], axis=1,inplace=True)

columnsTitles = ['LOT_ID','LOT_NAME','LOT_CPM','LOT_30_DAY_UNIQUES','TESTING_PERSONALIZATION','COMPANY','CONT_INT_AREA_DSCR','PRODUCT_INSTALL','PROJ_CD','PLAN_NAME','PROG_NAME','GBT_LVL_10_CD','GBT_LVL_17_CD','IND_DSCR','JOB_ROLE_DSCR','VEND_DSCR','AUDIENCE_TYPE', 'UPDATE_TS']
audiences_final = audiences_final[columnsTitles]
print(audiences_final)

#write audiences to tsv
audiences_final.to_csv(path+'audiences.tsv',index=False, sep="\t", quoting=csv.QUOTE_NONE, quotechar="",  escapechar="\\")
#audiences.to_csv('c:/Users/BobakAtefi/Desktop/IBM Projects/lotame api/audiences_def.tsv',index=False, sep="\t", quoting=csv.QUOTE_NONE, quotechar="",  escapechar="\\")

#connect to ACS
conn = jaydebeapi.connect(jdbc_driver_name, connection_string,[dsn_uid,dsn_pwd], jars = jdbc_driver_loc)
crsr = conn.cursor()

#insert audience data into MPW table
#query="""truncate table ACS_ADGN0.LOTAME_AUDIENCE_TAX_MAP"""
query="""DROP TABLE ACS_ADGN0.LOTAME_AUDIENCE_TAX_MAP IF EXISTS"""
crsr.execute(query)

query="""
create table ACS_ADGN0.LOTAME_AUDIENCE_TAX_MAP
(
	LOT_ID BIGINT
	,LOT_NAME CHARACTER VARYING(255)
	,LOT_CPM DOUBLE PRECISION
	,LOT_30_DAY_UNIQUES DOUBLE PRECISION
	,TESTING_PERSONALIZATION CHARACTER VARYING(255)
	,COMPANY CHARACTER VARYING(255)
	,CONT_INT_AREA_DSCR CHARACTER VARYING(255)
	,PRODUCT_INSTALL CHARACTER VARYING(255)
	,PROJ_CD CHARACTER VARYING(255)
	,PLAN_NAME CHARACTER VARYING(255)
	,PROG_NAME CHARACTER VARYING(255)
	,GBT_LVL_10_CD CHARACTER VARYING(255)
	,GBT_LVL_17_CD CHARACTER VARYING(255)
	,IND_DSCR CHARACTER VARYING(255)
	,JOB_ROLE_DSCR CHARACTER VARYING(255)
	,VEND_DSCR CHARACTER VARYING(255)
	,AUDIENCE_TYPE CHARACTER VARYING(2)
	,UPDATE_TS TIMESTAMP
)
"""
crsr.execute(query)
#crsr.commit()

#INSERT INTO ACS_ADGN0.LOTAME_AUDIENCE_TAX_MAP
#insert audience data into MPW tablepython
query="""
INSERT INTO ACS_ADGN0.LOTAME_AUDIENCE_TAX_MAP  
SELECT * 
FROM EXTERNAL '{0}'
USING 
(
 REMOTESOURCE 'JDBC'
 DELIMITER '\t'
 SKIPROWS 1
 MAXERRORS 1
 ESCAPECHAR '\'
 )""".format(path+'audiences.tsv')
crsr.execute(query)
#crsr.commit()