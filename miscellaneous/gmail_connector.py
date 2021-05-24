""" This file is used to go through my email and find the csv call records sent by a program in our call center. Can be adapted to any csv attachements in email given
a regular structure for email subjects/names/attachments. It then goes on to update a database using psycopg2  """


from __future__ import print_function
import numpy as np
import pickle
import os.path, os, sys
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import datetime

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

"""Shows basic usage of the Gmail API.
Lists the user's Gmail labels.
"""
creds = None
# The file token.pickle stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.


# creating relative paths
base_path = sys.argv[0]
route_name = base_path+'//..//..//..'
route = os.path.abspath(route_name)
# print(f'Base route for this file is: {route}')
pickle_path = route + '\\env_variables\\' + 'token_gmail_automated.pickle'
creds_path = route + '\\env_variables\\' + 'credentials_gmail_automated.json'
data_dump_base = route + '\\data_dumps\\'


if os.path.exists(pickle_path):
    with open(pickle_path, 'rb') as token:
        creds = pickle.load(token)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            creds_path, SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open(pickle_path, 'wb') as token:
        pickle.dump(creds, token)


service = build('gmail', 'v1', credentials=creds)

import base64
import pandas as pd
from io import StringIO, BytesIO

def get_attachment(query):
    results = service.users().messages().list(userId='me', q = query).execute()
    msg_id = results['messages'][0]['id']
    message = service.users().messages().get(userId='me', id = msg_id).execute()
    for x in message['payload']['parts']:
        if x['mimeType'] == 'text/csv':
            attachment_id = x['body']['attachmentId']
    data = service.users().messages().attachments().get(userId='me', id=attachment_id, messageId=msg_id).execute()['data']
    str_csv  = base64.urlsafe_b64decode(data.encode('UTF-8'))
    df = pd.read_csv(BytesIO(str_csv))
    return df

contacts_df = get_attachment("subject: Ordered 30 Day Contact Report")
calls_df = get_attachment("subject: Ordered Call Log 30 Days")

contacts_df['CONTACT RECORD CREATE TIMESTAMP'] = pd.to_datetime(contacts_df['CONTACT RECORD CREATE TIMESTAMP'], infer_datetime_format=True)

calls_df['TIMESTAMP'] = pd.to_datetime(calls_df['TIMESTAMP'], infer_datetime_format=True)
calls_df['Contact Create DateTime'] = pd.to_datetime(calls_df['Contact Create DateTime'], infer_datetime_format=True)
calls_df.drop(calls_df.index[calls_df['CONTACT ID'].isnull()], inplace=True)
calls_df['CONTACT ID'] = calls_df['CONTACT ID'].astype('int')
calls_df = calls_df[['CALL ID','CONTACT ID','TIMESTAMP', 'CAMPAIGN','AGENT NAME','Source List',
                     'Contact Create DateTime','DISPOSITION']]

fdisp_map = {'Answering Machine - VMTE': '',
 'Scheduled Call Back': '',
 'No Disposition': '',
 'Successful Appointment': 'FDISP',
 'Hung Up - Try Again': '',
 'Declined': '',
 'Does Not Qualify': 'FDISP',
 'Remove From Dialer': 'FDISP',
 'No Voicemail - Priority': '',
 'No Voicemail': '',
 'Scheduled By EC': 'FDISP',
 'Added to No Call List': 'FDISP',
 'Misc - Not a Lead': 'FDISP',
 'Transfer': '',
 'Caller Disconnected': '',
 'Transferred To 3rd Party': '',
 'Missed Call': '',
 'Agent Error': '',
 'Do Not Call': '',
 'Voicemail Dump': '',
 'Forced Logout': '',
 'Sent To Voicemail': '',
 'Queue Callback Timeout': '',
 'Duplicated Callback Request': '',
 'Abandon': ''}

calls_df['fdisp'] = calls_df['DISPOSITION'].map(fdisp_map)
calls_df.drop_duplicates(subset=['CALL ID'], inplace=True)

contacts_df = contacts_df[['CONTACT ID', 'CONTACT RECORD CREATE TIMESTAMP', 'CUSTOMER NAME', 'email',
       'number1', 'zip', 'Source List', 'Last Disposition']]
contacts_df.rename(columns={'CONTACT ID': 'contact_id', 'CONTACT RECORD CREATE TIMESTAMP': 'contact_created',
                           'Source List': 'source'},inplace=True)

call_csv_path = data_dump_base + 'tableau_cc_testing.csv'
calls_df.to_csv(call_csv_path) #switch to data_dumps

import numpy as np
import pandas as pd
import psycopg2

db_keys_path = route + '\\env_variables\\' + 'call_db_creds.pickle'
with open(db_keys_path, 'rb') as f:
    db_creds = pickle.load(f)

db_database = db_creds['database']
db_user = db_creds['user']
db_password = db_creds['password']
db_host = db_creds['host']

# will load in environmental variables once connected to AWS database
conn = psycopg2.connect(database=db_database, user=db_user, 
                        password = db_password,
                        host = db_host)

cur = conn.cursor()
cur.execute("rollback;")
query = """ SELECT call_id FROM calls """
cur.execute(query)
call_id_list_full = cur.fetchall()
current_ids_calls = [x[0] for x in call_id_list_full]
quick_list_calls = calls_df.values.tolist()

for x in quick_list_calls:
    if str(x[0]) in current_ids_calls:
        query = """ UPDATE calls
        SET contact_id = %s,
        timestamp = %s,
        campaign = %s,
        agent_name = %s,
        source = %s,
        contact_create_date = %s,
        disposition = %s,
        fdisp = %s
        WHERE call_id = %s"""
        data = (str(x[1]),x[2],str(x[3]),str(x[4]),str(x[5]),x[6],str(x[7]),str(x[8]),str(x[0]))
        cur.execute(query, data)
    else:
        query = """ INSERT INTO calls (call_id,contact_id,timestamp,campaign,agent_name,source,contact_create_date,disposition,fdisp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        data = (str(x[0]),str(x[1]),x[2],str(x[3]),str(x[4]),str(x[5]),x[6],str(x[7]),str(x[8]),)
        cur.execute(query, data)
        
cur.close()

cur = conn.cursor()
cur.execute("rollback;")
query = """ SELECT contact_id FROM contacts_five9 """
cur.execute(query)
contact_id_list_full = cur.fetchall()
current_ids_contacts = [x[0] for x in contact_id_list_full]
quick_list_contacts = contacts_df.values.tolist()

for x in quick_list_contacts:
    if str(x[0]) in current_ids_contacts:
        query = """ UPDATE contacts_five9
        SET create_time = %s,
        customer_name = %s,
        email = %s,
        number = %s,
        zip = %s,
        source = %s,
        last_disposition = %s
        WHERE contact_id = %s """
        data = (x[1],str(x[2]),str(x[3]),str(x[4]),str(x[5]),str(x[6]),str(x[7]),str(x[0]),)
        cur.execute(query, data)
    else:
        query = """ INSERT INTO contacts_five9 (contact_id,create_time,customer_name,email,number,zip,source,last_disposition)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
        data = (str(x[0]),x[1],str(x[2]),str(x[3]),str(x[4]),str(x[5]),x[6],str(x[7]),)
        cur.execute(query, data)
        
cur.close()
conn.close()
print('Call Database Updated.')