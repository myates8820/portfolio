import datetime
import pandas as pd
import numpy as np
import psycopg2
import os
from sqlalchemy import create_engine, insert, update, delete, Table, Column, Integer, String, MetaData, Date, DateTime, Float, Boolean, select, BigInteger
from typing import Iterator, Optional
import io

def query_selector(query,cols,table,db,return_df=False):
    if db == 'call_database':
        conn = psycopg2.connect(database=os.environ['call_db_database'], user=os.environ['call_db_user'], 
                            password = os.environ['call_db_password'],
                            host = os.environ['call_db_host'])
    else:
        conn = psycopg2.connect(database=os.environ['database'], user=os.environ['user'], 
                            password = os.environ['password'],
                            host = os.environ['host'])
    cur = conn.cursor()
    cur.execute("rollback;")

    if cols == '*':
        final_query = "SELECT * FROM [table]"
        final_query = final_query.replace('[table]', table)
        col_query = "Select * FROM [table] LIMIT 0"
        cur.execute(col_query.replace('[table]',table))
        colnames = [desc[0] for desc in cur.description]
    else:
        query_build = ', '.join(cols)
        final_query = query.replace('[cols]', query_build).replace('[table]', table)
        col_query = "Select [cols] FROM [table] LIMIT 0"
        col_query = col_query.replace('[cols]', query_build).replace('[table]', table)
        cur.execute(col_query.replace('[table]',table))
        colnames = [desc[0] for desc in cur.description]
    cur.execute(final_query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    if return_df:
        returned = pd.DataFrame(data=data,columns=colnames)
        del data
        return returned
    else:
        data.insert(0,colnames)
        return data

# few more functions for cleaning and uploading
def id_handler(x):
    if x is None or x == '':
        return None
    else:
        return str(int(x))

def null_handler(x):
    if x == '':
        return 0
    elif x is None:
        return 0
    else:
        return x

def float_converter(column_array,df):
    for x in column_array:
        df[x].fillna(0.0,inplace=True)
        df[x] = df[x].apply(null_handler)
        df[x] = df[x].apply(lambda x: str(x).replace(',',''))
        df[x] = df[x].astype('float64')

class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


# takes sql table and matching df and uploads data. need to drop/clear table if already exists
def item_uploader(table_name,upload_df):

    df_values_list = upload_df.values.tolist()
    df_columns = upload_df.columns.tolist()

    upload_conn = psycopg2.connect(database=os.environ['database'], user=os.environ['user'], 
                            password = os.environ['password'],
                            host = os.environ['host'])


    try:
#     modified function from above link, simplified to work with paginated list and structure instead of dictionaries
        def string_io_iterator(connection,table,columns,rows: Iterator[list]) -> None:
            with connection.cursor() as cursor:
                    row_string_iterator = StringIteratorIO((
                        row + '\n'
                        for row in rows
                    ))
                    cursor.copy_from(row_string_iterator,table,columns=columns,sep=";", null='missing_data')
                    connection.commit()

        # quick loop to enable pagination for the upload
        item_number = 0
        full_length = len(df_values_list)
        final_page = False
        paginate = True
        while paginate:

            pagination_length = 500
            end_number = item_number+pagination_length
            if end_number>=full_length:
                end_index = full_length
                final_page = True
            else:
                end_index = end_number
            # print(end_index)

            value_list = []
            for x in df_values_list[item_number:end_index]:
                string_row = []
                for i in x:
                    if i is None:
                        string_row.append('missing_data')
                    elif i == 'nan':
                        string_row.append('missing_data')
                    else:
                        string_row.append(str(i).replace(';',':')) #need to fix any instances of ; so it doesn't break the delimiter
                row = ";".join(string_row)
                value_list.append(str(row))

            string_io_iterator(upload_conn,table_name,df_columns,value_list)

            if final_page:
                paginate=False
            else:
                item_number+=pagination_length
    except Exception as e:
        print(e)

query = "SELECT [cols] FROM [table]"
lead_source_cols = ['id',
 'lead_source',
 'method',
 'amount',
 'budget',
 'created_at',
 'updated_at',
 'active',
 'banked_amount',
 'budget_lead_source',
 'cac_lead_source',
 'cac_category_lead_source',
 'returnable_statuses']

lead_source_updated = query_selector(query,lead_source_cols,'settings','call_database',return_df=True)
lead_source_updated.loc[len(lead_source_updated)] = [100000,'SolarLeadFactory LLC.','',0.0,'',datetime.datetime(2021,7,1),datetime.datetime(2021,7,1),'','','',
    'Solar Lead Factory','3PL Vendors','']
lead_source_updated['cac_category_lead_source'] = np.where(lead_source_updated.cac_category_lead_source=='Other','Advertising',lead_source_updated.cac_category_lead_source)
float_converter(['amount'],lead_source_updated)

engine = create_engine(os.environ['sql_alchemy_conn_string'], echo=False)
meta = MetaData()
with engine.connect() as conn:
    conn.execute("DROP TABLE IF EXISTS lead_sources_updated;")

lead_sources_updated = Table(
    'lead_sources_updated', meta,
    Column('id',BigInteger),
    Column('lead_source',String),
    Column('method',String),
    Column('amount',Float),
    Column('budget',String),
    Column('created_at',DateTime),
    Column('updated_at',String),
    Column('active',String),
    Column('banked_amount',String),
    Column('budget_lead_source',String),
    Column('cac_lead_source',String),
    Column('cac_category_lead_source',String),
    Column('returnable_statuses',String),
)

meta.create_all(engine)
item_uploader('lead_sources_updated',lead_source_updated)
print('Uploaded Updated Lead Sources Table')