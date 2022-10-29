#importing libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def create_s3_object():    
    creds = get_creds()
    s3_conn=boto3.resource(service_name=creds['AWS_S3']['service_name'],
                    region_name=creds['AWS_S3']['region_name'],
                    aws_access_key_id=creds['AWS_S3']['aws_access_key_id'],
                    aws_secret_access_key=creds['AWS_S3']['aws_secret_access_key'])   
    return s3_conn

def date_clean(incoming_date):
    """Input:incoming_date (df['date_column'])
    Output:datetime: datetime value
    """
    
    if isinstance(incoming_date, datetime):
        return incoming_date
    elif incoming_date == None:
        return pd.NaT
    elif isinstance(incoming_date, str):
        if  incoming_date == '0000-00-00 00:00:00':
            return pd.NaT
        else:
            datetime_object = datetime.strptime(incoming_date, '%Y-%m-%d %H:%M:%S')
            return datetime_object
    else:
        if (incoming_date.isna()==True):
            return pd.NaT
        
        
def connect_to_sqldb(host,user,password,database):
        mydb = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database
        )
        return mydb
    

def connect_to_postgres(database, host, port , user,password):
    postgres_conn = psycopg2.connect(
        "host='" + host + \
        "' port="+ port +" user=" + user + \
        " password=" + password + 
        " dbname='" + database + "'"
    )
    return postgres_conn
 
#fetch data from S3 db
def fetch_file_from_s3(filepath_name,sheet_no,bucket_name,file_type): 
    s3_conn=create_s3_object()
    if True:
        obj = s3_conn.Bucket(bucket_name).Object(filepath_name).get()
        if file_type=='excel':
            df = pd.read_excel(io.BytesIO(obj['Body'].read()),sheet_no)
        elif file_type=='json':
            obj = s3.get_object(Bucket=bucket_name, Key=filepath_name)
            df = pd.read_json(obj['Body'],lines=True, compression='gzip')
        else:
            df = pd.read_csv(io.BytesIO(obj['Body'].read()),sheet_no)
        return df
  else:
    return 0

def get_df_rows(df):
    #input: takes a pandas.DataFrame type data.
    #output: return a integer with the number of rows.
    return len(df.index)

def get_today_datetime(timezone, date=False):
    #input:timezone (String) ist or utc,date: True or False(default)
    #output:datetime: Returns datetime by default,date: Returns date if date=True
    if timezone == 'ist':
        tz = pytz.timezone('Asia/Kolkata')
    elif timezone == "utc":
        tz = pytz.timezone('UTC')
    if date:
        now_date = datetime.now(tz).date()
        return now_date
    else:
        now_datetime = datetime.now(tz)
        return now_datetime
