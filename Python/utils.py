#importing libraries
import pandas as pd
import numpy as np


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

