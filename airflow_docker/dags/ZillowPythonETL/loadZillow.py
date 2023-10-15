import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

def load(ti):
    load_dotenv()
    user = os.environ.get('DB_USER')
    password = os.environ.get('DB_PWD')
    host = os.environ.get('DB_HOST')
    
    # Get name of CSV with clean data
    filename = ti.xcom_pull(key='clean_filename', task_ids='clean_zillow')

    url = f'postgresql://{user}:{password}@{host}'

    db = create_engine(url)
    conn = db.connect()

    df = pd.read_csv(filename)
    df.to_sql('sourcezillow', con=conn, if_exists='append', index=False)
