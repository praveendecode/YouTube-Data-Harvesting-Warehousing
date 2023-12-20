from sqlalchemy import create_engine
import psycopg2
import pandas as pd


df = pd.read_csv('RFS_new.csv')  

db_url = 'postgresql://postgres:root@localhost:5432/rfs'

engine = create_engine(db_url)

table_name = 'retail_sales_forecast' 

df.to_sql(table_name,engine, index=False, if_exists='replace')

engine.dispose()
