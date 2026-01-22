#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine


year = 2-21
month = 1

pg_user = 'root'
pg_pass = 'root'
pg_host = 'localhost'
pg_db = 'ny_taxi'
pg_port = 5432

prefix='https://github.com/DataTalksClub/nyc-tlc-data/releases/download'
url=f'{prefix}/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz'
url

df = pd.read_csv(url)
df.head()
len(df)
df

dtype={
    "VendorID": "Int64" ,
    "passenger_count": "Int64", 	
    "trip_distance": "float64", 	
    "RatecodeID": "Int64", 	
    "store_and_fwd_flag":"string", 	
    "PULocationID": "Int64", 	
    "DOLocationID": "Int64", 	
    "payment_type": "Int64", 	
    "fare_amount": "float64", 	
    "extra": "float64", 	
    "mta_tax": "float64", 	
    "tip_amount": "float64", 	
    "tolls_amount": "float64", 	
    "improvement_surcharge": "float64", 	
    "total_amount": "float64", 	
    "congestion_surcharge": "float64"
    }
parse_dates = [
    "tpep_pickup_datetime", 	
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    url,
    dtype = dtype,
    parse_dates=parse_dates
)




engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
print(pd.io.sql.get_schema(df,name='yellow_taxi_data',con=engine))


# In[78]:


df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# When we insert data we dont dump all the data at once.
# Theres no way to track progresss or to know if there is some issue.
# Best way is to break it down in to chunks

# In[79]:


df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000,
)


# next(df_iter) is used to get the next iteration / chunk / batch

# In[80]:


#df = next(df_iter)


# instead of using next we will iterate over it using for 
# and to see the progress we will use library called tqdm

# In[81]:


from tqdm.auto import tqdm


# In[82]:


for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[ ]:




