import sys
import pandas as pd 

print('arguments', sys.argv)

month = int(sys.argv[1])
print(f'Hello Pipeline, month ={month}')

df = pd.DataFrame({"day":[1, 3],"num_passengers":[3, 4]})
df['month'] = month

df.to_parquet(f"output_{month}.parquet")

print(df.head())

