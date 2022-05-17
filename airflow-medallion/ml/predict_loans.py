import snowflake.connector
import os
from dotenv import find_dotenv, load_dotenv
import pandas as pd
import seaborn as sns
#import numpy as np
#from scipy import stats
#from datetime import datetime
#from sklearn import preprocessing
#from sklearn.model_selection import KFold
#from sklearn.linear_model import LinearRegression


load_dotenv(find_dotenv())


conn = snowflake.connector.connect(
        user = os.getenv('SNOWFLAKE_USER'),
        password = os.getenv('SNOWFLAKE_PASSWORD'),
        account = os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse = 'QUALYTICS_WH',
        database = 'MEDALLION_ARCHITECTURE_DEMO',
        schema = 'DEMO',
        autocommit = True)


cursor = conn.cursor()
query=(f"""select S.ISSUE_YEAR YEAR,
                    max(L.POPULATION) POPULATION,
                    count(distinct S.id) LOAN_COUNT
                from DEMO.SILVER_LC_LOANS S
                join demo.lookups_lc_state_population L on (S.ADDR_STATE = L.STATE and S.ISSUE_YEAR = L.YEAR)
            group by S.ADDR_STATE, S.ISSUE_YEAR;""")
print(query)
output = cursor.execute(query)

df = cursor.fetch_pandas_all()

cursor.close()
del cursor 
conn.close()

#print(df.to_markdown()) 
sns.lmplot(x='POPULATION', y='LOAN_COUNT', data=df, hue='YEAR') 

