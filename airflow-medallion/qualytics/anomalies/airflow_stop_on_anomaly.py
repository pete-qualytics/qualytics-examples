import snowflake.connector
import os
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

def check(warehouse, database, schema, table, tag):
    conn = snowflake.connector.connect(
            user = os.getenv('SNOWFLAKE_USER'),
            password = os.getenv('SNOWFLAKE_PASSWORD'),
            account = os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse = warehouse,
            database = database,
            schema = schema,
            autocommit = True)


    cursor = conn.cursor()
    query=(f"""select count(*) cnt from {database}.{schema}.{table} where quality_check_tags = '{tag}';""")
    print(query)
    output = cursor.execute(query)

    for i in cursor:
        print(i[0])
        anomalyCount = int(i[0])
    
    if anomalyCount > 0:
        raise ValueError('Critical errors present')
    
    cursor.close()
    del cursor 
    conn.close()
 
#check('QUALYTICS_WH', 'MEDALLION_ARCHITECTURE_DEMO', 'DEMO','_MEDALLION_S3_SOURCE_RECORDS', 'Critical')
