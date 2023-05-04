
## get page from database
def get_page_num(user,passwd,host,db):
    
    from sqlalchemy import create_engine
    import pandas as pd
    
    engine = create_engine(f'postgresql://{user}:{passwd}@{host}/{db}')
    engine.connect()

    query = """
    SELECT stock_code, page_num FROM crawling_docs WHERE check_num = 0 ORDER BY page_num ASC;
    """
    
    df=pd.read_sql(query, con=engine)
    codes, page = df.values[0]
    return codes, page

def update_page_num(codes, page, checkSum, user,passwd,host,db):
    
    from sqlalchemy import create_engine
    import pandas as pd
    
    engine = create_engine(f'postgresql://{user}:{passwd}@{host}/{db}')
    if checkSum == False: query = f'UPDATE crawling_docs SET page_num = {page} WHERE stock_code = {codes}'
    if checkSum == True: query = f'UPDATE crawling_docs SET check_num = 1 WHERE stock_code = {codes}'

    with engine.connect() as conn:
        result = conn.execute(query)
    
    return result

def save_hdfs(data, hdfs, port, path):
    
    from datetime import datetime
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    now = datetime.now()
    table = pa.Table.from_pandas(data)
    
    # Connect to HDFS
    hdfs = pa.HadoopFileSystem(host='hdfs', port=int(port))
    
    file_name = str(now) + "text.parquet"
    hdfsPath = path + file_name
    
    with hdfs.open(hdfsPath, 'wb') as f:
       pq.write_table(table, f)
    
    hdfs.close()