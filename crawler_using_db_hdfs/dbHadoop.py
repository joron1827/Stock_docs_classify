
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
    
    import pandas as pd
    from sqlalchemy import Table,  MetaData, create_engine, update

    engine = create_engine(f'postgresql://{user}:{passwd}@{host}/{db}')
    engine.connect()

    
 
    
    metadata = MetaData()
    CRAWL = Table('crawling_docs', metadata, autoload_with=engine)

    if checkSum == False: u = update(CRAWL).values({"page_num": page}).where(CRAWL.c.stock_code == codes)
    if checkSum == True: u = update(CRAWL).values({"check_num": 1}).where(CRAWL.c.stock_code == codes)
    
    engine.execute(u)
    
    return 

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