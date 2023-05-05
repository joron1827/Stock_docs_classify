
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
    print("codes와 page를 찾았습니다.",codes,page)
    return codes, page

def update_page_num(codes, page, checkSum, user,passwd,host,db):
    
    from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, update, text

    engine = create_engine(f'postgresql://{user}:{passwd}@{host}/{db}')
    conn = engine.connect()

    metadata = MetaData()
    CRAWL = Table('crawling_docs', metadata, autoload_with=engine)


    if checkSum == False:
        print('page를 저장합니다.') 
        u = update(CRAWL).where(CRAWL.c.stock_code == codes).values({"page_num": page})
        conn.execute(u)
    if checkSum == True: 
        print('마지막 페이지 도달')
        u = update(CRAWL).values({"check_num": 1}).where(CRAWL.c.stock_code == codes)
        conn.execute(u)
    
    return 

def save_hdfs(data, hdfs, port, path):
    
    from datetime import datetime
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    now = datetime.now()
    now = now.strftime('%y-%m-%d-%h-%M-%s')
    table = pa.Table.from_pandas(data)
    
    # Connect to HDFS
    hdfs = pa.HadoopFileSystem(host=hdfs, port=int(port))
    
    file_name = str(now) + ".parquet"
    hdfsPath = path + file_name
    
    with hdfs.open(hdfsPath, 'wb') as f:
       pq.write_table(table, f)
    
    hdfs.close()