
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

    print("code :", codes , "page: ", page)

    return str(codes), int(page)


def update_page_num(codes, page, checkSum, user,passwd,host,db):
    
    from sqlalchemy import create_engine
    import pandas as pd
    
    engine = create_engine(f'postgresql://{user}:{passwd}@{host}/{db}')
    engine.connect()
    
    query = """
    SELECT * FROM crawling_docs;
    """
    df=pd.read_sql(query, con=engine)

    # save current page using code
    df.loc[df['stock_code'] == codes, 'page_num'] = page

    # if finish get text of last page, check_num will be 1
    if checkSum == True:
        df.loc[df['stock_code'] == codes, 'check_num'] = 1
    
    df.to_sql(name='crawling_docs', con=engine, if_exists='replace', index=False)

    return

def save_hdfs(data, hdfs, port, path):
    
    from datetime import datetime
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    now = datetime.now()
    now = now.strftime('%y/%m/%d, %H:%M:%S')
    table = pa.Table.from_pandas(data)
    
    print("수집된 데이터 샘플: ")
    print(data.head())

    # Connect to HDFS
    hdfs = pa.HadoopFileSystem(host=hdfs, port=int(port))
    
    file_name = str(now) + "-postgres.parquet"

    print("저장될 파일 이름 : ", file_name)
    print("저장될 파일 경로: ", path)

    hdfsPath = path + file_name
    
    with hdfs.open(hdfsPath, 'wb') as f:
       pq.write_table(table, f)
    
    hdfs.close()