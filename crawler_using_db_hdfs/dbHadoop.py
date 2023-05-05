
## get page from database
def get_page_num(hdfs, port, path):
    
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd
    
    
    # Connect to HDFS
    hdfs = pa.HadoopFileSystem(host=hdfs, port=int(port))
    
    file_name = "code/code_info.parquet"
    hdfsPath = path + file_name
    
    with hdfs.open(hdfsPath, 'rb') as f:
        table = pq.read_table(table, f)
        df = table.to_pandas()    
    hdfs.close()
    
    # check_num이 1이 아닌 종목만 추출
    codes, page = df[df.check_num != 1].loc[0,['stock_code','page_num']].values

    return str(codes), int(page)

def update_page_num(codes, page, checkSum, hdfs, port, path):
    
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd

    # Connect to HDFS
    # HDFS 경로 설정
    hdfs_path = f"{path}code/code_info.parquet"

    # HDFS에 저장된 Parquet 파일 읽기
    fs =  pa.HadoopFileSystem(host=hdfs, port=int(port))
    table = pq.read_table(hdfs_path, filesystem=fs)

    # PyArrow Table을 Pandas DataFrame으로 변환
    df = table.to_pandas()

    if checkSum == False:
        df.loc[df['stock_code'] == codes, 'page_num'] = page

    if checkSum == True:
        df.loc[df['stock_code'] == codes, 'check_num'] = 1

    # 업데이트된 Pandas DataFrame을 PyArrow Table로 변환
    table = pa.Table.from_pandas(df)

    # 업데이트된 PyArrow Table을 Parquet 파일로 저장
    pq.write_table(table, hdfs_path, filesystem=fs)
    

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