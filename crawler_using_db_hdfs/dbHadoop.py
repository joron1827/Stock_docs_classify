
## get page from database
def get_page_num(hdfs, port, path):
    
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd
    
    # Connect to HDFS
    hdfs = pa.HadoopFileSystem(host=hdfs, port=int(port))
    
    file_name = "code/code_info.parquet"
    hdfsPath = path + file_name
    
    with hdfs.open(hdfsPath) as f:
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

    hdfs = pa.HadoopFileSystem(host=hdfs, port=int(port))
    
    file_name = "code/code_info.parquet"
    hdfsPath = path + file_name

    with hdfs.open(hdfsPath) as f:
        table = pq.read_table(table, f)
        df = table.to_pandas()
    hdfs.close()

    if checkSum == False:
        df = df.loc[df['stock_code'] == codes, 'page_num'] = page

    if checkSum == True:
        df = df.loc[df['stock_code'] == codes, 'check_num'] = 1

    hdfs = pa.HadoopFileSystem(host=hdfs, port=int(port))
    new_table = pa.Table.from_pandas(df)

    with hdfs.open(hdfsPath) as f:
        pq.write_table(new_table, f)

    hdfs.close()
    
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