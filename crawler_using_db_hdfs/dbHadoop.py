
## get page from database
def get_page_num(hdfsHost, port, path):
    
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd
    
    # Connect to HDFS
    hdfs = pa.HadoopFileSystem(host=hdfsHost, port=int(port))
    
    # code, page info path
    file_name = "code/code_info.parquet"
    hdfsPath = path + file_name
    
    # open info file and create df to get code&page
    with hdfs.open(hdfsPath) as f:
        table = pq.read_table(f)
        df = table.to_pandas()    
    hdfs.close()
    

    # extract codes and page not check_num == 1
    codes, page = df[df.check_num != 1].loc[0,['stock_code','page_num']].values

    return str(codes), int(page)


def update_page_num(codes, page, checkSum, hdfsHost, port, path):
    
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd

    hdfs = pa.HadoopFileSystem(host=hdfsHost, port=int(port))
    
    file_name = "code/code_info.parquet"
    hdfsPath = path + file_name

    with hdfs.open(hdfsPath) as f:
        table = pq.read_table(f)
        df = table.to_pandas()
    hdfs.close()


    # save current page using code
    df.loc[df['stock_code'] == codes, 'page_num'] = page

    # if finish get text of last page, check_num will be 1
    if checkSum == True:
        df.loc[df['stock_code'] == codes, 'check_num'] = 1

    # save new table at same file
    
    hdfs = pa.HadoopFileSystem(host=hdfsHost, port=int(port))
    new_table = pa.Table.from_pandas(df)

    with hdfs.open(hdfsPath, 'wb') as f:
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