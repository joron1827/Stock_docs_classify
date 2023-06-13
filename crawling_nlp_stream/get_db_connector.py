
## get page from database
def get_code(user,passwd,host,db):
    
    from sqlalchemy import create_engine
    import pandas as pd
    
    engine = create_engine(f'postgresql://{user}:{passwd}@{host}/{db}')
    engine.connect()

    query = """
    SELECT stock_code, page_num FROM crawling_docs;
    """
    
    df=pd.read_sql(query, con=engine)
    codes, page = df.values[0]

    print("code :", codes , "page: ", page)

    return str(codes), int(page)

def get_last_time(user,passwd,host,db):

    from sqlalchemy import create_engine
    import pandas as pd
    
    engine = create_engine(f'postgresql://{user}:{passwd}@{host}/{db}')
    engine.connect()

    query = """
    SELECT code, date
        FROM (
            SELECT code, date, ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS row_num
            FROM processed_data
        ) subquery
        WHERE row_num = 1;
    """
    
    df=pd.read_sql(query, con=engine)
    codes, page = df.values[0]
    
    return str(codes), str(page)

def update_text(df, user,passwd,host,db):
    
    from sqlalchemy import create_engine
    import pandas as pd
    
    engine = create_engine(f'postgresql://{user}:{passwd}@{host}/{db}')
    engine.connect()
    

    df.to_sql(name='processed_data', con=engine, if_exists='append', index=False)

    return
