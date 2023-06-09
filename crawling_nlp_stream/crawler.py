
def ns_text_crawler(codes : str, date_last, result_df):
    
    import requests
    import pandas as pd
    from bs4 import BeautifulSoup
    import re
    from time import time

    HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "sec-ch-ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    #     "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    "User-Agent": "Mozilla/5.0"
    }
    


    start = 1

    for page in range(start, 100):
        url = "https://finance.naver.com/item/board.naver?code=%s&page=%s" %(str(codes), str(page))
        html = requests.get(url, headers=HEADERS).content
        soup = BeautifulSoup(html.decode('euc-kr', 'replace'), 'html.parser')
        table = soup.find('table', {'class': 'type2'})
        try:
            tb = table.select('tbody > tr')
        except:
            print(url)
            return result_df
        ## save text at result_df
        for i in range(2, len(tb)):
            if len(tb[i].select('td > span')) > 0:
                ## catch last page
                s= tb[i].select('a')[0]['href']
                real_page_num = re.findall(r'page=(\d{1,9})', s) ## actual page number

                ## if catch lastpage, stop crawling and return values
                # if str(page) != real_page_num[0]: return result_df, current_page, True 

                date = tb[i].select('td > span')[0].text
                if(datetime.strptime(date) < datetime.strptime(date_last)):
                    print(codes)
                    return result_df
                title = tb[i].select('td.title > a')[0]['title']
                views = tb[i].select('td > span')[1].text
                pos = tb[i].select('td > strong')[0].text
                neg = tb[i].select('td > strong')[1].text
                table = pd.DataFrame({'code' : codes, 'date':[date], 'title' : [title], 'views': [views], 'pos' : [pos], 'neg' : [neg]})
                result_df = pd.concat([result_df,table], axis=0)
    return result_df