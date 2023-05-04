
def ns_text_crawler(codes : str, start, term):
    
    import requests
    import pandas as pd
    from bs4 import BeautifulSoup
    import re
    
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
    
    result_df = pd.DataFrame([])
    current_page = start
    ## 시작 페이지부터 한번에 수집할 페이지 텀만큼 반복 수행
    for page in range(start+1, start+1+term):
        
        url = "https://finance.naver.com/item/board.naver?code=%s&page=%s" %(codes, str(page))    
        html = requests.get(url, headers=HEADERS).content
        soup = BeautifulSoup(html.decode('euc-kr', 'replace'), 'html.parser')
        table = soup.find('table', {'class' : 'type2'})
        tb = table.select('tbody > tr')
        
        ## 마지막 페이지+1으로 조회해도 마지막 페이지와 동일함
        s= tb[5].select('a')[0]['href']
        real_page_num = re.findall(r'page=(\d{1,9})', s) ## 실제 페이지 번호
        
        if str(page) != real_page_num[0]: return result_df, current_page, True ## 페이지 번호가 다를 경우 중지.
        
        ## 게시글 하나씩 result_df에 저장
        for i in range(2, len(tb)):
            if len(tb[i].select('td > span')) > 0:
                date = tb[i].select('td > span')[0].text
                title = tb[i].select('td.title > a')[0]['title']
                views = tb[i].select('td > span')[1].text
                pos = tb[i].select('td > strong')[0].text
                neg = tb[i].select('td > strong')[1].text
                table = pd.DataFrame({'code' : codes, '날짜':[date], '제목' : [title], '조회': [views], '공감' : [pos], '비공감' : [neg]})
                result_df = pd.concat([result_df,table], axis=0)
        current_page = page
                
    ## 완료시 df, page, check_sum 반환   
    return result_df, current_page, False