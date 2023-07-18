# Functions for crawling text data of Naver Stock Community

* We are going to crawl text data of Naver Stock Community, (belongs KOSPI)
* These Functions should be working on K8s system via Cronjob
* To prevent blocked from Naver when if request, We use Batch process using Cronjob
* Text data will be saved at hdfs using pyarrow

---
## dbHadoop

* get stock infomation from save parquet file
* update checksum if crawler finish to get text of last page
* save text to hdfs using pyarrow
* need to install pyarrow before using this

#### get_page_num
* stock community has a lot of pages. so when if crawling stopped, crawler needs state of last crawled page
* get page information from Postgresql which has state of stock community pages.

#### update_page_num
* save page after crawling every cycle at postgresql

#### save_hdfs
* save crawled text file at hdfs using pyarrow
  
---

## crawler

* get title text from naver finance community
* crawler returns page number and text data frame

---

## main

* needs some arguments
  
1.  get page from file which has code, page, checksum infomation. file is saved at hdfs
2.  crawler will get text using code and page. need to set up terms for each job.
3.  save current page when crawler finish or stoped
4.  save text at hdfs as parquet file which has time with file name