## dbHadoop

* get stock infomation from save parquet file
* update checksum if crawler finish to get text of last page
* save text to hdfs using pyarrow
* need to install pyarrow before using this
  
## crawler

* get title text from naver finance community


## main

* needs some arguments
  
1.  get page from file which has code, page, checksum infomation. file is saved at hdfs
2.  crawler will get text using code and page. need to set up terms for each job.
3.  save current page when crawler finish or stoped
4.  save text at hdfs as parquet file which has time with file name