# Stock_docs_classify

This project aims to perform sentiment analysis using stock data and text data, and visualize the results. It collects text data from Naver stock discussion forums, conducts labeling based on specific criteria, and then analyzes it using Spark.


## System

* Kubernetes
    * Kubernetes is the environment where all components of this project operate.
    * Both distributed processing and data storage function in a containerized environment.
* Hadoop
    * Hadoop file system(hdfs) is running with container using helm Chart.
    * Text data will be saved on hdfs 
* JupyterHub
    * JupyterHub is enable to run notebook on K8s.
    * Pyspark can be executed via jupyter notebook using spark session.
    * SQLAlchemy, Pandas, Other libraries are working on jupyter notebook.
* Pyspark
    * Pyspark for Data processing, Analysis.
    * Spark executor will be executed via cluster mode on K8s system
* PostgreSQL
    * It is used for temporary storage of certain information that occurs during data crawling.
    * Save data of inferred data.
    * Flask app uses PostgreSQL to get result of sentimetal analysis.


## Data Labeling

### Stock Price Data
* Get Stock Price Data using API of Daishin.
* Caculate RSI and Local Min/Max using Pyspark
* Get Time Data Using RSI (Under 30 : Buy, Over 70 : Sell)
* Get Time Data Using Local Min/Max (Moving averages)

### Text Data
* Get text data of each KOSPI Stock from Naver Finance Community.
* K8s system should run Crawler as batch process to prevent blocking from Naver.
* Text Data will be save at hdfs using pyarrow.
* Using Time Data of Stock Price, Text data can get Label for each Stocks


## NLP(Natural Language Processing)
* Tokenize
* Stopword
* Top word remove
* Vectorize(TF-IDF)



