import os, sys, pickle
import argparse, sys
import pandas as pd
import crawler
import get_db_connector


parser = argparse.ArgumentParser()

parser.add_argument('-dbUser', help=' : Please set db user')
parser.add_argument('-dbPasswd', help=' : Please set db passwd')
parser.add_argument('-dbHost', help=' : Please set db host ex)127.0.0.1:1234')
parser.add_argument('-db', help=' : Please set db name')
parser.add_argument('-hdfs', help=' : Please set hdfs host')
parser.add_argument('-hdfsPort', help=' : Please set hdfs port')
parser.add_argument('-hdfsPath', help=' : Please set hdfs save path')

args = parser.parse_args()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

conf = (SparkConf().setMaster("k8s://https://192.168.219.100:6443") # Your master address name
        .set("spark.kubernetes.container.image", "joron1827/pyspark:v3") # Spark image name
        .set("spark.driver.port", "2222") # Needs to match svc
        .set("spark.driver.blockManager.port", "7777")
        .set("spark.driver.host", "driver-service.spark-joban.svc.cluster.local") # Needs to match svc
        .set("spark.driver.bindAddress", "0.0.0.0")
        .set("spark.kubernetes.namespace", "spark-joban")
        .set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-joban")
        .set("spark.kubernetes.authenticate.serviceAccountName", "spark-joban")
        .set("spark.executor.instances", "4")
        .set("spark.kubernetes.container.image.pullPolicy", "IfNotPresent")
        .set("spark.app.name", "SparkStream")
        .set("spark.executor.cores", "4")
        .set("spark.executor.memory", "16g"))


spark = SparkSession.builder.config(conf=conf).getOrCreate()


def main(argv, args, spark) : 
    print('\n')
    print(f'argv : ', argv)
    print(f'args : ', args)
    print(f'args.dbUser : ', args.dbUser)
    print(f'args.dbPasswd : ', args.dbPasswd)
    print(f'args.dbHost : ', args.dbHost)
    print(f'args.db : ', args.db)
    
    print(f'args.hdfs : ', args.hdfs)
    print(f'args.hdfsPort : ', args.hdfsPort)
    print(f'args.hdfsPath : ', args.hdfsPath)
    print('\n')
    
    ## get Stock Code and Last time of text data.
    code, dateTime = get_db_connector.get_last_time(str(args.dbUser), str(args.dbPasswd), str(args.dbHost),str(args.db))
    
    result_df = pd.DataFrame({})
    for i, j in zip(code, dateTime):
        result_df = crawler.ns_text_crawler(i, j, result_df)

       

if __name__ == '__main__' :
    argv = sys.argv
    main(argv, args)

