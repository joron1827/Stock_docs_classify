import os, sys, pickle
import argparse, sys

import crawler
import dbHadoop


parser = argparse.ArgumentParser()

parser.add_argument('-dbUser', help=' : Please set Database user name') 
parser.add_argument('-dbPasswd', help=' : Please set Database user password')
parser.add_argument('-dbHost', help=' : Please set Database host ex)127.0.0.1:1223')
parser.add_argument('-dbName', help=' : Please set Database name')
parser.add_argument('-hdfs', help=' : Please set hdfs host')
parser.add_argument('-hdfsPort', help=' : Please set hdfs port')
parser.add_argument('-hdfsPath', help=' : Please set hdfs save path')

args = parser.parse_args()

def main(argv, args) : 
    print('\n')
    print(f'argv : ', argv)
    print(f'args : ', args)
    
    print(f'args.dbUser : ', args.dbUser)
    print(f'args.dbPasswd : ', args.dbPasswd)
    print(f'args.dbHost : ', args.dbHost)
    print(f'args.dbName : ', args.dbName)
    print(f'args.hdfs : ', args.hdfs)
    print(f'args.hdfsPort : ', args.hdfsPort)
    print(f'args.hdfsPath : ', args.hdfsPath)
    print('\n')
    
    codes, page = dbHadoop.get_page_num(str(args.dbUser), str(args.dbPasswd),str(args.dbHost),str(args.dbName))

    ## crawling 500 pages for each batch, batch start every 10 mins
    data, page, checkSum = crawler.ns_text_crawler(codes, page, term = 10)
    dbHadoop.update_page_num(codes, page, checkSum, str(args.dbUser), str(args.dbPasswd), str(args.dbHost),str(args.dbName))
    dbHadoop.save_hdfs(data, str(args.hdfs), int(args.hdfsPort), str(args.hdfsPath))
    
if __name__ == '__main__' :
    argv = sys.argv
    main(argv, args)

