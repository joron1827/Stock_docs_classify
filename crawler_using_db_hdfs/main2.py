import os, sys, pickle
import argparse, sys

import crawler
import dbHadoop2


parser = argparse.ArgumentParser()

parser.add_argument('-dbUser', help=' : Please set db user')
parser.add_argument('-dbPasswd', help=' : Please set db passwd')
parser.add_argument('-dbHost', help=' : Please set db host ex)127.0.0.1:1234')
parser.add_argument('-db', help=' : Please set db name')
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
    print(f'args.db : ', args.db)
    
    print(f'args.hdfs : ', args.hdfs)
    print(f'args.hdfsPort : ', args.hdfsPort)
    print(f'args.hdfsPath : ', args.hdfsPath)
    print('\n')
    
    codes, page = dbHadoop2.get_page_num(str(args.dbUser), str(args.dbPasswd), str(args.dbHost),str(args.db))

    ## crawling 500 pages for each batch, batch start every 15 mins
    data, page, checkSum = crawler.ns_text_crawler(str(codes), int(page), term = 1500)

    dbHadoop2.save_hdfs(data, str(args.hdfs), int(args.hdfsPort), str(args.hdfsPath))
    
    dbHadoop2.update_page_num(str(codes), int(page), int(checkSum), str(args.dbUser), str(args.dbPasswd), str(args.dbHost),str(args.db))

if __name__ == '__main__' :
    argv = sys.argv
    main(argv, args)

