#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from pyspark import SparkConf, SparkContext
import  re
import sys
"""
-------------------------------------------------
   Description :	
   SourceFile  :	canshu
   Author      :	yukua
   Date	       :	2021/9/24
-------------------------------------------------
"""

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:\export\server\Java\jdk1.8.0_241'
    os.environ['HADOOP_HOME'] = 'D:\export\hadoopjava\hadoop-3.3.0'
    os.environ['PYSPARK_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    spark_conf = SparkConf().setAppName("pyspark countword").setMaster("local[2]")
    sc = SparkContext(conf=spark_conf)
    args = sys.argv
    if (len(args)!=3):
        print("没参数")
    input_rdd = sc.textFile(args[1])
    result_rdd = input_rdd.flatMap(lambda line: re.split("\\s+", line)) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda tmp, item: tmp + item)
    result_rdd.saveAsTextFile(args[2])
