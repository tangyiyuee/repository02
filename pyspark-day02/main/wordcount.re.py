#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from pyspark import SparkConf,SparkContext
import re
"""
-------------------------------------------------
   Description :	
   SourceFile  :	wordcount1
   Author      :	yukua
   Date	       :	2021/9/22
-------------------------------------------------
"""

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:\export\server\Java\jdk1.8.0_241'
    os.environ['HADOOP_HOME'] = 'D:\export\hadoopjava\hadoop-3.3.0'
    os.environ['PYSPARK_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    Spark_Conf = SparkConf().setAppName("spark wordcount").setMaster("local[2]")
    sc = SparkContext(conf=Spark_Conf)
    input_rdd = sc.textFile("../datas/words.txt")
    result_rdd = input_rdd.flatMap(lambda line: re.split("\\s+", line)) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda tmp, item: tmp + item)
    result_rdd.foreach(lambda line:print(line))
    sc.stop()
