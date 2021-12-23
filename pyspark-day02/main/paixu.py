#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from pyspark import SparkConf, SparkContext
import re
import sys
"""
-------------------------------------------------
   Description :	
   SourceFile  :	paixu
   Author      :	yukua
   Date	       :	2021/9/24
-------------------------------------------------
"""

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_65'
    os.environ['HADOOP_HOME'] = '/export/server/hadoop-3.3.0'
    os.environ['PYSPARK_PYTHON'] = '/export/server/anaconda3/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/export/server/anaconda3/bin/python3'
    spark_conf = SparkConf().setAppName("pyspark countword")
    args = sys.argv
    sc = SparkContext(conf=spark_conf)
    input_rdd = sc.textFile(args[1])
    result_rdd = input_rdd.flatMap(lambda line: re.split("\\s+", line)) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda tmp, item: tmp + item)

    result_rdd.saveAsTextFile(args[2])
    sc.stop()