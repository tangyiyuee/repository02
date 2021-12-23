#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
import os
import time
"""
-------------------------------------------------
   Description :	创建SparkContext上下实例化对象
   SourceFile  :	01_test_sparkcontext
   Author      :	yukua
   Date	       :	2021/9/22
-------------------------------------------------
"""

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:\export\server\Java\jdk1.8.0_241'
    os.environ['HADOOP_HOME'] = 'D:\export\hadoopjava\hadoop-3.3.0'
    os.environ['PYSPARK_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    spark_conf = SparkConf().setAppName("SparkTest").setMaster("local[2]")

    # 2. 构建SparkContext上下文对象
    sc = SparkContext(conf=spark_conf)
    # <SparkContext master=local[2] appName=SparkTest>
    print(sc)
    time.sleep(100000)
    # 应用结束，关闭资源
    sc.stop()