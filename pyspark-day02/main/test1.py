                                                                                                                                                                                                                                                                                                                                                                                                                                             #!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from pyspark import SparkConf, SparkContext
import re
"""
-------------------------------------------------
   Description :	
   SourceFile  :	test1
   Author      :	yukua
   Date	       :	2021/9/23
-------------------------------------------------
"""

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:\export\server\Java\jdk1.8.0_241'
    os.environ['HADOOP_HOME'] = 'D:\export\hadoopjava\hadoop-3.3.0'
    os.environ['PYSPARK_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    spark_conf = SparkConf().setAppName("pyspark countword").setMaster("local[3]")
    sc = SparkContext(conf=spark_conf )
    input_rdd = sc.textFile("../datas/words.txt")
    word_rdd = input_rdd.flatMap(lambda line : re.split("\\s+", line))
    touple = word_rdd.map(lambda word:(word,1))
    result = touple.reduceByKey(lambda tmp,item : tmp+item)
    result.foreach(lambda item: print(item))
    result.saveAsTextFile("../datas/output1")
    sc.stop()


