                                                                                                                                                   #!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
"""
-------------------------------------------------
   Description :	
   SourceFile  :	wordcount
   Author      :	yukua
   Date	       :	2021/9/22
-------------------------------------------------
"""

if __name__ == '__main__':
    os.environ['JAVA_HOME'] = 'D:\export\server\Java\jdk1.8.0_241'
    os.environ['HADOOP_HOME'] = 'D:\export\hadoopjava\hadoop-3.3.0'
    os.environ['PYSPARK_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\ProgramData\Anaconda3\python.exe'
    # spark_conf = SparkConf().setAppName("PySpark WordCount").setMaster("local[2]")
    # sc = SparkContext(conf=spark_conf)
    # input_rdd = sc.textFile("../datas/words.txt")
    # word_rdd = input_rdd.map(lambda line:line.split(" "))
    # touple = word_rdd.map(lambda word:(word,1))
    # result_rdd = touple.reduceByKey(lambda temp, item: temp + item)
    # result_rdd.foreach(lambda word:print(word))
    # sc.stop()
    spark =SparkSession.builder.appName("hah").master("local[*]").getOrCreate()
    input_rdd  = spark.sparkContext.textFile("../datas/words.txt")
    df = input_rdd.flatMap(lambda line:str(line).split(" ")).map(lambda x:[x])
    df2=df.toDF(["word"])
    df2.createOrReplaceTempView("words")
    spark.sql("""
    select word, count(*) cnt  from words group by word order  by cnt desc
    """).show()

    df3 = spark.read.format("text").load("../datas/words.txt")
    df3.withColumn("value", explode(split(df3["value"]," "))).select("value")
