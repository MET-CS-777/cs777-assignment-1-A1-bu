from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output1> <output2>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    lines = sc.textFile(sys.argv[1])

    #Task 1
    #Your code goes here
    rows = lines.map(lambda line: line.split(','))

    # filter using correctRows
    valid_rows = rows.filter(correctRows)

    # map to (medallion, hack_license), count
    pair_counts = (
        valid_rows.map(lambda x: ((x[0], x[1]), 1))
        .reduceByKey(lambda a, b: a + b)
    )

    # top 10 by count
    top10_t = pair_counts.takeOrdered(10, key=lambda x: -x[1])

    # change to rdd for saving
    results_1 = sc.parallelize(top10_t)

    results_1.coalesce(1).saveAsTextFile(sys.argv[2])
    print(results_1)


    #Task 2
    #Your code goes here

    # map to (driver, total, trip time), count
    valid_rows = rows.filter(correctRows).filter(lambda x: x[5] != 0)

    d_pairs = (valid_rows.map(lambda x: ((x[1], float(x[16]) / float(x[5])))))

    top10_d = d_pairs.takeOrdered(10, key= lambda x: -x[1])

    # Get top 10 by count
    results_2 = sc.parallelize(top10_d)

    #savings output to argument
    results_2.coalesce(1).saveAsTextFile(sys.argv[3])
    print(results_2)


    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here

    sc.stop()

