import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    print(len(sys.argv))
    if (len(sys.argv) != 3):
        print("Usage: spark-etl [input-folder] [output-folder]")
        sys.exit(0)

    spark = SparkSession\
        .builder\
        .appName("CapstoneJigsaw")\
        .getOrCreate()

    loans = spark.read.option("inferSchema", "true").option("header", "true").csv(sys.argv[1])

    loans.printSchema()

    print(loans.show())

    print("Total number of records: " + str(loans.count()))

    loans.write.parquet(sys.argv[2])
