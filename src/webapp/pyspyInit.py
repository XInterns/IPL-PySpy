import pyspark                
from pyspark import SparkContext 
from pyspark.sql import SQLContext

sc = SparkContext("local","webapp")
sql = SQLContext(sc)

data_path = "input/csv/"                                # path directory to input csv files
data_opath = "output/csv/"                              # path directory to output csv files
