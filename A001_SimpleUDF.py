import os
import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, concat, concat_ws
from pyspark.sql.types import DoubleType, StringType

os.environ['PYSPARK_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python310/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Admin/AppData/Local/Programs/Python/Python310/python.exe'

print("----------------------Pre - env - setup--------------------------")


#sc=SparkContext(appName="Batch 38 first app", master="local[*]") # 8 (all avialble cores) - RDD - single session
#spark=SparkSession.builder.appName("batch 38 first df").master("local[*]").getOrCreate() - DF - multi session

spark=SparkSession.builder.appName("batch 38 UDF").master("local[*]").getOrCreate()

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(r"C:\Users\Admin\PythonProject\PythonProject\Batch38\files\supermarket.csv")
df.show()

# upper => hi => HI
# spark + python
# D     + S      = constant folding
# udf < function


# python  - standalone
def convert_upper(input):
    return input.upper() # STRING

# spark + python
udf_upper=udf(convert_upper, StringType())

#spark - distributed
df.withColumn("upper_data", udf_upper(col("place"))).show()