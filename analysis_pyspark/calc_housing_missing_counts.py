# Packages needed for notebook
from pyspark.sql.functions import udf, struct, col
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

def run():
    df = sc.read.csv('/final_data/*', header=True).persist()
    df = df.withColumn("null_count", df.null_count.cast("int"))

    state_grouped = df.groupBy(df.State).agg(func.mean('null_count').alias('state_null_count'))
    state_grouped.write.csv('/final_state_missing_counts',header=True)
   
    fips_grouped = df.groupBy(df.fips).agg(func.mean('null_count').alias('fips_null_count'))
    fips_grouped.write.csv('/final_fips_missing_counts',header=True)    

    state_housing_counts = df.groupBy(df.State).agg(func.count('LON').alias("num_addresses"))
    fips_housing_counts = df.groupBy(df.fips).agg(func.count('LONG').alias("num_addresses"))

    state_housing_counts.write.csv('/final_state_housing_counts',header=True)
    fips_housing_counts.write.csv('/final_fips_housing_counts',header=True)

    return

if __name__ == "__main__":
    sc = SparkSession.builder.appName('missingness_for_plotting').getOrCreate()
    s_context = sc.sparkContext
    run()
    print("run succesful")

