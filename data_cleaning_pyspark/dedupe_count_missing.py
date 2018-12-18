# Packages needed for notebook
import pandas
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

# define state list
statesCap = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", 
             "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", 
             "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", 
             "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", 
             "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", 
             "VT", "VA", "WA", "WV", "WI", "WY"]
states = [x.lower() for x in statesCap]

def add_state_and_dedupe(states):
    """
    Adds known state to each set of files, dedupes, and unions into a single df
    """
    first_state = True
    for state in states:
        pathway = "/data/%s/*" %state
        print(pathway)
        df = sc.read.csv(pathway, header=True).persist()
        df = df.withColumn('State',lit(state))
        df = df.drop_duplicates().persist()
        df = df.drop('REGION').persist()
        output_path = '/clean_states/%s' %state
        df.write.csv(output_path, header=True)
    return "Write Complete"

def count_missing(states):
    """
    Adds known state to each set of files, dedupes, and unions into a single df
    """
    for state in states:
        pathway = "/clean_states/%s/*" %state
        print(pathway)
        df = sc.read.csv(pathway, header=True).persist()
        count_empty_columns = udf(lambda row: len([x for x in row if x == None]), IntegerType())
        new_df = df.withColumn("null_count", count_empty_columns(struct([df[x] for x in df.columns])))
        output_path = '/states_missingness/%s' %state
        new_df.write.csv(output_path, header=True)
    return "Write Complete"

def run():
    add_state_and_dedupe(states)
    count_missing(states)
    return

if __name__ == "__main__":
    sc = SparkSession.builder.appName('dedupe_count_nulls').getOrCreate()
    s_context = sc.sparkContext
    run()
    print("run succesful")
