# Packages needed for notebook
import pandas as pd
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def calc_fip(row, fip_dict):
    try:
        fips =  fip_dict[str(row.infer_zip)]
        return fips
    except:
        return None       
        
def udf_fip(fip_dict):
    return udf(lambda l: calc_fip(l,fip_dict))

def add_zeroes(value):
    if len(value)==5:
        return value
    elif len(value)==4:
        new_value = '0'+str(value)
        return new_value
    elif len(value)==3:
        new_value = '00'+str(value)
        return new_value
    else:
        return None

def run():
    # define state list
    statesCap = [ "AL", "AR", "CA", "CO", "CT", "DC", "DE", 
                 "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", 
                 "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", 
                 "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", 
                 "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", 
                 "VT", "VA", "WA", "WV", "WI", "WY"] #AK excluded as ran in test 
    states = [x.lower() for x in statesCap]
    fips_map = pd.read_csv('/home/wce/clsadmin/data/ZIP-COUNTY-FIPS_2017-06.csv')
    for state in states:
        pathway = '/states_with_zip/%s/*' %state
        df = sc.read.csv(pathway, header=True, schema = StructType([StructField("LON",DoubleType()),StructField("LAT",DoubleType()),StructField("Number",IntegerType()),StructField("Street",StringType()),StructField("Unit",IntegerType()),StructField("City",StringType()),StructField("District",StringType()),StructField("POSTCODE",StringType()),StructField("ID",IntegerType()),StructField("Hash",StringType()),StructField("State", StringType()),StructField("null_count",IntegerType()),StructField("infer_zip",StringType())])).persist()
        df = df.rdd.repartition(256).toDF(sampleRatio=1.0)
#        fip_codes = fips_map[fips_map['STATE']==state.upper()]
        fip_codes = fips_map # added because the above had to be commented out
        fip_codes = fip_codes[['ZIP','STATE','STCOUNTYFP']]
        fip_codes = fip_codes.drop_duplicates()
        fip_codes['STCOUNTYFP'] = fip_codes['STCOUNTYFP'].astype(str)
        fip_codes['ZIP'] = fip_codes['ZIP'].astype(str)
        fip_codes['STCOUNTYFP'] = fip_codes['STCOUNTYFP'].apply(add_zeroes)
        fip_codes['ZIP'] = fip_codes['ZIP'].apply(add_zeroes)
        fip_dict = {}
        for index,row in fip_codes.iterrows():
            fip_dict[row['ZIP']] = row['STCOUNTYFP']
    
        output_path = '/final_data/%s' %state
        print(output_path)
        df = df.withColumn('infer_zip',df.infer_zip.cast("string"))
        final_df = df.withColumn("fips", udf_fip(fip_dict)(struct(['infer_zip']))).persist()
        final_df = final_df.fillna('')
        final_df = final_df.withColumn("District", final_df.District.cast("string"))
        final_df = final_df.withColumn("ID", final_df.ID.cast("string"))
        final_df = final_df.withColumn("Number", final_df.Number.cast("int"))
        final_df = final_df.withColumn("Street", final_df.Street.cast("string"))
        final_df = final_df.withColumn("Unit", final_df.Unit.cast("int"))
        final_df = final_df.withColumn("City", final_df.City.cast("string"))
        final_df = final_df.withColumn("POSTCODE", final_df.POSTCODE.cast("string"))
        final_df = final_df.withColumn("infer_zip", final_df.infer_zip.cast("string"))
        final_df = final_df.withColumn("fips", final_df.fips.cast("string"))
        final_df.show(100)
        final_df.write.csv(output_path, header=True, nullValue='')
    return


if __name__ == "__main__":
    sc = SparkSession.builder.appName('state_zips').getOrCreate()
    s_context = sc.sparkContext
    run()
    print("run succesful")
