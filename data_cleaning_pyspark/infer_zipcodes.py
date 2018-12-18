# Packages needed for notebook
import pandas as pd
import math
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def distance(origin, destination):
    """
    Used previously written code as no need to reinvent wheel
    Function calculates distance between two points assuming Earth is a perfect sphere (it isn't, but close enough!)
    """
    lon1, lat1 = origin
    lon2, lat2 = destination
    radius = 6371 # assumed radius of earth in km

    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d

def calc_zip(row, zip_dict):
    try:
        x = float(row.LON)
        y = float(row.LAT)
    except:
        return 999999

    if row.POSTCODE is None:
        low_dist = 1e5
        zc = 999999
        for k,v in zip_dict.items():
            dist = distance(v['coordinates'],(x,y))
            if dist < low_dist:
                zc = k
                low_dist = dist
        return zc
            
    else:
        return row.POSTCODE
        
def udf_zip(zip_dict):
    return udf(lambda l: calc_zip(l, zip_dict))

def run():
    # define state list
    #statesCap = ["LA", "AZ", "AR", "CO", "CT", "DC", "DE", 
                 #"FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY","LA"   Had to fix ascii encode halfway through 
#    statesCap = ["CA", "AL", "AK", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", 
#                 "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", 
#                 "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", 
#                 "VT", "VA", "WA", "WV", "WI", "WY"] 
    statesCap = ["CA"]
    states = [x.lower() for x in statesCap]
    orig_zip_codes = pd.read_csv('/home/wce/clsadmin/data/free-zipcode-database.csv')
    for state in states:
        pathway = '/states_missingness/%s/*' %state
        df = sc.read.csv(pathway, header=True, schema = StructType([StructField("LON",DoubleType()),StructField("LAT",DoubleType()),StructField("Number",IntegerType()),StructField("Street",StringType()),StructField("Unit",IntegerType()),StructField("City",StringType()),StructField("District",StringType()),StructField("POSTCODE",StringType()),StructField("ID",IntegerType()),StructField("Hash",StringType()),StructField("State", StringType()),StructField("null_count",IntegerType())])).persist()
        df = df.rdd.repartition(256).toDF(sampleRatio=1.0)

        zip_codes = orig_zip_codes[orig_zip_codes['State']==state.upper()]
        zip_codes = zip_codes[['Zipcode','State','Long','Lat']]
        zip_codes = zip_codes.drop_duplicates()

        zip_dict = {}
        for index,row in zip_codes.iterrows():
            zip_dict[row['Zipcode']] = {'coordinates':(float(row['Long']),float(row['Lat'])),'state':row['State']}
    
        output_path = '/states_with_zip/%s' %state
        print(output_path)
        final_df = df.withColumn("infer_zip", udf_zip(zip_dict)(struct(['LON','LAT','POSTCODE']))).persist()
        final_df = final_df.filter(final_df.LON.isNotNull()).filter(final_df.LAT.isNotNull()).persist()
        final_df = final_df.fillna('')
        final_df = final_df.withColumn("District", final_df.District.cast("string"))
        final_df = final_df.withColumn("ID", final_df.ID.cast("string"))
        final_df = final_df.withColumn("Number", final_df.Number.cast("int"))
        final_df = final_df.withColumn("Street", final_df.Street.cast("string"))
        final_df = final_df.withColumn("Unit", final_df.Unit.cast("int"))
        final_df = final_df.withColumn("City", final_df.City.cast("string"))
        final_df = final_df.withColumn("POSTCODE", final_df.POSTCODE.cast("string"))
#       print(df.schema)
#        final_df.show(100)
        final_df.write.csv(output_path, header=True, nullValue='')
    return


if __name__ == "__main__":
    sc = SparkSession.builder.appName('state_zips').getOrCreate()
    s_context = sc.sparkContext
    run()
    print("run succesful")
