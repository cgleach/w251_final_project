# Packages needed for notebook
import pandas as pd
import math
from pyspark.sql.functions import udf, struct
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


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
        return None

    if row.POSTCODE is None:
        low_dist = 1e5
        zc = None
        for k,v in zip_dict.items():
            dist = distance(v['coordinates'],(x,y))
            if dist < low_dist:
                zc = k
                low_dist = dist
        return zc
            
    else:
        return row.POSTCODE[:5]
        
def udf_zip(zip_dict):
    return udf(lambda l: calc_zip(l, zip_dict))

def run():
    # define state list
    statesCap = ["AL", "AK", "AZ", "AR", "CO", "CT", "DC", "DE", 
                 "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", 
                 "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", 
                 "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", 
                 "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", 
                 "VT", "VA", "WA", "WV", "WI", "WY"] #CA excluded as already ran
    states = [x.lower() for x in statesCap]
    orig_zip_codes = pd.read_csv('/home/wce/clsadmin/data/free-zipcode-database.csv')
    for state in states:
        pathway = '/states_missingness/%s/*' %state
        df = sc.read.csv(pathway, header=True).persist()
        df = df.rdd.repartition(256).toDF(sampleRatio=.2)

        zip_codes = orig_zip_codes[orig_zip_codes['State']==state.upper()]
        zip_codes = zip_codes[['Zipcode','State','Long','Lat']]
        zip_codes = zip_codes.drop_duplicates()

        zip_dict = {}
        for index,row in zip_codes.iterrows():
            zip_dict[row['Zipcode']] = {'coordinates':(float(row['Long']),float(row['Lat'])),'state':row['State']}
    
        output_path = '/states_with_zip/%s' %state
        print(output_path)
        final_df = df.withColumn("infer_zip", udf_zip(zip_dict)(struct(['LON','LAT','POSTCODE']))).persist()
        final_df.write.csv(output_path, header=True)
    return


if __name__ == "__main__":
    sc = SparkSession.builder.appName('state_zips').getOrCreate()
    s_context = sc.sparkContext
    run()
    print("run succesful")