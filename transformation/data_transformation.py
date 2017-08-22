#!/usr/bin/env python
'''
    File name         : data_transformation.py
    File Description  : Transform data read from S3 firehose and run K-Means clustering
    Notes             : Run this program "sudo spark-submit data_transformation.py &" 
                        in spark master (EMR) node.
    Author            : Srini Ananthakrishnan
    Date created      : 03/09/2017
    Date last modified: 03/09/2017
    Python Version    : 2.7
'''

# import packages
import yaml
import psycopg2
import psycopg2.extras
import pandas as pd

from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark.sql.types import *
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


# Global Constant
APP_NAME = "Uber Tweets"

"""
ReadFaker Class and insert are forked from:
https://gist.github.com/gerigk/2149399
"""
class ReadFaker:
    """
    This could be extended to include the index column optionally. Right now the index
    is not inserted
    """
    def __init__(self, data):
        #self.iter = data.iterrows()
        self.iter = data.itertuples()

    def readline(self, size=None):
        try:
            line = [element for element in self.iter.next()[1:]]
        except StopIteration:
            return ''
        else:

            return ("\t".join(['%s'] * len(line)) + '\n') % tuple(line)

    read = readline


def get_conn_string():
    """Function AWS RDS get connection string with credentials
    Args: 
    	None
    Return:
        RDS connection string
    """
    
    # Load RDS credentials
    rds_credentials = yaml.load(open(os.path.expanduser('~/.rds_api_cred.yml')))

    # Populate RDS credentials
    DB_NAME = rds_credentials['rds']['name'] 
    DB_USER = rds_credentials['rds']['user'] 
    DB_HOST = rds_credentials['rds']['host'] 
    DB_PASSWORD  = rds_credentials['rds']['password'] 
    DB_PORT = rds_credentials['rds']['port'] 

    conn_string = "dbname='" + DB_NAME + "' user='" + DB_USER + "' host='" + DB_HOST + "' port='" + DB_PORT + "' password='" + DB_PASSWORD + "'" 
    # print the connection string we will use to connect
    print("Connecting to database\n ->%s" % (conn_string))
    return conn_string
    
    
def insert(df, table, con=None, columns = None):
    """Function insert dataframe row by row to postgress DB using psycopg2
    Args: 
    	df : Dataframe to insert
    	table : table to copy in postgress DB
    Return:
        inserted_rows: Number of rows inserted
    """
    
    conn_string = get_conn_string()
    time1 = datetime.now()
    close_con = False
    if not con:
        try:
            con = psycopg2.connect(conn_string)
            # con = dbLoader.getCon()   ###dbLoader returns a connection with my settings
            close_con = True
        except psycopg2.Error, e:
            print e.pgerror
            print e.pgcode
            return "failed"
    inserted_rows = df.shape[0]
    data = ReadFaker(df)
    
    try:
        curs = con.cursor()
        print 'inserting %s entries into %s ...' % (inserted_rows, table)
        if columns is not None:
            curs.copy_from(data, table, null='nan', columns=[col for col in columns])
        else:
            curs.copy_from(data, table, null='nan')
        con.commit()
        curs.close()
        if close_con:
            con.close()
    except psycopg2.Error, e:
        print e.pgerror
        print e.pgcode
        con.rollback()
        if close_con:
            con.close()
        return "failed"

    time2 = datetime.now()
    print time2 - time1
    print 'Done inserting....!!'
    return inserted_rows


def main(sc, spark):
    """Function main transforms S3 data, run KMeans Clustering and insert to postgress DB
    Args: 
    	spark : spark context
    Return:
        None
    """

    # Read ubertweets json from AWS S3 bucket
    sparkDf = spark.read.json("s3a://ubertweets/2017/*/*/*/*")

    # Select fields for processing
    sparkDf = sparkDf.selectExpr('timestamp','text','uberX','uberXL','ASSIST','BLACK','POOL','SELECT','SUV','WAV','geo.coordinates')

    # Drop any rows with None data
    sparkDf = sparkDf.dropna()

    # Get geo-location coordinates of tweets
    coordinates = sparkDf.select("coordinates")

    # convert coordinates list to tuples and transform as RDD
    coordinatesRDD = coordinates.rdd.map(lambda x: (x[0][0], x[0][1]))

    # Cache the RDD
    coordinatesRDD.cache()

    # Build the KMeans Clustering model
    clusters = KMeans.train(coordinatesRDD, 8, maxIterations=10, initializationMode="random")

    # Save the cluster centers (centroids) as pandas dataframe
    centeroidsDF = pd.DataFrame(clusters.clusterCenters, columns=['latitude','longitude'])

    # Insert the centroids data as table into postgressSQL (AWS RDS)
    insert(centeroidsDF,"centeroids")

    # Predict cluster
    cluster_list = clusters.predict(coordinatesRDD).collect()
    
    # Transform spark DF into pandas DF. 
    # WARNING: Only do this if the pandas DF data can fit in memory
    sparkPDF = sparkDf.toPandas()
    
    # Create new column with predicted cluster information
    # WARNING: Only do this if the pandas DF data can fit in memory
    sparkPDF['cluster']=cluster_list 
    
    # Replace tweets text which contains \n with empty space.
    # insert to Postgress database will fail if \n is present in tweet text
    sparkPDF['text'].replace(regex=True,inplace=True,to_replace=r'\n',value=r'')


    # Insert transformed (3NF) into postgress database
    insert(sparkPDF,"sparkdf")



if __name__ == '__main__':
    """Main Function. Connect and record streams
    Args:
        None
    Return:
        None
    """

    # Create Spark Context
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Create Spark Session
    spark = SparkSession.builder \
            .master("local") \
            .appName(APP_NAME) \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
            
    # invoke main routine
    main(sc, spark)
    
