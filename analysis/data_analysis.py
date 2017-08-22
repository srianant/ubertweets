#!/usr/bin/env python
'''
    File name         : data_analysis.py
    File Description  : Analyze Uber Tweets data store in postgressSQL DB.
    Notes             : Run this program "sudo python data_analysis.py &" in EC2 instance
    Author            : Srini Ananthakrishnan
    Date created      : 03/09/2017
    Date last modified: 03/09/2017
    Python Version    : 2.7
'''


# import packages
import os
import yaml
import pandas as pd
import psycopg2
import psycopg2.extras
import pandas as pd
import json
import re
import sys
import plotly
import plotly.plotly as py
from plotly.graph_objs import *
import plotly.graph_objs as go
import plotly.tools as tls

from datetime import datetime
from geopy.geocoders import Nominatim



def get_rds_cursor():
    """Function connects to AWS RDS Postgress DB
    Args: 
    	None
    Return:
        DB connection session and cursor object
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

    # Connnect to RDS database
    try:
        # get a connection, if a connect cannot be made an exception will be raised here
        conn = psycopg2.connect(conn_string)
        print("Connection successful!!...%s",conn)
    except:
        print("ERROR: Unable to connect to postgres RDS database !!!")

    # conn.cursor will return a cursor object, you can use this query to perform queries
    # note that in this example we pass a cursor_factory argument that will
    # dictionary cursor so COLUMNS will be returned as a dictionary so we
    # can access columns by their name instead of index.
    cur = conn.cursor()
    return cur, conn


def get_postgress_tables(cur, conn):
    """Function get postgress SQL tables
    Args: 
    	DB connection session and cursor object
    Return:
        ubertweets sparkdf and Centroids tables
    """

    cur.execute("DROP VIEW IF EXISTS centeroids_view")
    cur.execute("""CREATE VIEW centeroids_view
                AS
                SELECT *
                FROM centeroids""")

    conn.commit()
    # get results
    cur.execute("SELECT * FROM centeroids_view;")
    results_clusters = cur.fetchall()

    cur.execute("DROP VIEW IF EXISTS sparkdf_view")
    cur.execute("""CREATE VIEW sparkdf_view
                AS
                SELECT *
                FROM sparkdf""")

    conn.commit()
    # get results
    cur.execute("SELECT * FROM sparkdf_view;")
    results_sparkdf = cur.fetchall()

    conn.close()
    return results_clusters, results_sparkdf


def get_location_names(centroids):
    """Function get geo-location names
    Args: 
    	KMeans cluster centroids
    Return:
        Location names based on latitude and longitude
    """

    geolocator = Nominatim()
    loc_name = []
    for loc in centroids.values:
        str1 = str(loc[0]),str(loc[1])
        location = geolocator.reverse(str1)
        loc_name.append(str(location.raw.get('address').get('road')))
    return loc_name


def get_uber_products_cluster_wise(sparkdf):
    """Function get uber products and cluster info
    Args: 
    	sparkdf: 3NF data frame stored in DB
    Return:
        Uber products dataframe with timestamp and cluster info
    """

    uberXdf = pd.DataFrame([x for x in sparkdf.uberX])
    uberXdf.columns = ['low','high']
    uberXdf['timestamp'] = sparkdf.timestamp
    uberXdf['cluster'] = sparkdf.cluster

    uberXLdf = pd.DataFrame([x for x in sparkdf.uberXL])
    uberXLdf.columns = ['low','high']
    uberXLdf['timestamp'] = sparkdf.timestamp
    uberXLdf['cluster'] = sparkdf.cluster
    
    BLACKdf = pd.DataFrame([x for x in sparkdf.BLACK])
    BLACKdf.columns = ['low','high']
    BLACKdf['timestamp'] = sparkdf.timestamp
    BLACKdf['cluster'] = sparkdf.cluster

    ASSISTdf = pd.DataFrame([x for x in sparkdf.ASSIST])
    ASSISTdf.columns = ['low','high']
    ASSISTdf['timestamp'] = sparkdf.timestamp
    ASSISTdf['cluster'] = sparkdf.cluster

    SELECTdf = pd.DataFrame([x for x in sparkdf.SELECT])
    SELECTdf.columns = ['low','high']
    SELECTdf['timestamp'] = sparkdf.timestamp
    SELECTdf['cluster'] = sparkdf.cluster

    POOLdf = pd.DataFrame([x for x in sparkdf.POOL])
    POOLdf.columns = ['low','high']
    POOLdf['timestamp'] = sparkdf.timestamp
    POOLdf['cluster'] = sparkdf.cluster

    SUVdf = pd.DataFrame([x for x in sparkdf.SUV])
    SUVdf.columns = ['low','high']
    SUVdf['timestamp'] = sparkdf.timestamp
    SUVdf['cluster'] = sparkdf.cluster
    
    WAVdf = pd.DataFrame([x for x in sparkdf.WAV])
    WAVdf.columns = ['low','high']
    WAVdf['timestamp'] = sparkdf.timestamp
    WAVdf['cluster'] = sparkdf.cluster
    
    return uberXdf, uberXLdf, BLACKdf, ASSISTdf, SELECTdf, POOLdf, SUVdf, WAVdf
    

def plot_price_surge_vs_time(uberXdf, uberXLdf, BLACKdf, ASSISTdf, SELECTdf, POOLdf, SUVdf, WAVdf):
    """Function plot using plotly price surge vs time
    Args: 
    	Uber products dataframe with timestamp and cluster info
    Return:
        None
    """

    uberX = go.Scatter(
        x = uberXdf.timestamp,
        y = uberXdf.high,
        mode = 'markers',
        name = 'uberX'
    )

    uberXL = go.Scatter(
        x = uberXLdf.timestamp,
        y = uberXLdf.high,
        mode = 'markers',
        name = 'uberXL'
    )

    BLACK = go.Scatter(
        x = BLACKdf.timestamp,
        y = BLACKdf.high,
        mode = 'markers',
        name = 'BLACK'
    )

    ASSIST = go.Scatter(
        x = ASSISTdf.timestamp,
        y = ASSISTdf.high,
        mode = 'markers',
        name = 'ASSIST'
    )

    POOL = go.Scatter(
        x = POOLdf.timestamp,
        y = POOLdf.high,
        mode = 'markers',
        name = 'POOL'
    )

    WAV = go.Scatter(
        x = WAVdf.timestamp,
        y = WAVdf.high,
        mode = 'markers',
        name = 'WAV'
    )

    SUV = go.Scatter(
        x = SUVdf.timestamp,
        y = SUVdf.high,
        mode = 'markers',
        name = 'SUV'
    )

    SELECT = go.Scatter(
        x = SELECTdf.timestamp,
        y = SELECTdf.high,
        mode = 'markers',
        name = 'SELECT'
    )
    
    data = [uberX, uberXL, BLACK, SELECT, POOL, SUV, WAV, ASSIST]
    layout = go.Layout(
        title='Uber Products Price Surge Vs Time',
        xaxis=dict(
            title='Time',
            titlefont=dict(
                family='Courier New Bold, monospace',
                size=18,
                color='#913d3d'
            )
        ),
        yaxis=dict(
            title='Price',
            titlefont=dict(
                family='Courier New Bold, monospace',
                size=18,
                color='#913d3d'
            )
        )
    )
    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename='uber_data')
    
    # tls.get_embed('https://plot.ly/~srianant/0')


def plot_cluster_wise_price_surge(uberXdf, uberXLdf, BLACKdf, ASSISTdf, SELECTdf, POOLdf, SUVdf, WAVdf):
    """Function plot using plotly cluster wise price surge
    Args: 
    	Uber products dataframe with timestamp and cluster info
    Return:
        None
    """

    uberX = go.Scatter(
        x = uberXdf.cluster,
        y = uberXdf.high,
        mode = 'markers',
        name = 'uberX'
    )

    uberXL = go.Scatter(
        x = uberXLdf.cluster,
        y = uberXLdf.high,
        mode = 'markers',
        name = 'uberXL'
    )

    BLACK = go.Scatter(
        x = BLACKdf.cluster,
        y = BLACKdf.high,
        mode = 'markers',
        name = 'BLACK'
    )

    ASSIST = go.Scatter(
        x = ASSISTdf.cluster,
        y = ASSISTdf.high,
        mode = 'markers',
        name = 'ASSIST'
    )

    POOL = go.Scatter(
        x = POOLdf.cluster,
        y = POOLdf.high,
        mode = 'markers',
        name = 'POOL'
    )

    WAV = go.Scatter(
        x = WAVdf.cluster,
        y = WAVdf.high,
        mode = 'markers',
        name = 'WAV'
    )

    SUV = go.Scatter(
        x = SUVdf.cluster,
        y = SUVdf.high,
        mode = 'markers',
        name = 'SUV'
    )

    SELECT = go.Scatter(
        x = SELECTdf.cluster,
        y = SELECTdf.high,
        mode = 'markers',
        name = 'SELECT'
    )

    data = [uberX, uberXL, BLACK, SELECT, POOL, SUV, WAV, ASSIST]
    layout = go.Layout(
        title='Cluster Wise Uber Products Price Surge',
        xaxis=dict(
            title='Clusters Centroid',
            titlefont=dict(
                family='Courier New Bold, monospace',
                size=18,
                color='#913d3d'
            )
        ),
        yaxis=dict(
            title='Price in Dollars',
            titlefont=dict(
                family='Courier New Bold, monospace',
                size=18,
                color='#913d3d'
            )
        )
    )
    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename='uber_price_surge_per_cluter')
    

def plot_mapbox_of_centeroids(centeroids):
    """Function plot using plotly mapbox of cluster centeroids
    Args: 
    	KMeans cluster centeroids
    Return:
        None
    """
    
    mapbox_access_token = 'pk.eyJ1IjoiY2hlbHNlYXBsb3RseSIsImEiOiJjaXFqeXVzdDkwMHFrZnRtOGtlMGtwcGs4In0.SLidkdBMEap9POJGIe1eGw'

    loc_name = get_location_names(centroids)
    
    data = Data([
        Scattermapbox(
            lat=map(str, centroids.latitude.values),
            lon=map(str, centroids.longitude.values),
            mode='markers',
            marker=Marker(
                size=15
            ),
            text=loc_name,
        )
    ])
    layout = Layout(
        autosize=True,
        title='Cluster Centroids',
        hovermode='closest',
        mapbox=dict(
            accesstoken=mapbox_access_token,
            bearing=0,
            center=dict(
                lat=37.773972,
                lon=-122.431297
            ),
            pitch=0,
            zoom=10
        ),
    )
    fig = dict(data=data, layout=layout)
    py.plot(fig, filename='Multiple Mapbox', validate=False)
    


def main():
    """Function analysis ubertweets data stored in postgress DB and plots
    Args: 
    	None
    Return:
        None
    """
 
    plotly.tools.set_credentials_file(username='srianant', api_key='ZmJDI5MfLeeLHoLnCftf')
    
    cur, conn = get_rds_cursor()
    
    results_clusters, results_sparkdf = get_postgress_tables(cur, conn)

    centroids = pd.DataFrame(results_clusters,columns=['latitude','longitude'])

    sparkdf = pd.DataFrame(results_sparkdf,columns=['timestamp', 'text', 'uberX', 'ASSIST', 'uberXL', 'BLACK', 
                                                'POOL', 'SELECT', 'SUV', 'WAV', 'coordinates', 'cluster'])
    
    uberXdf, uberXLdf, BLACKdf, ASSISTdf, SELECTdf, POOLdf, SUVdf, WAVdf = 
            get_uber_products_cluster_wise(sparkdf)

    plot_price_surge_vs_time(uberXdf, uberXLdf, BLACKdf, ASSISTdf, SELECTdf, 
                             POOLdf, SUVdf, WAVdf)
                             
    plot_cluster_wise_price_surge(uberXdf, uberXLdf, BLACKdf, ASSISTdf, SELECTdf, 
                                  POOLdf, SUVdf, WAVdf):

    plot_mapbox_of_centeroids(centeroids)


if __name__ == '__main__':
    """Main Function.
    Args:
        None
    Return:
        None
    """
    main()
    
    
