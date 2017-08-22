#!/usr/bin/env python
'''
    File name         : data_injection.py
    File Description  : Uber and Twitter data are mergred and injected to AWS S3 firehose.
    Notes             : Run this program "sudo python data_injection.py &" in EC2 instance
    Author            : Srini Ananthakrishnan
    Date created      : 03/09/2017
    Date last modified: 03/09/2017
    Python Version    : 2.7
'''

# import packages
import os
import sys
import time
import yaml
import json
import boto3
import pprint

from uber_rides.session import Session
from uber_rides.client import UberRidesClient
from twitter import *
from datetime import datetime


def connect_to_uber():
    """Function connects to Uber Rides server with credentials
    Args: 
    	None
    Return:
        Uber client session
    """

    # Load uber credentials
    uber_credentials = yaml.load(open(os.path.expanduser('~/.uber_api_cred.yml')))
    # CLIENT_ID = uber_credentials['uber']['client_id']
    SERVER_TOKEN = uber_credentials['uber']['server_token']
    # CLIENT_SECRET = uber_credentials['uber']['client_secret']
    # APP_NAME = uber_credentials['uber']['app_name']

    session = Session(server_token=SERVER_TOKEN)
    uber_client = UberRidesClient(session)
    print("Connection to uber sucessfull...\n")
    return uber_client


def connect_to_twitter_search():
    """Function connects to Twitter server with credentials
    Args: 
    	None
    Return:
        Twitter client session
    """

    # Load twitter credentials
    twitter_credentials = yaml.load(open(os.path.expanduser('~/.twitter_api_cred.yml')))

    CONSUMER_SECRET = twitter_credentials['twitter']['consumer_secret']
    CONSUMER_KEY = twitter_credentials['twitter']['consumer_key']
    TOKEN = twitter_credentials['twitter']['token']
    TOKEN_SECRET = twitter_credentials['twitter']['token_secret']

    twitter = Twitter(auth=OAuth(TOKEN, TOKEN_SECRET, CONSUMER_KEY, CONSUMER_SECRET))
    print("Connection to twitter sucessfull...\n")
    return twitter


def write_to_s3firehose(tweet):
    """Function puts input data to AWS S3 Firehose using boto3
    Args:
        tweet   : Uber and Twitter Merged Json Stream
    Return:
        None
    """

    # Connect to S3 firehose using boto3
    try:
        s3client = boto3.client('firehose', region_name="us-east-1")
        # print("Connection to S3 firehose sucessfull...\n")
    except Exception as e:
        print("ERROR: Unable to connect to S3 firehose !!")
        return

    try:
        # write/record each tweet and terminate with '\$0$'
        resp = s3client.put_record(DeliveryStreamName='ubertweetsS3Firehose',
                        Record={'Data': (tweet)+'\$0$'})
    except Exception as e:
        print("ERROR: Recording tweets into S3 Firehose Database\n")
        print(e)


def merge_two_dicts(x, y):
    """Function merge two dictionaries
    Args:
        x, y  : Dictionaries to merge
    Return:
        Merged dictionary
    """
    
    z = x.copy()
    z.update(y)
    return z


def json_serial(obj):
    """Function serializes objects that are not serializable by default json code
    Args:
        x, y  : Dictionaries to merge
    Return:
        Merged dictionary
    """

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type not serializable")


def merge_data_and_record(twitter, uber_client):
    """Function merge Uber and Twitter stream as Json object. Record the result to firehose
    Args:
        twitter  : Twitter session instance
        uber_client : Uber session instance
    Return:
        None
    """

    # Make San Francisco as default location. Parameterize in the future.
    latitude = 37.773972     # geographical centre of search
    longitude = -122.431297  # geographical centre of search
    max_range = 85           # search range in kilometres (approx 50miles)

    last_id = -1L

    print("Merge and Record to S3 firehose ")
    while True:
        try:
            # ---------------------------------------------------------
            # perform a search based on latitude and longitude
            # twitter API docs:
            # https://dev.twitter.com/rest/reference/get/search/tweets
            # ---------------------------------------------------------
            query = twitter.search.tweets(q = "", geocode = "%f,%f,%dkm" % (latitude, longitude, max_range),
                                          count = 100, since_id = last_id)

            for result in query["statuses"]:
                # -----------------------------------------------------
                # only process a result if it has a geolocation
                # -----------------------------------------------------
                if result.get("geo"):
                    uber_price = {}
                    geo = result.get("geo")
                    if result.get('id'):
                        if result.get('user'):
                            user = result.get('user')
                            if user.get('screen_name'):
                                tweetUser = user.get('screen_name')
                            else:
                                continue
                        else:
                            continue

                        if result.get('text'):
                            tweetText = result.get('text')
                            tweetText = tweetText.encode('ascii', 'replace')
                        else:
                            continue

                        __time = datetime.now()

                        if geo.get('coordinates'):
                            coordinates = geo.get('coordinates')
                            latitude = coordinates[0]
                            longitude = coordinates[1]

                            # Get estimates for time and price
                            response = uber_client.get_price_estimates(
                                start_latitude=latitude,
                                start_longitude=longitude,
                                end_latitude=latitude,
                                end_longitude=longitude,
                                seat_count=2
                            )
                            result['timestamp'] = __time

                            # Get UBER product name and surge multiplier
                            uber_estimate = response.json.get('prices')
                            for line in uber_estimate:
                                if line.get('display_name'):
                                    product_name = line.get('display_name')
                                    uber_price.setdefault(product_name, [])
                                    low_estimate = line.get('low_estimate')
                                    high_estimate = line.get('high_estimate')
                                    uber_price[product_name] = [low_estimate, high_estimate]
                                else:  # end of display_name
                                    continue
                        else:  # end of geo
                            continue
                    else:  # end of id
                        continue
                    # ------------------------------------------------
                    # now merge both uber and tweet data into json str
                    # ------------------------------------------------
                    mdict = merge_two_dicts(result, uber_price)
                    mdict_json = json.dumps(mdict, default=json_serial)

                    # write the json ubertweets to S3 firehose
                    ret = write_to_s3firehose(mdict_json)
                    if ret == -1:
                        return ret
                    # pprint.pprint(mdict_json)
                    # print("###################################")
                else:  # end of geo
                    continue
                last_id = result.get('id') + 1
        except Exception as e:
            # print("#")
            # print(e)
            time.sleep(61*15)  # tweet request once in 15 mins
            continue

if __name__ == '__main__':
    """Main Function. Connect and record streams
    Args:
        None
    Return:
        None
    """
    
    
    while True:
        # Connect to twitter stream
        twitter = connect_to_twitter_search()

        # Connect to uber stream
        uber_client = connect_to_uber()

        # Merge (twitter + uber) data and record to firehose
        merge_data_and_record(twitter, uber_client)
