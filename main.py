# Using Python 3.7.7
import requests
import os
from datetime import datetime
from time import sleep
import logging

from pymongo import MongoClient
client = MongoClient(os.environ.get('MONGODB_URI'))

mydb = client["fox_tweet_scraping"]
tweets_col = mydb["tweets"]


def paginate(json_response):
    if "next_token" in json_response["meta"].keys():
        pagination_token = json_response["meta"]["next_token"]
        stop = False
    else:
        pagination_token = None
        stop = True
    return stop, pagination_token


# Function to get each news source's timeline of tweets,
# including media and context annotations
# Paginate through results and write to csv
def get_user_tweets(headers, user_id, start_time):
    logging.info('Getting users Tweet-level info')
    # create structure of Tweet-level csv

    logging.info(user_id)

    pagination_token = None
    stop = False
    while not stop:
        # get max number of Tweets
        try:
            response = requests.get(
                "https://api.twitter.com/2/tweets/search/all",
                headers=headers,
                params={
                    'query': 'from:{}'.format(user_id),
                    'tweet.fields': "created_at,text,conversation_id,in_reply_to_user_id,public_metrics,referenced_tweets,lang,author_id,context_annotations,attachments",
                    "expansions": "attachments.media_keys",
                    "media.fields": "duration_ms,preview_image_url,public_metrics",
                    "max_results": 100,
                    "next_token": pagination_token,
                    "start_time": start_time,
                    # "end_time": end_time
                }
            )

            # print(response.status_code)
            if response.status_code != 200:
                raise Exception(response.status_code, response.text)

            json_response = response.json()

            sleep(3)  # 300 requests to this endpoint / 15 minutes
        except Exception as e:
            logging.exception("get_user_tweets() error: %s", e)
            break

        # break if no more tweets
        if json_response["meta"]["result_count"] == 0:
            break

        print('Inserting batch into MongoDB...')

        def inject_timestamp(doc):
            doc["_docCreatedAt"] = datetime.utcnow()
            return doc

        all_data = map(inject_timestamp, json_response["data"])
        tweets_col.insert_many(all_data, ordered=False)

        # paginate
        stop, pagination_token = paginate(json_response)


def main():
    print('Start time: {}'.format(datetime.now()))

    # Setup API authentication
    bearer_token = os.environ.get("BEARER_TOKEN")  # auth()
    headers = {"Authorization": "Bearer {}".format(bearer_token)}  # create_headers(bearer_token)

    fox_news_id = "1367531"

    # 2. Get each news source's timeline of Tweets for a given time span
    get_user_tweets(headers, fox_news_id, start_time="2022-02-01T00:00:00Z")

    print('End time: {}'.format(datetime.now()))


main()

