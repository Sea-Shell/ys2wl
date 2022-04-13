import argparse
import json
import jq
import logging
import os
import math
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

log = logging.getLogger(__name__)

credentials = None
scopes = ['https://www.googleapis.com/auth/youtube.readonly']
loggFormat = "%(asctime)5s %(levelname)10s %(message)s (%(name)s)"

def authenticate(credentials_file=None, pickle_credentials=None, scopes=None):
    if os.path.exists(pickle_credentials):
        log.info("Loading credentials from %s" % pickle_credentials)
        with open(pickle_credentials, "rb") as token:
            credentials = pickle.load(token)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            log.info("Refreshing access token")
            credentials.refresh(Request())
        else:
            log.info("Fetching new tokens")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes=scopes)
            flow.run_local_server(port=8080, prompt='consent')
            credentials = flow.credentials

            with open(pickle_credentials, "wb") as f:
                log.info("Saving credentials to pickle file")
                pickle.dump(credentials, f)
    return credentials

def get_subscriptions(credentials=None, nextPage=None):
    subscriptions_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("Getting all your subscription")
        subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical")
    else:
        log.debug("get_subscriptions() invoked with nextPageToken %s!" % nextPage)
        subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical", pageToken=nextPage)

    subscriptions_response = subscriptions_request.execute()
    log.debug("subscriptions_response is of type %s and items count %s" % (type(subscriptions_response),len(subscriptions_response["items"])))
    if nextPage is None:
        log.info("Total amount of subscriptions: %s (from youtube API)" % subscriptions_response["pageInfo"]["totalResults"])
    sub_dict = subscriptions_response
    
    if "nextPageToken" in subscriptions_response:
        log.info("nextPageToken detected!")
        nextPageToken = subscriptions_response.get("nextPageToken")
        subscriptions_response_nextpage = get_subscriptions(credentials, nextPage=nextPageToken)
        log.debug("subscriptions_response_nextpage is of type %s and items count %s" % (type(subscriptions_response_nextpage),len(subscriptions_response_nextpage["items"])))
    else:
        subscriptions_response_nextpage = {}

    sub_dict = subscriptions_response_nextpage | sub_dict

    #sub_list = jq.all('[ .items[] | { "title": .snippet.title, "id": .snippet.resourceId.channelId } ]', json.dumps(subscriptions_response))

    return subscriptions_response


def get_subscription_activity(credentials=None, channel=None, nextPage=None):
    activity_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("Getting activity for channelId: %s" % channel)
        activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, channelId=channel)
    else:
        log.debug("Getting activity for channel %s invoked with nextPageToken %s!" % (channel, nextPage))
        activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, channelId=channel, pageToken=nextPage)
    
    activity_response = activity_request.execute()
    if nextPage is None:
        log.info("Total amount of activity: %s (from youtube API)" % activity_response["pageInfo"]["totalResults"])
    act_array = activity_response["items"]

    if "nextPageToken" in activity_response:
        nextPageToken = activity_response.get("nextPageToken")
        activity_response_nextpage = get_subscription_activity(credentials, channel=channel, nextPage=nextPageToken)
        act_array = act_array + activity_response_nextpage


    return act_array

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pickle-file', default='credentials.pickle', help='File to store access token once authenticated')
    parser.add_argument('--credentials-file', default='client_secret.json', help='JSON file with credentials to oAuth2 account')
    parser.add_argument('--max-results', default='50', type=int, help='JSON file with credentials to oAuth2 account')
    parser.add_argument('--log-level', default='warning', help='Set loglevel. debug,info,warning or error')
        
    args = parser.parse_args()

    if args.log_level == "debug":
        loglevel = logging.DEBUG
    elif args.log_level == "info":
        loglevel = logging.INFO
    elif args.log_level == "warning":
        loglevel = logging.WARNING
    elif args.log_level == "error":
        loglevel = logging.ERROR

    logging.basicConfig(format=loggFormat, level=loglevel)


    credentials = authenticate(args.credentials_file, args.pickle_file, scopes=scopes)
    
    subscriptions = get_subscriptions(credentials=credentials)
    #print(json.dumps(subscriptions))
    log.info("Subscriptions in list count: %s" % len(subscriptions["items"]))

    #print(get_subscription_activity(credentials=credentials, channel="UCn7w-zvOSD3ADT-na6uOTZQ"))
    
    #sprint(subscriptions_response2)
    #print(subscriptions_response.get("nextPageToken"))
    

if __name__ == '__main__':
    main()
