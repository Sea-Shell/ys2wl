import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import jq
import logging
import math
import os
import operator
import pickle
import sqlite3
import time
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

log = logging.getLogger(__name__)
now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
oneyearback = datetime.now() - relativedelta(years=1)
oneyearback = oneyearback.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
scopes = [
    'https://www.googleapis.com/auth/youtubepartner',
    'https://www.googleapis.com/auth/youtube.force-ssl',
    'https://www.googleapis.com/auth/youtube', 
    'https://www.googleapis.com/auth/youtube.readonly'
    ]
loggFormat = "%(asctime)5s %(levelname)10s %(message)s (%(name)s)"
dateFormat = ""
last_run_file = "last-run.file"

def get_last_run(last_run_file=None, publishedAfter=None):
    if (last_run_file is not None and os.path.exists(last_run_file)) and publishedAfter is None:
        log.info("%s is set and exists!" % last_run_file)
        with open(last_run_file, "rb") as last_run_line:
            log.info("Read content of %s" % last_run_file)
            last_run = parser.pars(last_run_line)
    elif publishedAfter is not None:
        log.info("publishedAfter is set.")
        last_run = publishedAfter
    else:
        log.info("Settings to default, 1 year in the past")
        last_run = oneyearback
    
    return last_run

def authenticate(credentials_file=None, pickle_credentials=None, scopes=None):
    credentials = None

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
    log.debug("subscriptions_response is of type %s and items count %s" % (type(subscriptions_response),len(subscriptions_response)))
    sub_dict = subscriptions_response["items"]
    log.debug("sub_dict is of type %s and items count %s" % (type(sub_dict),len(sub_dict)))
    if nextPage is None:
        log.info("Total amount of subscriptions: %s (from youtube API)" % subscriptions_response["pageInfo"]["totalResults"])
    
    if "nextPageToken" in subscriptions_response:
        log.info("nextPageToken detected!")
        nextPageToken = subscriptions_response.get("nextPageToken")
        subscriptions_response_nextpage = get_subscriptions(credentials=credentials, nextPage=nextPageToken)
        sub_dict_nextpage = subscriptions_response_nextpage
        log.debug("sub_dict_nextpage is of type %s and items count %s" % (type(sub_dict_nextpage),len(sub_dict_nextpage)))
        sub_dict = [*sub_dict, *sub_dict_nextpage]

    return sub_dict

def get_subscription_activity(credentials=None, channel=None, publishedAfter=None, nextPage=None):
    activity_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("Getting activity for channelId: %s" % channel)
        activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, publishedAfter=publishedAfter, uploadType="upload", channelId=channel)
    else:
        log.debug("Getting activity for channel %s invoked with nextPageToken %s!" % (channel, nextPage))
        activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, publishedAfter=publishedAfter, uploadType="upload", channelId=channel, pageToken=nextPage)
    
    activity_response = activity_request.execute()
    log.debug("activity_response is of type %s and items count %s" % (type(activity_response),len(activity_response)))
    act_array = activity_response["items"]
    log.debug("act_array is of type %s and items count %s" % (type(act_array),len(act_array)))
    if nextPage is None:
        log.info("Total amount of activity: %s (from youtube API)" % activity_response["pageInfo"]["totalResults"])

    if "nextPageToken" in activity_response:
        nextPageToken = activity_response.get("nextPageToken")
        activity_response_nextpage = get_subscription_activity(credentials=credentials, channel=channel, nextPage=nextPageToken)
        act_array = [*act_array, *activity_response_nextpage]


    return act_array

def get_channel_id(credentials=None):
    channel_youtube = build("youtube", "v3", credentials=credentials)
    channel_request = channel_youtube.channels().list(
        part="snippet,contentDetails",
        mine=True
    )
    channel_response = channel_request.execute()
    channel_list = channel_response["items"]

    channel_list = jq.all('.[] | { "title": .snippet.title, "id": .id }', channel_list)

    return channel_list

def get_user_playlists(credentials=None, channelId=None, nextPage=None):
    playlists_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("Getting all your playlists")
        playlists_request = playlists_youtube.playlists().list(part="snippet,contentDetails", channelId=channelId, maxResults=50)
    else:
        log.debug("get_subscriptions() invoked with nextPageToken %s!" % nextPage)
        playlists_request = playlists_youtube.subscriptions().list(part="snippet,contentDetails", channelId=channelId, maxResults=50, pageToken=nextPage)

    playlists_response = playlists_request.execute()
    log.debug("playlists_response is of type %s and items count %s" % (type(playlists_response),len(playlists_response)))
    plists_dict = playlists_response["items"]
    log.debug("plists_dict is of type %s and items count %s" % (type(plists_dict),len(plists_dict)))
    if nextPage is None:
        log.info("Total amount of playlists: %s (from youtube API)" % playlists_response["pageInfo"]["totalResults"])
    
    if "nextPageToken" in playlists_response:
        log.info("nextPageToken detected!")
        nextPageToken = playlists_response.get("nextPageToken")
        playlists_response_nextpage = get_user_playlists(credentials=credentials, channelId=channelId, nextPage=nextPageToken)
        plists_dict_nextpage = playlists_response_nextpage
        log.debug("plists_dict_nextpage is of type %s and items count %s" % (type(plists_dict_nextpage),len(plists_dict_nextpage)))
        plists_dict = [*plists_dict, *plists_dict_nextpage]

    return plists_dict

def get_playlist(credentials=None, channelId=None, playlistId=None, nextPage=None):
    playlist_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("Getting playlist %s" % playlistId)
        playlist_request = playlist_youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlistId, maxResults=50)
    else:
        log.debug("get_subscriptions() invoked with nextPageToken %s!" % nextPage)
        playlist_request = playlist_youtube.subscriptions().list(part="snippet,contentDetails", playlistId=playlistId, maxResults=50, pageToken=nextPage)

    playlist_response = playlist_request.execute()
    log.debug("playlist_response is of type %s and items count %s" % (type(playlist_response),len(playlist_response)))
    playlist_dict = playlist_response["items"]
    log.debug("playlist_dict is of type %s and items count %s" % (type(playlist_dict),len(playlist_dict)))
    if nextPage is None:
        log.info("Total amount items on playlist: %s (from youtube API)" % playlist_response["pageInfo"]["totalResults"])
    
    if "nextPageToken" in playlist_response:
        log.info("nextPageToken detected!")
        nextPageToken = playlist_response.get("nextPageToken")
        playlist_response_nextpage = get_user_playlists(credentials=credentials, channelId=channelId, nextPage=nextPageToken)
        playlist_dict_nextpage = playlist_response_nextpage
        log.debug("playlist_dict_nextpage is of type %s and items count %s" % (type(playlist_dict_nextpage),len(playlist_dict_nextpage)))
        playlist_dict = [*playlist_dict, *playlist_dict_nextpage]

    return playlist_dict

def add_to_playlist(credentials=None, channelId=None, playlistId=None, videoId=None):
    playlist_youtube = build("youtube", "v3", credentials=credentials)
    playlist_request = playlist_youtube.playlistItems().insert(
        part="snippet",
        body={
          "kind": "youtube#playlistItem",
          "snippet": {
            "playlistId": playlistId,
            "resourceId": {
              "kind": "youtube#video",
              "videoId": videoId
            },
            "channelId": channelId
          }
        }
    )
    playlist_response = playlist_request.execute()
    log.debug("Playlist Insert respons: {}".format(json.dumps(playlist_response, indent=4)))
    log.info("%s added to %s in position %s" % (videoId, playlistId, playlist_response["snippet"].get("position")))
    time.sleep(1)

    return playlist_response

def save_to_db(connection=None, videoId=None, timestamp=None, title=None, subscriptionId=None):
    sql = 'INSERT OR REPLACE INTO videos (videoId, timestamp, title, subscriptionId) VALUES(?, ?, ?, ?)'
    data = [(videoId, timestamp, title, subscriptionId)]
    with connection:
        connection.executemany(sql, data)
    log.info("Video %s (%s) from %s added to database" % (title, videoId, subscriptionId))
            
def get_from_db(connection=None, videoId=None, subscriptionId=None):
    log.debug("Checking %s from %s in database" % (videoId, subscriptionId))
    with connection:
        data = connection.execute("SELECT videoId FROM videos WHERE videoId = ? AND subscriptionId = ?", (videoId, subscriptionId))
    
    return data

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pickle-file', default='credentials.pickle', help='File to store access token once authenticated')
    parser.add_argument('--credentials-file', default='client_secret.json', help='JSON file with credentials to oAuth2 account')
    parser.add_argument('--max-results', default='50', type=int, help='JSON file with credentials to oAuth2 account')
    parser.add_argument('--published-after', help='Timestamp in ISO8601 (YYYY-MM-DDThh:mm:ss.sZ) format.')
    parser.add_argument('--youtube-channel', default='', help='Name of channel to do stuff with')
    parser.add_argument('--youtube-playlist', default='', help='Name of channel to do stuff with')
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
    
    con = sqlite3.connect('my.db')
    with con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                videoId TEXT NOT NULL PRIMARY KEY,
                timestamp TEXT,
                title TEXT,
                subscriptionId TEXT
            );
        """)

    print(get_last_run(last_run_file=last_run_file, publishedAfter=args.published_after))

    credentials = authenticate(credentials_file=args.credentials_file, pickle_credentials=args.pickle_file, scopes=scopes)

    channels = get_channel_id(credentials=credentials)
    channel = jq.all('.[]|select(all(.title; contains("%s")))|{"id": .id, "title": .title}' % (args.youtube_channel), channels)[0]

    user_playlists = get_user_playlists(credentials=credentials, channelId=channel["id"])
    user_playlists_refined = jq.all('.[] | { "title": .snippet.title, "id": .id }', user_playlists)
    user_playlist = jq.all('.[]|select(all(.title; contains("%s")))|{"id": .id, "title": .title}' % (args.youtube_playlist), user_playlists_refined)[0]
    
    selected_playlist = get_playlist(credentials=credentials, channelId=channel["id"], playlistId=user_playlist["id"])
    selected_playlist_refined = jq.all('.[] | { "title": .snippet.title, "id": .snippet.resourceId.videoId }', selected_playlist)
    
    subscriptions = get_subscriptions(credentials=credentials)
    subscriptions_refined = jq.all('.[] | { "title": .snippet.title, "id": .snippet.resourceId.channelId }', subscriptions)

    log.debug("Channels: "+json.dumps(channels, indent=4))
    log.debug("Playlists: "+json.dumps(user_playlists, indent=4))
    log.debug("Subscriptions: "+json.dumps(subscriptions_refined, indent=4, sort_keys=True))
    
    log.info("Channels detected: %s" % len(channels))
    log.info("Channel selected: %s (%s)" % (channel["title"], channel["id"]))
    log.info("Playlists on channel: %s" % len(user_playlists_refined))
    log.info("Playlist selected: %s (%s)" % (user_playlist["title"], user_playlist["id"]))
    log.debug("Content of Playlist selected: {}".format(json.dumps(selected_playlist_refined, indent=4)))
    log.info("Subscriptions on selected channel: %s" % len(subscriptions_refined))

    i=1
    for subs in subscriptions_refined:
        log.info("Processing subscription %s (%s)" % (subs["title"], subs["id"]))
                
        sub_activity = get_subscription_activity(credentials=credentials, channel=subs["id"], publishedAfter=args.published_after)
        sub_activity_refined = jq.all('.[] | { "title": .snippet.title, "videoId": .contentDetails.upload.videoId, "publishedAt": .snippet.publishedAt }', sub_activity)
        sub_activity_refined.sort(key = lambda x:x['publishedAt'], reverse=True)

        print(json.dumps(sub_activity_refined, indent=4))
        
        a=1
        for activity in sub_activity_refined:
            log.debug("Processing %s (%s)" % (activity["title"], activity["videoId"]))
            
            results = get_from_db(connection=con, videoId=activity["videoId"], subscriptionId=subs["id"])
            count_in_database = len(results.fetchall())
            
            
            if activity["videoId"] not in selected_playlist_refined[0].values():
                if count_in_database < 1:
                    save_to_db(connection=con, videoId=activity["videoId"], timestamp=now, title=activity["title"], subscriptionId=subs["id"])
                add_to_playlist(credentials=credentials, channelId=channel["id"], playlistId=user_playlist["id"], videoId=str(activity["videoId"]))
            else:
                if count_in_database < 1:
                    save_to_db(connection=con, videoId=activity["videoId"], timestamp=now, title=activity["title"], subscriptionId=subs["id"])
                log.info("Video %s (%s) already in playlist %s" % (activity["title"], activity["videoId"], user_playlist["title"]))

            a=a + 1
            if a > 5:
                break
        
        i=i + 1
        if i > 1:
            break

        # for key, value in subs:
        #     print("key: %s" % key)
        #     print("value: %s" % value)

    # sub_activity = get_subscription_activity(credentials=credentials, channel="UCr8oc-LOaApCXWLjL7vdsgw", publishedAfter=args.published_after)
    # activity_refined = jq.all('.[] | { "id": .contentDetails.upload.videoId, "title": .snippet.title }', sub_activity)
    # log.info("Activity list count: %s" % len(activity_refined))
    # print(json.dumps(activity_refined, indent=4, sort_keys=True))
    
    #sprint(subscriptions_response2)
    #print(subscriptions_response.get("nextPageToken"))
    
    con.close()
    

if __name__ == '__main__':
    main()
