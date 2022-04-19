import argparse
from datetime import timezone, datetime
from distutils.filelist import glob_to_re
from dateutil.relativedelta import relativedelta
import json
import jq
import logging
import os
import pickle
import sqlite3
import sys
import time
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

log = logging.getLogger(__name__)
now = datetime.now(timezone.utc).isoformat() #.strftime("%Y-%m-%dT%H:%M:%S.%-SZ")
oneyearback = datetime.now(timezone.utc) - relativedelta(years=1)
oneyearback = oneyearback.isoformat()#strftime("%Y-%m-%dT%H:%M:%S.%-SZ")
errors=0
scopes = [
    'https://www.googleapis.com/auth/youtubepartner',
    'https://www.googleapis.com/auth/youtube.force-ssl',
    'https://www.googleapis.com/auth/youtube', 
    'https://www.googleapis.com/auth/youtube.readonly'
    ]
loggFormat = "%(asctime)5s %(levelname)10s %(message)s (%(name)s)"

def get_last_run(connection=None):
    log.debug("Checking last run in DB")
    with connection:
        data = connection.execute("SELECT timestamp FROM last_run WHERE id = 1")
        
    data = data.fetchall()
    
    return data

def set_last_run(connection=None, timestamp=None):
    sql = 'INSERT OR REPLACE INTO last_run (id, timestamp) VALUES(?, ?)'
    data = [(1, timestamp)]
    with connection:
        try:
            connection.executemany(sql, data)
            log.info("Timestamp for last run is set to %s" % timestamp)
        except:
            log.error("set_last_run: Error!")
    log.info("Last run updated in DB: %s" % (timestamp))

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
    global errors
    
    subscriptions_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("Getting all your subscription")
        subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical")
    else:
        log.debug("get_subscriptions() invoked with nextPageToken %s!" % nextPage)
        subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical", pageToken=nextPage)

    try:
        subscriptions_response = subscriptions_request.execute()
        log.debug("subscriptions_response is of type %s and items count %s" % (type(subscriptions_response),len(subscriptions_response)))
    except HttpError as err:
        errors = errors + 1
        log.error("get_subscriptions: Error: {}".format(err))
        return False
    
    sub_dict = subscriptions_response["items"]
    log.debug("sub_dict is of type %s and items count %s" % (type(sub_dict),len(sub_dict)))
    if nextPage is None:
        log.info("Total amount of subscriptions: %s (from youtube API)" % subscriptions_response["pageInfo"]["totalResults"])
    
    if "nextPageToken" in subscriptions_response:
        log.info("nextPageToken detected!")
        nextPageToken = subscriptions_response.get("nextPageToken")
        subscriptions_response_nextpage = get_subscriptions(credentials=credentials, nextPage=nextPageToken) if subscriptions_response_nextpage != False else []
        sub_dict_nextpage = subscriptions_response_nextpage
        log.debug("sub_dict_nextpage is of type %s and items count %s" % (type(sub_dict_nextpage),len(sub_dict_nextpage)))
        sub_dict = [*sub_dict, *sub_dict_nextpage]

    return sub_dict

def get_subscription_activity(credentials=None, channel=None, publishedAfter=None, nextPage=None):
    global errors
    
    activity_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("Getting activity for channelId: %s" % channel)
        activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, publishedAfter=publishedAfter, uploadType="upload", channelId=channel)
    else:
        log.debug("Getting activity for channel %s invoked with nextPageToken %s!" % (channel, nextPage))
        activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, publishedAfter=publishedAfter, uploadType="upload", channelId=channel, pageToken=nextPage)
    
    try:
        activity_response = activity_request.execute()
        log.debug("activity_response is of type %s and items count %s" % (type(activity_response),len(activity_response)))
        print(activity_response)
        print(activity_request)
    except HttpError as err:
        errors = errors + 1
        log.error("get_subscription_activity: Error: {}".format(err))
        return False
        
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
    global errors
    
    channel_youtube = build("youtube", "v3", credentials=credentials)
    channel_request = channel_youtube.channels().list(
        part="snippet,contentDetails",
        mine=True
    )
    try:
        channel_response = channel_request.execute()
    except HttpError as err:
        errors = errors + 1
        log.error("get_channel_id: Error: {}".format(err))
        return False

    channel_list = channel_response["items"]

    channel_list = jq.all('.[] | { "title": .snippet.title, "id": .id }', channel_list)

    return channel_list

def get_user_playlists(credentials=None, channelId=None, nextPage=None):
    global errors
    
    playlists_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("Getting all your playlists")
        playlists_request = playlists_youtube.playlists().list(part="snippet,contentDetails", channelId=channelId, maxResults=50)
    else:
        log.debug("get_subscriptions() invoked with nextPageToken %s!" % nextPage)
        playlists_request = playlists_youtube.subscriptions().list(part="snippet,contentDetails", channelId=channelId, maxResults=50, pageToken=nextPage)

    try:
        playlists_response = playlists_request.execute()
        log.debug("playlists_response is of type %s and items count %s" % (type(playlists_response),len(playlists_response)))
    except HttpError as err:
        errors = errors + 1
        log.error("get_user_playlists: Error: {}".format(err))
        return False
    
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
    global errors
    
    playlist_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("Getting playlist %s" % playlistId)
        playlist_request = playlist_youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlistId, maxResults=50)
    else:
        log.debug("get_subscriptions() invoked with nextPageToken %s!" % nextPage)
        playlist_request = playlist_youtube.subscriptions().list(part="snippet,contentDetails", playlistId=playlistId, maxResults=50, pageToken=nextPage)

    try:
        playlist_response = playlist_request.execute()
        log.debug("playlist_response is of type %s and items count %s" % (type(playlist_response),len(playlist_response)))
    
        if nextPage is None:
            log.info("Total amount items on playlist: %s (from youtube API)" % playlist_response["pageInfo"]["totalResults"])
    except HttpError as err:
        errors = errors + 1
        log.error("get_playlist: Error: {}".format(err))
        return False
        
        
    playlist_dict = playlist_response["items"]
    log.debug("playlist_dict is of type %s and items count %s" % (type(playlist_dict),len(playlist_dict)))
        
    
    if "nextPageToken" in playlist_response:
        log.info("nextPageToken detected!")
        nextPageToken = playlist_response.get("nextPageToken")
        playlist_response_nextpage = get_user_playlists(credentials=credentials, channelId=channelId, nextPage=nextPageToken)
        playlist_dict_nextpage = playlist_response_nextpage
        log.debug("playlist_dict_nextpage is of type %s and items count %s" % (type(playlist_dict_nextpage),len(playlist_dict_nextpage)))
        playlist_dict = [*playlist_dict, *playlist_dict_nextpage]
    
    return playlist_dict

def add_to_playlist(credentials=None, connection=None, channelId=None, playlistId=None, subscriptionId=None, videoId=None, videoTitle=None):
    global errors
    
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
            }
          }
        }
    )
    try:
        playlist_response = playlist_request.execute()
        log.debug("Playlist Insert respons: {}".format(json.dumps(playlist_response, indent=4)))
        log.info("%s added to %s in position %s" % (videoId, playlistId, playlist_response["snippet"].get("position")))
        
    except HttpError as err:
        errors = errors + 1
        log.error("add_to_playlist: Error: {}".format(err))
        return False
    
    save_to_db(connection=connection, videoId=videoId, timestamp=now, title=videoTitle, subscriptionId=subscriptionId)

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
        
    data = data.fetchall()
    
    return data

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pickle-file', default='credentials.pickle', help='File to store access token once authenticated')
    parser.add_argument('--credentials-file', default='client_secret.json', help='JSON file with credentials to oAuth2 account')
    parser.add_argument('--max-results', default='50', type=int, help='JSON file with credentials to oAuth2 account')
    parser.add_argument('--published-after', help='Timestamp in ISO8601 (YYYY-MM-DDThh:mm:ss.sZ) format.')
    parser.add_argument('--youtube-channel', default='', help='Name of channel to do stuff with')
    parser.add_argument('--youtube-playlist', default='', help='Name of channel to do stuff with')
    parser.add_argument('--youtube-activity-limit', default='110', type=int, help='How much activity to process pr. subscription')
    parser.add_argument('--youtube-subscription-limit', default='0', type=int, help='How much activity to process pr. subscription')
    parser.add_argument('--youtube-playlist-sleep', default='10', type=int, help='how log to wait betwene playlist API insert-calls')
    parser.add_argument('--youtube-subscription-sleep', default='30', type=int, help='how log to wait betwene playlist API insert-calls')
    parser.add_argument('--log-level', default='warning', help='Set loglevel. debug,info,warning or error')
    parser.add_argument('--log-file', dest='log_file', default='stream', help='file to cast logs to. if you want all output to stdout type "stream"')
        
    args = parser.parse_args()

    if args.log_level == "debug":
        loglevel = logging.DEBUG
    elif args.log_level == "info":
        loglevel = logging.INFO
    elif args.log_level == "warning":
        loglevel = logging.WARNING
    elif args.log_level == "error":
        loglevel = logging.ERROR
        
    if args.log_file == "stream":
        logging.basicConfig(stream=sys.stdout, format=loggFormat, level=loglevel)
    else:
        logging.basicConfig(filename=args.log_file, format=loggFormat, level=loglevel)
    
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
        con.execute("""
            CREATE TABLE IF NOT EXISTS last_run (
                id NUMBER NOT NULL PRIMARY KEY,
                timestamp TEXT
            );
        """)
        
    db_last_run = get_last_run(connection=con)
        
    if args.published_after is not None:
        published_after = args.published_after
    else: 
        if len(get_last_run(connection=con)) > 0:
            published_after = get_last_run(connection=con)[0]
        else:
            published_after = now
    
    log.info("now: %s" % now)
    log.info("oneyearback: %s" % oneyearback)
    log.info("db_last_run: %s" % db_last_run)
    log.info("args.published_after: %s" % args.published_after)
    log.info("published_after: %s" % published_after)
    
    #return 0

    credentials = authenticate(credentials_file=args.credentials_file, pickle_credentials=args.pickle_file, scopes=scopes)

    channels = get_channel_id(credentials=credentials)
    channel = jq.all('.[]|select(all(.title; contains("%s")))|{"id": .id, "title": .title}' % (args.youtube_channel), channels)[0] if channels != False else []

    user_playlists = get_user_playlists(credentials=credentials, channelId=channel["id"]) if channels != False else False
    user_playlists_refined = jq.all('.[] | { "title": .snippet.title, "id": .id }', user_playlists) if user_playlists != False else []
    user_playlist = jq.all('.[]|select(all(.title; contains("%s")))|["id": .id, "title": .title]' % (args.youtube_playlist), user_playlists_refined) if not isinstance(user_playlists_refined, list) else []
    
    selected_playlist = get_playlist(credentials=credentials, channelId=channel["id"], playlistId=user_playlist["id"]) if channels != False and user_playlist != False else False
    selected_playlist_refined = jq.all('.[] | { "title": .snippet.title, "id": .snippet.resourceId.videoId }', selected_playlist) if selected_playlist != False else []
    
    subscriptions = get_subscriptions(credentials=credentials)
    subscriptions_refined = jq.all('.[] | { "title": .snippet.title, "id": .snippet.resourceId.channelId }', subscriptions) if subscriptions != False else []
    
    last_run = get_last_run(connection=con)

    log.debug("Last script run: %s" % (last_run))
    log.debug("Channels: "+json.dumps(channels, indent=4))
    log.debug("Playlists: "+json.dumps(user_playlists, indent=4))
    log.debug("Subscriptions: "+json.dumps(subscriptions_refined, indent=4, sort_keys=True))
    
    log.info("Channels detected: %s" % len(channels)) if channels != False else None
    log.info("Channel selected: %s (%s)" % (channel["title"], channel["id"])) if channels != False else None
    log.info("Playlists on channel: %s" % len(user_playlists_refined)) if user_playlists != False else None
    log.info("Playlist selected: %s (%s)" % (user_playlist["title"], user_playlist["id"])) if user_playlists != False else None
    log.debug("Content of Playlist selected: {}".format(json.dumps(selected_playlist, indent=4))) if user_playlists != False else None
    log.debug("Content of selected_playlist_refined: {}".format(json.dumps(selected_playlist_refined, indent=4))) if user_playlists != False else None
    log.info("Subscriptions on selected channel: %s" % len(subscriptions_refined)) if subscriptions_refined != False else None

    s=0
    for subs in subscriptions_refined:
        log.info("Processing subscription %s (%s), but sleeping for %s seconds first" % (subs["title"], subs["id"], args.youtube_subscription_sleep))
        time.sleep(args.youtube_subscription_sleep)
        
        sub_activity = get_subscription_activity(credentials=credentials, channel=subs["id"], publishedAfter=published_after)
        sub_activity_refined = jq.all('.[] | select(all(.snippet.type; contains("upload"))) | { "title": .snippet.title, "videoId": .contentDetails.upload.videoId, "publishedAt": .snippet.publishedAt }', sub_activity) if sub_activity != False else []
        sub_activity_refined.sort(key = lambda x:x['publishedAt'], reverse=True) if sub_activity != False else []

        log.debug("sub_activity_refined: {}".format(json.dumps(sub_activity_refined, indent=4)))
        
        a=0
        for activity in sub_activity_refined:
            log.debug("Processing %s (%s)" % (activity["title"], activity["videoId"]))
            
            results = get_from_db(connection=con, videoId=activity["videoId"], subscriptionId=subs["id"])
            count_in_database = len(results)
            log.debug("Database results: {}".format(results))
            log.info("Database count: %s" % (count_in_database))
            
            log.debug("selected_playlist_refined -> %s" % (type(selected_playlist_refined)))
            if isinstance(selected_playlist_refined, list) and len(selected_playlist_refined) == 0:
                if not count_in_database > 0:
                    time.sleep(args.youtube_playlist_sleep)
                    add_to_playlist(credentials=credentials, connection=con, channelId=channel["id"], playlistId=user_playlist["id"], subscriptionId=subs["id"], videoId=str(activity["videoId"]), videoTitle=str(activity["title"]))
                    log.info("ADDING Video %s (%s) to EMPTY playlist %s" % (activity["title"], activity["videoId"], user_playlist["title"]))
                else:
                    log.info("Video %s (%s) already in database or playlist %s" % (activity["title"], activity["videoId"], user_playlist["title"]))
            else:
                if activity["videoId"] not in selected_playlist_refined[0].values():
                    if not count_in_database > 0:
                        time.sleep(args.youtube_playlist_sleep)
                        add_to_playlist(credentials=credentials, connection=con, channelId=channel["id"], playlistId=user_playlist["id"], subscriptionId=subs["id"], videoId=str(activity["videoId"]), videoTitle=str(activity["title"]))
                        log.info("ADDING Video %s (%s) to playlist %s" % (activity["title"], activity["videoId"], user_playlist["title"]))
                else:
                    log.info("Video %s (%s) already in database or playlist %s" % (activity["title"], activity["videoId"], user_playlist["title"]))

            a=a + 1
            if args.youtube_activity_limit != 0 and a >= args.youtube_activity_limit:
                log.info("YouTube activity limit reached! exiting activity loop")
                break
        
        s=s + 1
        if args.youtube_subscription_limit != 0 and s >= args.youtube_subscription_limit:
            log.info("YouTube subscription limit reached! exiting subscription loop")
            break
    
    if errors == 0:
        set_last_run(connection=con, timestamp=now)
    else:
        log.error("Last run timestamp not set because of errors! (%s)" % errors)
    
    con.close()
    

if __name__ == '__main__':
    main()
