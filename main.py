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
sqlite_db = "my.db"
errors=0
criticals = [400, 401, 402, 403, 404, 405, 409, 410, 412, 413, 416, 417, 428, 429, 500, 501, 503]
scopes = [
    'https://www.googleapis.com/auth/youtubepartner',
    'https://www.googleapis.com/auth/youtube.force-ssl',
    'https://www.googleapis.com/auth/youtube', 
    'https://www.googleapis.com/auth/youtube.readonly'
    ]
loggFormat = "%(asctime)5s %(levelname)10s %(message)s (%(name)s)"

def init_db():
    con = sqlite3.connect(sqlite_db)
    
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
    con.close()

def get_last_run():
    con = sqlite3.connect(sqlite_db)
    
    log.debug("get_last_run: Checking last run in DB")
    with con:
        data = con.execute("SELECT timestamp FROM last_run WHERE id = 1")
        
    data = data.fetchall()
    
    con.close()
    
    log.info("get_last_run: Last run in DB %s" % data[0])
    
    return data

def set_last_run(timestamp=None):
    con = sqlite3.connect(sqlite_db)
    
    sql = 'INSERT OR REPLACE INTO last_run (id, timestamp) VALUES(?, ?)'
    data = [(1, timestamp)]
    with con:
        try:
            con.executemany(sql, data)
            log.info("set_last_run: Timestamp for last run is set to %s" % timestamp)
        except:
            log.error("set_last_run: Error!")
    
    con.close()
    
    log.info("set_last_run: Last run updated in DB: %s" % (timestamp))

def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pickle-file', default='credentials.pickle', help='File to store access token once authenticated')
    parser.add_argument('--credentials-file', default='client_secret.json', help='JSON file with credentials to oAuth2 account')
    parser.add_argument('--max-results', default='50', type=int, help='JSON file with credentials to oAuth2 account')
    parser.add_argument('--published-after', help='Timestamp in ISO8601 (YYYY-MM-DDThh:mm:ss.sZ) format.')
    parser.add_argument('--youtube-channel', default='', help='Name of channel to do stuff with')
    parser.add_argument('--youtube-playlist', default='', help='Name of channel to do stuff with')
    parser.add_argument('--youtube-activity-limit', default='0', type=int, help='How much activity to process pr. subscription')
    parser.add_argument('--youtube-subscription-limit', default='0', type=int, help='How much activity to process pr. subscription')
    parser.add_argument('--youtube-playlist-sleep', default='10', type=int, help='how log to wait betwene playlist API insert-calls')
    parser.add_argument('--youtube-subscription-sleep', default='30', type=int, help='how log to wait betwene playlist API insert-calls')
    parser.add_argument('--log-level', default='warning', help='Set loglevel. debug,info,warning or error')
    parser.add_argument('--log-file', dest='log_file', default='stream', help='file to cast logs to. if you want all output to stdout type "stream"')
    
    return parser.parse_args()

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
    global criticals
    
    subscriptions_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("get_subscriptions: Getting all your subscription")
        subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical")
    else:
        log.debug("get_subscriptions: get_subscriptions() invoked with nextPageToken %s!" % nextPage)
        subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical", pageToken=nextPage)

    try:
        subscriptions_response = subscriptions_request.execute()
        log.debug("get_subscriptions: subscriptions_response is of type %s and items count %s" % (type(subscriptions_response),len(subscriptions_response)))
    except HttpError as err:
        errors = errors + 1
        if err.resp.status in criticals:
            log.critical("Critical error encountered! {}".format(err))
            raise SystemExit(-1)
        else:
            log.error("get_subscriptions: Error: {}".format(err))
            
        return False
    
    sub_dict = subscriptions_response["items"]
    log.debug("get_subscriptions: sub_dict is of type %s and items count %s" % (type(sub_dict),len(sub_dict)))
    if nextPage is None:
        log.info("get_subscriptions: Total amount of subscriptions: %s (from youtube API)" % subscriptions_response["pageInfo"]["totalResults"])
    
    if "nextPageToken" in subscriptions_response:
        log.info("get_subscriptions: nextPageToken detected!")
        nextPageToken = subscriptions_response.get("nextPageToken")
        subscriptions_response_nextpage = get_subscriptions(credentials=credentials, nextPage=nextPageToken)
        sub_dict_nextpage = subscriptions_response_nextpage
        log.debug("get_subscriptions: sub_dict_nextpage is of type %s and items count %s" % (type(sub_dict_nextpage),len(sub_dict_nextpage)))
        sub_dict = [*sub_dict, *sub_dict_nextpage]

    return sub_dict

def get_subscription_activity(credentials=None, channel=None, publishedAfter=None, nextPage=None):
    global errors
    global criticals
    
    activity_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("get_subscription_activity: Getting activity for channelId: %s" % channel)
        activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, publishedAfter=publishedAfter, uploadType="upload", channelId=channel)
    else:
        log.debug("get_subscription_activity: Getting activity for channel %s invoked with nextPageToken %s!" % (channel, nextPage))
        activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, publishedAfter=publishedAfter, uploadType="upload", channelId=channel, pageToken=nextPage)
    
    try:
        activity_response = activity_request.execute()
        log.info("get_subscription_activity: activity_response is of type %s and items count %s" % (type(activity_response),len(activity_response)))
        log.debug("get_subscription_activity: activity_respons content: {}".format(json.dumps(activity_response, indent=4)))
    except HttpError as err:
        errors = errors + 1
        if err.resp.status in criticals:
            log.critical("Critical error encountered! {}".format(err))
            raise SystemExit(-1)
        else:
            log.error("get_subscription_activity: Error: {}".format(err))
        return False
        
    act_array = activity_response["items"]
    log.debug("get_subscription_activity: act_array is of type %s and items count %s" % (type(act_array),len(act_array)))
    if nextPage is None:
        log.info("get_subscription_activity: Total amount of activity: %s (from youtube API)" % activity_response["pageInfo"]["totalResults"])

    if "nextPageToken" in activity_response:
        nextPageToken = activity_response.get("nextPageToken")
        activity_response_nextpage = get_subscription_activity(credentials=credentials, channel=channel, nextPage=nextPageToken)
        act_array = [*act_array, *activity_response_nextpage]


    return act_array

def get_channel_id(credentials=None):
    global errors
    global criticals
    
    log.debug("get_channel_id: Geting list of channels")
    channel_youtube = build("youtube", "v3", credentials=credentials)
    channel_request = channel_youtube.channels().list(
        part="snippet,contentDetails",
        mine=True
    )
    try:
        channel_response = channel_request.execute()
        log.debug("get_channel_id: Respons: {}".format(json.dumps(channel_response, indent=4)))
    except HttpError as err:
        errors = errors + 1
        if err.resp.status in criticals:
            log.critical("Critical error encountered! {}".format(err))
            raise SystemExit(-1)
        else:
            log.error("get_channel_id: Error: {}".format(err))
        return False

    channel_list = channel_response["items"]

    channel_list = jq.all('.[] | { "title": .snippet.title, "id": .id }', channel_list)
    log.debug("get_channel_id: Final channel list: {}".format(json.dumps(channel_list, indent=4)))
    log.info("get_channel_id: Final channel list count: %s" % len(channel_list))

    return channel_list

def get_user_playlists(credentials=None, channelId=None, nextPage=None):
    global errors
    global criticals
    
    playlists_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("get_user_playlists: Getting all your playlists")
        playlists_request = playlists_youtube.playlists().list(part="snippet,contentDetails", channelId=channelId, maxResults=50)
    else:
        log.debug("get_subscriptions() invoked with nextPageToken %s!" % nextPage)
        playlists_request = playlists_youtube.subscriptions().list(part="snippet,contentDetails", channelId=channelId, maxResults=50, pageToken=nextPage)

    try:
        playlists_response = playlists_request.execute()
        log.debug("get_user_playlists: playlists_response is of type %s and items count %s" % (type(playlists_response),len(playlists_response)))
    except HttpError as err:
        errors = errors + 1
        if err.resp.status in criticals:
            log.critical("Critical error encountered! {}".format(err))
            raise SystemExit(-1)
        else:
            log.error("get_user_playlists: Error: {}".format(err))
        return False
    
    plists_dict = playlists_response["items"]
    log.info("get_user_playlists: plists_dict is of type %s and items count %s" % (type(plists_dict),len(plists_dict)))
    log.debug("get_user_playlists: plist_dict content: {}".format(json.dumps(plists_dict, indent=4)))
    if nextPage is None:
        log.info("get_user_playlists: Total amount of playlists: %s (from youtube API)" % playlists_response["pageInfo"]["totalResults"])
    
    if "nextPageToken" in playlists_response:
        log.info("get_user_playlists: nextPageToken detected!")
        nextPageToken = playlists_response.get("nextPageToken")
        playlists_response_nextpage = get_user_playlists(credentials=credentials, channelId=channelId, nextPage=nextPageToken)
        plists_dict_nextpage = playlists_response_nextpage
        log.debug("get_user_playlists: plists_dict_nextpage is of type %s and items count %s" % (type(plists_dict_nextpage),len(plists_dict_nextpage)))
        plists_dict = [*plists_dict, *plists_dict_nextpage]

    return plists_dict

def get_playlist(credentials=None, channelId=None, playlistId=None, nextPage=None):
    global errors
    global criticals
    
    playlist_youtube = build("youtube", "v3", credentials=credentials)
    if nextPage is None:
        log.info("get_playlist: Getting playlist %s" % playlistId)
        playlist_request = playlist_youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlistId, maxResults=50)
    else:
        log.debug("get_playlist: get_playlists() invoked with nextPageToken %s!" % nextPage)
        playlist_request = playlist_youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlistId, maxResults=50, pageToken=nextPage)

    try:
        playlist_response = playlist_request.execute()
        log.debug("get_playlist: playlist_response is of type %s and items count %s" % (type(playlist_response),len(playlist_response)))
    
        if nextPage is None:
            log.info("get_playlist: Total amount items on playlist: %s (from youtube API)" % playlist_response["pageInfo"]["totalResults"])
    except HttpError as err:
        errors = errors + 1
        log.warning("httpError status: %s", err.resp.status)
        if err.resp.status in criticals:
            log.critical("Critical error encountered! {}".format(err))
            raise SystemExit(-1)
        else:
            log.error("get_playlist: Error: {}".format(err))
        return False
        
        
    playlist_dict = playlist_response["items"]
    log.debug("get_playlist: playlist_dict is of type %s and items count %s" % (type(playlist_dict),len(playlist_dict)))
        
    
    if "nextPageToken" in playlist_response:
        log.info("get_playlist: nextPageToken detected!")
        nextPageToken = playlist_response.get("nextPageToken")
        playlist_response_nextpage = get_playlist(credentials=credentials, channelId=channelId, playlistId=playlistId, nextPage=nextPageToken)
        playlist_dict_nextpage = playlist_response_nextpage
        log.debug("get_playlist: playlist_dict_nextpage is of type %s and items count %s" % (type(playlist_dict_nextpage),len(playlist_dict_nextpage)))
        playlist_dict = [*playlist_dict, *playlist_dict_nextpage]
    
    return playlist_dict

def add_to_playlist(credentials=None, channelId=None, playlistId=None, subscriptionId=None, videoId=None, videoTitle=None):
    global errors
    global criticals
    
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
        if err.resp.status in criticals:
            log.critical("Critical error encountered! {}".format(err))
            raise SystemExit(-1)
        else:
            log.error("add_to_playlist: Error: {}".format(err))
        return False
    
    save_to_db(videoId=videoId, timestamp=now, title=videoTitle, subscriptionId=subscriptionId)

    return playlist_response

def save_to_db(videoId=None, timestamp=None, title=None, subscriptionId=None):
    con = sqlite3.connect(sqlite_db)
    
    sql = 'INSERT OR REPLACE INTO videos (videoId, timestamp, title, subscriptionId) VALUES(?, ?, ?, ?)'
    data = [(videoId, timestamp, title, subscriptionId)]
    with con:
        try:
            con.executemany(sql, data)
        except:
            log.error("Database error!")
    
    log.info("Video %s (%s) from %s added to database" % (title, videoId, subscriptionId))
    
    con.close()
            
def get_from_db(videoId=None, subscriptionId=None):
    con = sqlite3.connect(sqlite_db)
    
    log.debug("get_from_db: Checking %s from %s in database" % (videoId, subscriptionId))
    with con:
        data = con.execute("SELECT videoId FROM videos WHERE videoId = ? AND subscriptionId = ?", (videoId, subscriptionId))
        
    data = data.fetchall()
    
    con.close()
    
    log.debug("get_from_db: results {}".format(json.dumps(data, indent=4)))
    log.info("get_from_db: count: %s" % len(data))
    
    return data

def main():
    args = get_arguments()
    
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
        
    log.debug("Arguments: {}".format(args))
    
    init_db()
        
    db_last_run = get_last_run()
        
    if args.published_after is not None:
        published_after = args.published_after
    else: 
        if len(db_last_run) > 0:
            published_after = db_last_run[0]
        else:
            published_after = now
    
    log.info("now: %s" % now)
    log.info("oneyearback: %s" % oneyearback)
    log.info("db_last_run: %s" % db_last_run)
    log.info("args.published_after: %s" % args.published_after)
    log.info("published_after: %s" % published_after)

    credentials = authenticate(credentials_file=args.credentials_file, pickle_credentials=args.pickle_file, scopes=scopes)

    channels = get_channel_id(credentials=credentials)
    channel = jq.all('.[]|select(all(.title; contains("%s")))| { "id": .id, "title": .title }' % (args.youtube_channel), channels)[0]
    log.info("Channel selected: %s (%s)" % (channel["title"], channel["id"]))
    
    #return 0

    user_playlists = get_user_playlists(credentials=credentials, channelId=channel["id"])
    user_playlists_refined = jq.all('.[] | { "title": .snippet.title, "id": .id }', user_playlists) if user_playlists != False else []
    log.info("Playlists on channel: %s" % len(user_playlists_refined))
    user_playlist = jq.all('.[]|select(all(.title; contains("%s")))|{ "id": .id, "title": .title }' % (args.youtube_playlist), user_playlists_refined)[0]
    log.info("Playlist selected: %s (%s)" % (user_playlist["title"], user_playlist["id"]))
    
    
    selected_playlist = get_playlist(credentials=credentials, channelId=channel["id"], playlistId=user_playlist["id"])
    selected_playlist_refined = jq.all('.[] | { "title": .snippet.title, "id": .snippet.resourceId.videoId }', selected_playlist) if selected_playlist != False else []
    log.debug("Content of Playlist selected: {}".format(json.dumps(selected_playlist, indent=4))) if user_playlists != False else None
    log.debug("Content of selected_playlist_refined: {}".format(json.dumps(selected_playlist_refined, indent=4))) if user_playlists != False else None
    
    subscriptions = get_subscriptions(credentials=credentials)
    subscriptions_refined = jq.all('.[] | { "title": .snippet.title, "id": .snippet.resourceId.channelId }', subscriptions) if subscriptions != False else []

    log.debug("Last script run: %s" % (db_last_run))
    log.debug("Channels: "+json.dumps(channels, indent=4))
    log.debug("Playlists: "+json.dumps(user_playlists, indent=4))
    log.debug("Subscriptions: "+json.dumps(subscriptions_refined, indent=4, sort_keys=True))
    
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
            log.info("%s - Processing %s (%s)" % (subs["title"], activity["title"], activity["videoId"]))
            
            results = get_from_db(videoId=activity["videoId"], subscriptionId=subs["id"])
            count_in_database = len(results)
            
            if isinstance(selected_playlist_refined, list) and len(selected_playlist_refined) == 0:
                if not count_in_database > 0:
                    time.sleep(args.youtube_playlist_sleep)
                    add_to_playlist(credentials=credentials, channelId=channel["id"], playlistId=user_playlist["id"], subscriptionId=subs["id"], videoId=str(activity["videoId"]), videoTitle=str(activity["title"]))
                    log.info("%s - ADDING Video %s (%s) to EMPTY playlist %s" % (subs["title"], activity["title"], activity["videoId"], user_playlist["title"]))
                else:
                    log.info("%s - Video %s (%s) already in database or playlist %s" % (subs["title"], activity["title"], activity["videoId"], user_playlist["title"]))
            else:
                if activity["videoId"] not in selected_playlist_refined[0].values():
                    if not count_in_database > 0:
                        time.sleep(args.youtube_playlist_sleep)
                        add_to_playlist(credentials=credentials, channelId=channel["id"], playlistId=user_playlist["id"], subscriptionId=subs["id"], videoId=str(activity["videoId"]), videoTitle=str(activity["title"]))
                        log.info("%s - Video %s (%s) added to playlist %s" % (subs["title"], activity["title"], activity["videoId"], user_playlist["title"]))
                else:
                    log.info("%s - Video %s (%s) already in database or playlist %s" % (subs["title"], activity["title"], activity["videoId"], user_playlist["title"]))

            a=a + 1
            if args.youtube_activity_limit != 0 and a >= args.youtube_activity_limit:
                log.info("%s - YouTube activity limit reached! exiting activity loop" % (subs["title"]))
                break
        
        s=s + 1
        if args.youtube_subscription_limit != 0 and s >= args.youtube_subscription_limit:
            log.info("YouTube subscription limit reached! exiting subscription loop")
            break
    
    if errors == 0:
        set_last_run(timestamp=now)
    else:
        log.error("Last run timestamp not set because of errors! (%s)" % errors)
    

if __name__ == '__main__':
    main()
