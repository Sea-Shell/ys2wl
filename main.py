from ast import Subscript
import configargparse
from datetime import timezone, datetime, timedelta
from distutils.filelist import glob_to_re
import json
from lib2to3.pgen2.token import LESS
import jq
import logging
from logging.handlers import RotatingFileHandler
import os
import pickle
from retrying import retry
import sqlite3
import sys
import time
from tzlocal import get_localzone
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

api_calls=0
errors=0
subscriptions_processed=0
subscriptions_skipped=0
videos_added=0
videos_skipped=0

# Time stuff...
times = dict()
times["local_timezone"] = get_localzone()
times["date_format"] = '%Y-%m-%dT%H:%M:%S'
times["now_datetime"] = datetime.now(tz=timezone.utc)
times["now"] = datetime.now(times["local_timezone"])
times["now_iso"] = times["now"].isoformat()
times["now_format"] = times["now"].strftime(times["date_format"])
times["yesterday"] = times["now"] - timedelta(days=1)
times["yesterday_iso"] = times["yesterday"].isoformat()
times["oneyearback"] = times["now"] - timedelta(weeks=52)
times["oneyearback_iso"] = times["oneyearback"].isoformat()

# Variables
args = None
log = None
loggFormat = "%(asctime)5s %(levelname)10s %(message)s (%(name)s)"
criticals = [400, 401, 402, 403, 404, 405, 409, 410, 412, 413, 416, 417, 428, 429, 500, 501, 503]
scopes = [
    'https://www.googleapis.com/auth/youtubepartner',
    'https://www.googleapis.com/auth/youtube.force-ssl',
    'https://www.googleapis.com/auth/youtube', 
    'https://www.googleapis.com/auth/youtube.readonly'
    ]

def get_arguments():
    parser = configargparse.ArgumentParser(description='Add latest activity from your subscriptions on YouTube to a playlist', default_config_files=['/etc/ysl/config.yml', '~/.ysl/config.yml'])
    parser.add('--config', env_var='CONFIG', is_config_file=True, help='Path to yaml config file')
    parser.add('--pickle-file', env_var="PICKLE_FILE", default='credentials.pickle', help='File to store access token once authenticated')
    parser.add('--credentials-file', env_var="CREDENTIALS_FILE", default='client_secret.json', help='JSON file with credentials to oAuth2 account')
    parser.add('--database-file', env_var="DATABASE_FILE", default='my.db', help='Location of sqlite database file. Will be created if not exists')
    parser.add('--local-json-files', env_var="LOCAL_JSON_FILES", action="store_true", help='JSON file with credentials to oAuth2 account')
    parser.add('--max-results', env_var="MAX_RESULTS", default='50', type=int, help='JSON file with credentials to oAuth2 account')
    parser.add('--published-after', env_var="PUBLISHED_AFTER", default=None, help='Timestamp in ISO8601 (YYYY-MM-DDThh:mm:ss.sZ) format.')
    parser.add('--reprocess-days', env_var="REPROCESS_DAYS", default=2, type=int, help='Amount of days before subscription will be processed again.')
    parser.add('--youtube-channel', env_var="YOUTUBE_CHANNEL", default='', help='Name of channel to do stuff with')
    parser.add('--youtube-playlist', env_var="YOUTUBE_PLAYLIST", default='', help='Name of channel to do stuff with')
    parser.add('--youtube-activity-limit', env_var="YOUTUBE_ACTIVITY_LIMIT", default='0', type=int, help='How much activity to process pr. subscription')
    parser.add('--youtube-subscription-limit', env_var="YOUTUBE_SUBSCRIPTION_LIMIT", default='0', type=int, help='How much activity to process pr. subscription')
    parser.add('--youtube-playlist-sleep', env_var="YOUTUBE_PLAYLIST_SLEEP", default='10', type=int, help='how log to wait betwene playlist API insert-calls')
    parser.add('--youtube-subscription-sleep', env_var="YOUTUBE_SUBSCRIPTION_SLEEP", default='30', type=int, help='how log to wait betwene playlist API insert-calls')
    parser.add('--log-level', env_var="LOG_LEVEL", default='warning', help='Set loglevel. debug,info,warning or error')
    parser.add('--log-file', env_var="LOG_FILE", dest='log_file', default='stream', help='file to cast logs to. if you want all output to stdout type "stream"')
    
    return parser.parse_args()

def setup_logger():
    global errors
    global loggFormat
    global times
    global args
    global api_calls
    global log
    
    log = logging.getLogger('YouTube_SubLater')
    format = logging.Formatter(fmt=loggFormat, datefmt=times["date_format"])
    
        
    if args.log_file == "stream":
        handler = logging.StreamHandler()
        handler.setFormatter(format)
        log.addHandler(handler)
    else:
        rotater = RotatingFileHandler(filename=args.log_file, maxBytes=100000000, backupCount=10)
        rotater.setFormatter(format)
        #rotater.doRollover()
        log.addHandler(rotater)
        
    if args.log_level == "debug":
        log.setLevel(logging.DEBUG)
    elif args.log_level == "info":
        log.setLevel(logging.INFO)
    elif args.log_level == "warning":
        log.setLevel(logging.WARNING)
    elif args.log_level == "error":
        log.setLevel(logging.ERROR)
    
    return log

def init_db():
    global args
    
    con = sqlite3.connect(args.database_file)
    
    with con:
        try:
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
            con.execute("""
                CREATE TABLE IF NOT EXISTS channel (
                    id NUMBER NOT NULL PRIMARY KEY,
                    title TEXT
                );
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS subscription (
                    id NUMBER NOT NULL PRIMARY KEY,
                    title TEXT,
                    timestamp TEXT
                );
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS playlist (
                    id NUMBER NOT NULL PRIMARY KEY,
                    title TEXT
                );
            """)
        except sqlite3.Error as err:
            log.error('init_db: Sql error: {}'.format(err.args))
            return False
    
    con.close()
    
    return True

def get_last_run():
    global args
    
    con = sqlite3.connect(args.database_file)
    
    log.info("get_last_run: Checking last run in DB")
    with con:
        try:
            query = con.execute("SELECT timestamp FROM last_run WHERE id = 1 LIMIT 1")
        except sqlite3.Error as err:
            log.error('get_last_run: Sql error: {}'.format(err.args))
            return False
        
    data = query.fetchall()
    
    con.close()
    
    log.info("get_last_run: Last run in DB %s" % data[0])
    
    return data[0]

def set_last_run(timestamp=None):
    global args
    
    if args.log_level != "debug":
        con = sqlite3.connect(args.database_file)
        
        sql = 'INSERT OR REPLACE INTO last_run (id, timestamp) VALUES(?, ?)'
        data = [(1, timestamp)]
        with con:
            try:
                con.executemany(sql, data)
                log.info("set_last_run: Last run updated in DB: %s" % timestamp)
            except sqlite3.Error as err:
                log.error('set_last_run: Sql error: {}'.format(err.args))
                return False
        
        con.close()
    else:
        log.info("set_last_run: NOT REALY! Last run updated in DB: %s" % (timestamp))

def insert_video_to_db(videoId=None, timestamp=None, title=None, subscriptionId=None):
    global args
    
    if args.log_level != "debug":
        con = sqlite3.connect(args.database_file)
        
        sql = 'INSERT OR REPLACE INTO videos (videoId, timestamp, title, subscriptionId) VALUES(?, ?, ?, ?)'
        data = [(videoId, timestamp, title, subscriptionId)]
        with con:
            try:
                con.executemany(sql, data)
            except sqlite3.Error as err:
                log.error('insert_video_to_db: Sql error: {}'.format(err.args))
                return False
        
        con.close()
    
    log.info("insert_video_to_db: Video %s (%s) from %s added to database" % (title, videoId, subscriptionId))

def get_video_from_db(videoId=None, subscriptionId=None):
    global args
    
    data = list()
    con = sqlite3.connect(args.database_file)
    
    log.info("get_video_from_db: Checking %s from %s in database" % (videoId, subscriptionId))
    try:
        query = con.execute('SELECT videoId FROM videos WHERE videoId=\"%s\" AND subscriptionId=\"%s\" LIMIT 1' % (videoId, subscriptionId))
    except sqlite3.Error as err:
        log.error('get_video_from_db: Sql error: {}'.format(err.args))
        
        return False
    
    rows = query.fetchall()
    log.debug("get_video_from_db: content of rows: {}".format(rows))
    log.info("get_video_from_db: count on rows: %s" % len(rows))
    con.close()
    
    if len(rows) > 0:
        for row in rows:
            data = "{ \"id\": \"%s\" }" % row[0]
            log.debug("get_video_from_db: content of data before json.loads(): {}".format(data))
            data = json.loads(data)
    
    return data

def insert_channel_to_db(channelId=None, channelTitle=None):
    global args
    
    if not args.local_json_files:
        con = sqlite3.connect(args.database_file)

        sql = 'INSERT OR REPLACE INTO channel (id, title) VALUES(?, ?)'
        data = [(channelId, channelTitle)]
        with con:
            try:
                con.executemany(sql, data)
                log.info("insert_channel_to_db: Channel %s with ID %s inserted into DB", channelId, channelTitle)
            except sqlite3.Error as err:
                log.error('insert_channel_to_db: Sql error: {}'.format(err.args))
                return False
            
        con.close()
        return True
    else:
        log.info("insert_channel_to_db: INSERT OR REPLACE INTO channel (id, title) VALUES(%s, %s)", channelId, channelTitle)
        return True

def get_channel_from_db():
    global args
    
    con = sqlite3.connect(args.database_file)
    data = list()
    
    try:
        query = con.execute("SELECT id, title FROM channel LIMIT 1")
        
        rows = query.fetchall()
        con.close()
    except sqlite3.Error as err:
        log.error('get_channel_from_db: Sql error: {}'.format(err.args))
        return False
    
    for row in rows:
        data = '[ { "id": "%s", "title": "%s" } ]' % (row[0], row[1])
        data = json.loads(data)
    
    log.debug("get_channel_from_db: results {}".format(json.dumps(data, indent=4)))
    log.info("get_channel_from_db: count: %s" % len(data))
    
    return data

def insert_playlist_to_db(playlistId=None, playlistTitle=None):
    global args
    
    con = sqlite3.connect(args.database_file)

    sql = 'INSERT OR REPLACE INTO playlist (id, title) VALUES(?, ?)'
    data = [(playlistId, playlistTitle)]
    with con:
        try:
            con.executemany(sql, data)
            log.info("insert_playlist_to_db: Channel %s with ID %s inserted into DB", playlistTitle, playlistId)
        except sqlite3.Error as err:
            log.error('insert_playlist_to_db: Sql error: {}'.format(err.args))
            return False
        
    con.close()

def get_playlist_from_db():
    global args
    
    con = sqlite3.connect(args.database_file)
    data = list()
    
    try:
        query = con.execute("SELECT id, title FROM playlist LIMIT 1")
        
        rows = query.fetchall()
        con.close()
    except sqlite3.Error as err:
        log.error('get_playlist_from_db: Sql error: {}'.format(err.args))
        return False
    
    for row in rows:
        data = '[ { "id": "%s", "title": "%s" } ]' % (row[0], row[1])
        data = json.loads(data)
    
    log.debug("get_playlist_from_db: results {}".format(json.dumps(data, indent=4)))
    log.info("get_playlist_from_db: count: %s" % len(data))
    
    return data

def insert_subscription_to_db(subscriptionId=None, subscriptionTitle=None, subscriptionTimestamp=None):
    global args
    
    con = sqlite3.connect(args.database_file)

    sql = 'INSERT OR REPLACE INTO subscription (id, title, timestamp) VALUES(?, ?, ?)'
    data = [(subscriptionId, subscriptionTitle, subscriptionTimestamp)]
    with con:
        try:
            con.executemany(sql, data)
            log.info("insert_subscription_to_db: Subscription %s with ID %s and timestamp: %s inserted into DB", subscriptionTitle, subscriptionId, subscriptionTimestamp)
        except sqlite3.Error as err:
            log.error('insert_subscription_to_db: Sql error: {}'.format(err.args))
            return False
        
    con.close()

def get_subscription_from_db(subscriptionId=None):
    global args

    con = sqlite3.connect(args.database_file)
    data = list()
    
    try:
        query = con.execute("SELECT `id`,`title`,`timestamp` FROM subscription WHERE `id` = ? LIMIT 1", (subscriptionId,))
        
        rows = query.fetchall()
        con.close()
    except sqlite3.Error as err:
        log.error('get_subscription_from_db: Sql error: {}'.format(err.args))
        return False
    
    for row in rows:
        data = '[ { "id": "%s", "title": "%s", "timestamp": "%s" } ]' % (row[0], row[1], row[2])
        data = json.loads(data)
    
    log.debug("get_subscription_from_db: results {}".format(json.dumps(data, indent=4)))
    log.info("get_subscription_from_db: count: %s" % len(data))
    
    return data

def exit_func():
    global errors
    global criticals
    global args
    global api_calls
    global subscriptions_processed
    global subscriptions_skipped
    global videos_added
    global videos_skipped
    
    log.info("Number of API calls made: %s", api_calls)
    log.info("Number of subscriptions processed: %s", subscriptions_processed)
    log.info("Number of subscriptions skiped: %s", subscriptions_skipped)
    log.info("Number of videos added to playlist: %s", videos_added)
    log.info("Number of videos skipped: %s", videos_skipped)
    log.info("Number of Errors: %s", errors)
    log.warning("------------------------[ Stop ]------------------------")

def authenticate(credentials_file=None, pickle_credentials=None, scopes=None):
    credentials = None

    if os.path.exists(pickle_credentials):
        log.info("authenticate: Loading credentials from %s" % pickle_credentials)
        with open(pickle_credentials, "rb") as token:
            credentials = pickle.load(token)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            log.info("authenticate: Refreshing access token")
            credentials.refresh(Request())
        else:
            log.info("authenticate: Fetching new tokens")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes=scopes)
            flow.run_local_server(port=8080, prompt='consent')
            credentials = flow.credentials

            with open(pickle_credentials, "wb") as f:
                log.info("authenticate: Saving credentials to pickle file")
                pickle.dump(credentials, f)
    return credentials

def get_subscriptions(credentials=None, nextPage=None):
    global errors
    global criticals
    global args
    global api_calls
    
    if args.local_json_files:
        subscriptions_response = json.loads(open('debug/subscriptions_list.json').read().strip())
    else:
        subscriptions_youtube = build("youtube", "v3", credentials=credentials)
        if nextPage is None:
            log.info("get_subscriptions: Getting all your subscription")
            subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical")
        else:
            subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical", pageToken=nextPage)

        try:
            subscriptions_response = subscriptions_request.execute()
            log.info("get_subscriptions: subscriptions_response is of type %s and items count %s" % (type(subscriptions_response),len(subscriptions_response)))
            api_calls = api_calls + 1
        except HttpError as err:
            errors = errors + 1
            if err.resp.status in criticals:
                log.critical("get_subscriptions: Critical error encountered! {}".format(err))
                exit_func()
                raise SystemExit(-1)
            else:
                log.error("get_subscriptions: Error: {}".format(err))
                
            return False
    
    sub_dict = subscriptions_response["items"]
    log.info("get_subscriptions: sub_dict is of type %s and items count %s" % (type(sub_dict),len(sub_dict)))
    
    if "nextPageToken" in subscriptions_response:
        log.info("get_subscriptions: nextPageToken detected!")
        nextPageToken = subscriptions_response.get("nextPageToken")
        subscriptions_response_nextpage = get_subscriptions(credentials=credentials, nextPage=nextPageToken)
        sub_dict_nextpage = subscriptions_response_nextpage
        sub_dict = [*sub_dict, *sub_dict_nextpage]
    
    if nextPage is None:
        log.warning("get_subscriptions: Total amount of subscriptions: %s (from youtube API)" % subscriptions_response["pageInfo"]["totalResults"])

    return sub_dict

def get_subscription_activity(credentials=None, channel=None, publishedAfter=None, nextPage=None):
    global errors
    global criticals
    global args
    global api_calls
    
    if args.local_json_files:
        activity_response = json.loads(open('debug/subscription_activity_list.json').read().strip())
    else:
        activity_youtube = build("youtube", "v3", credentials=credentials)
        if nextPage is None:
            log.info("get_subscription_activity: Getting activity for channelId: %s with publishedAfter set to \"%s\"" % (channel, publishedAfter))
            activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, publishedAfter=publishedAfter, uploadType="upload", channelId=channel)
        else:
            activity_request = activity_youtube.activities().list(part="snippet,contentDetails", maxResults=50, publishedAfter=publishedAfter, uploadType="upload", channelId=channel, pageToken=nextPage)
        
        try:
            activity_response = activity_request.execute()
            api_calls = api_calls + 1
        except HttpError as err:
            errors = errors + 1
            if err.resp.status in criticals:
                log.critical("get_subscription_activity: Critical error encountered! {}".format(err))
                exit_func()
                raise SystemExit(-1)
            else:
                log.error("get_subscription_activity: Error: {}".format(err))
            return False
        
    act_array = activity_response["items"]

    if "nextPageToken" in activity_response:
        nextPageToken = activity_response.get("nextPageToken")
        activity_response_nextpage = get_subscription_activity(credentials=credentials, channel=channel, nextPage=nextPageToken)
        act_array = [*act_array, *activity_response_nextpage]
    
    if nextPage is None:
        log.warning("get_subscription_activity: Total amount of activity for subscription: %s (from youtube API)" % activity_response["pageInfo"]["totalResults"])
    
    return act_array

def get_channel_id(credentials=None):
    global errors
    global criticals
    global args
    global api_calls
    
    if args.local_json_files:
        channel_response = json.loads(open('debug/channels_list.json').read().strip())
    else:
        log.info("get_channel_id: Geting list of channels")
        channel_youtube = build("youtube", "v3", credentials=credentials)
        channel_request = channel_youtube.channels().list(
            part="snippet,contentDetails",
            mine=True
        )
        try:
            channel_response = channel_request.execute()
            log.debug("get_channel_id: Respons: {}".format(json.dumps(channel_response, indent=4)))
            api_calls = api_calls + 1
        except HttpError as err:
            errors = errors + 1
            if err.resp.status in criticals:
                log.critical("get_channel_id: Critical error encountered! {}".format(err))
                exit_func()
                raise SystemExit(-1)
            else:
                log.error("get_channel_id: Error: {}".format(err))
            return False

    log.debug("get_channel_id: channel_response: {}".format(json.dumps(channel_response, indent=4)))
    channel_list = channel_response["items"]

    channel_list = jq.all('.[] | { "title": .snippet.title, "id": .id }', channel_list)
    log.debug("get_channel_id: Final channel list: {}".format(json.dumps(channel_list, indent=4)))
    log.info("get_channel_id: Final channel list count: %s" % len(channel_list))

    return channel_list

def get_user_playlists(credentials=None, channelId=None, nextPage=None):
    global errors
    global criticals
    global args
    global api_calls
    
    if args.local_json_files:
        user_playlists_response = json.loads(open('debug/user_playlists_list.json').read().strip())
    else:
        user_playlists_youtube = build("youtube", "v3", credentials=credentials)
        if nextPage is None:
            log.info("get_user_playlists: Getting all your playlists")
            user_playlists_request = user_playlists_youtube.playlists().list(part="snippet,contentDetails", channelId=channelId, maxResults=50)
        else:
            user_playlists_request = user_playlists_youtube.subscriptions().list(part="snippet,contentDetails", channelId=channelId, maxResults=50, pageToken=nextPage)

        try:
            user_playlists_response = user_playlists_request.execute()
            log.info("get_user_playlists: playlists_response is of type %s and items count %s" % (type(user_playlists_response),len(user_playlists_response)))
            api_calls = api_calls + 1
        except HttpError as err:
            errors = errors + 1
            if err.resp.status in criticals:
                log.critical("get_user_playlists: Critical error encountered! {}".format(err))
                exit_func()
                raise SystemExit(-1)
            else:
                log.error("get_user_playlists: Error: {}".format(err))
            return False
    
    plists_dict = user_playlists_response["items"]
    log.debug("get_user_playlists: plist_dict content: {}".format(json.dumps(plists_dict, indent=4)))
    
    if "nextPageToken" in user_playlists_response:
        nextPageToken = user_playlists_response.get("nextPageToken")
        user_playlists_response_nextpage = get_user_playlists(credentials=credentials, channelId=channelId, nextPage=nextPageToken)
        plists_dict_nextpage = user_playlists_response_nextpage
        plists_dict = [*plists_dict, *plists_dict_nextpage]
    
    if nextPage is None:
        log.warning("get_user_playlists: Total amount of playlists: %s (from youtube API)" % user_playlists_response["pageInfo"]["totalResults"])

    return plists_dict

def get_playlist(credentials=None, channelId=None, playlistId=None, nextPage=None):
    global errors
    global criticals
    global args
    global api_calls
    
    if args.local_json_files:
        playlist_response = json.loads(open('debug/user_playlist.json').read().strip())
    else:
        playlist_youtube = build("youtube", "v3", credentials=credentials)
        if nextPage is None:
            log.info("get_playlist: Getting playlist %s" % playlistId)
            playlist_request = playlist_youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlistId, maxResults=50)
        else:
            playlist_request = playlist_youtube.playlistItems().list(part="snippet,contentDetails", playlistId=playlistId, maxResults=50, pageToken=nextPage)

        try:
            playlist_response = playlist_request.execute()
            api_calls = api_calls + 1
        except HttpError as err:
            errors = errors + 1
            if err.resp.status in criticals:
                log.critical("get_playlist: Critical error encountered! {}".format(err))
                exit_func()
                raise SystemExit(-1)
            else:
                log.warning("get_playlist: httpError status: %s", err.resp.status)
            return False
        
        
    playlist_dict = playlist_response["items"]
        
    
    if "nextPageToken" in playlist_response:
        nextPageToken = playlist_response.get("nextPageToken")
        playlist_response_nextpage = get_playlist(credentials=credentials, channelId=channelId, playlistId=playlistId, nextPage=nextPageToken)
        playlist_dict_nextpage = playlist_response_nextpage
        playlist_dict = [*playlist_dict, *playlist_dict_nextpage]
    
    if nextPage is None:
        log.warning("get_playlist: Total amount of playlists: %s (from youtube API)" % playlist_response["pageInfo"]["totalResults"])
    
    return playlist_dict

def add_to_playlist(credentials=None, channelId=None, playlistId=None, subscriptionId=None, videoId=None, videoTitle=None):
    global errors
    global criticals
    global args
    global api_calls
    global videos_added
    
    if args.local_json_files:
        playlist_response = json.loads(open('debug/add_to_playlist_respons.json').read().strip())
    else:
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
            log.debug("add_to_playlist: Playlist Insert respons: {}".format(json.dumps(playlist_response, indent=4)))
            log.info("add_to_playlist: %s added to %s in position %s" % (videoId, playlistId, playlist_response["snippet"].get("position")))
            api_calls = api_calls + 1
            videos_added = videos_added + 1
        except HttpError as err:
            errors = errors + 1
            if err.resp.status in criticals:
                log.critical("add_to_playlist: Critical error encountered! {}".format(err))
                exit_func()
                raise SystemExit(-1)
            else:
                log.error("add_to_playlist: Error: {}".format(err))
            return False
        
        insert_video_to_db(videoId=videoId, timestamp=times["now_iso"], title=videoTitle, subscriptionId=subscriptionId)

    return playlist_response

def main():
    global times
    global errors
    global log
    global args
    global api_calls
    global videos_added
    global videos_skipped
    global subscriptions_processed
    global subscriptions_skipped
    
    args = get_arguments()
    
    setup_logger()
    
    log.warning("------------------------[ Start ]------------------------")
    log.debug("Arguments: {}".format(args))
    
    init_db()
        
    db_last_run = get_last_run()
            
    if args.published_after is not None:
        log.info("using --published-after value")
        published_after = str(args.published_after)
        
        published_after = datetime.fromisoformat(published_after)
        published_after_iso = published_after.isoformat()
    else: 
        published_after = None
        published_after_iso = None
    

    
    log.debug("now_iso: %s" % times["now_iso"])
    log.debug("yesterday_iso: %s" % times["yesterday_iso"])
    log.debug("oneyearback_iso: %s" % times["oneyearback_iso"])
    log.debug("db_last_run: %s" % db_last_run)
    log.debug("args.published_after: %s" % args.published_after)
    log.debug("published_after_iso: %s" % published_after_iso)

    credentials = authenticate(credentials_file=args.credentials_file, pickle_credentials=args.pickle_file, scopes=scopes)

    channel_from_db = get_channel_from_db()
    if not channel_from_db:
        channels = get_channel_id(credentials=credentials)
        channel = jq.all('.[]|select(all(.title; contains("%s")))| { "id": .id, "title": .title }' % (args.youtube_channel), channels)[0]
        insert_channel_to_db(channelId=channel["id"], channelTitle=channel["title"])
    else:
        channel = channel_from_db[0]
    
    log.info("Channel selected: %s (%s)" % (channel["title"], channel["id"]))

    playlist_from_db = get_playlist_from_db()
    if not playlist_from_db:
        user_playlists = get_user_playlists(credentials=credentials, channelId=channel["id"])
        user_playlists_refined = jq.all('.[] | { "title": .snippet.title, "id": .id }', user_playlists)
        log.info("Playlists on channel: %s" % len(user_playlists_refined))
        user_playlist = jq.all('.[]|select(all(.title; contains("%s")))|{ "id": .id, "title": .title }' % (args.youtube_playlist), user_playlists_refined)[0]
        insert_playlist_to_db(playlistId=user_playlist["id"], playlistTitle=user_playlist["title"])
    else:
        user_playlist = playlist_from_db[0]
    
    log.info("Playlist selected: %s (%s)" % (user_playlist["title"], user_playlist["id"]))
    
    subscriptions = get_subscriptions(credentials=credentials)
    subscriptions_refined = jq.all('.[] | { "title": .snippet.title, "id": .snippet.resourceId.channelId }', subscriptions)

    log.debug("Last script run: %s" % (db_last_run))
    log.debug("Subscriptions: "+json.dumps(subscriptions_refined, indent=4, sort_keys=True))
    
    log.info("Subscriptions on selected channel: %s" % len(subscriptions_refined))

    s=0
    su=0
    for subs in subscriptions_refined:
        su=su + 1
        log.info("Processing subscription number %s  \"%s\" (%s), but sleeping for %s seconds first" % (su, subs["title"], subs["id"], args.youtube_subscription_sleep))
        subscription_from_db = get_subscription_from_db(subscriptionId=subs["id"])
        log.debug("subscription_from_db: {}".format(subscription_from_db))
        log.debug("subscription_from_db: timestamp: %s",subscription_from_db[0].get("timestamp"))
        
        if not subscription_from_db:
            insert_subscription_to_db(subscriptionId=subs["id"], subscriptionTitle=subs["title"], subscriptionTimestamp=times["oneyearback_iso"])
            subscription_last_run = times["oneyearback_iso"]
            log.info("subscription_last_run was empty in DB, so setting it to one year back: %s", subscription_last_run)
        else:
            if args.published_after is not None:
                log.info("using --published-after value")
                subscription_last_run = published_after_iso
            else:
                log.info("NOT using --published-after value")
                if not subscription_from_db[0].get("timestamp") and subscription_from_db[0].get("timestamp") == None:
                    subscription_last_run = times["oneyearback_iso"]
                    log.info("subscription_last_run was empty in DB, and --published-after  was not set. Setting it to one year back: %s", subscription_last_run)
                else:
                    subscription_last_run = subscription_from_db[0].get("timestamp")
                    log.info("subscription_last_run had value in DB: %s", subscription_last_run)
                    
        reprocess_days = times["now"] - timedelta(days=args.reprocess_days)
        
        if not datetime.fromisoformat(subscription_last_run) < reprocess_days:
            subscriptions_skipped = subscriptions_skipped + 1
            log.warning("This subscription was processed within %s. Skipping for now", reprocess_days)
            continue
        
        log.info("This subscription was last processed %s" % (datetime.fromisoformat(subscription_last_run).strftime(times["date_format"])))
        
        sub_activity = get_subscription_activity(credentials=credentials, channel=subs["id"], publishedAfter=subscription_last_run)
        sub_activity_refined = jq.all('.[] | select(all(.snippet.type; contains("upload"))) | { "title": .snippet.title, "videoId": .contentDetails.upload.videoId, "publishedAt": .snippet.publishedAt }', sub_activity) if sub_activity != False else []
        sub_activity_refined.sort(key = lambda x:x['publishedAt'], reverse=True) if sub_activity != False else []

        log.debug("sub_activity_refined: activity total count: %s" % (len(sub_activity_refined)))
        log.debug("sub_activity_refined: {}".format(json.dumps(sub_activity_refined, indent=4)))
        
        a=0
        for activity in sub_activity_refined:
            log.info("%s - Processing %s (%s)" % (subs["title"], activity["title"], activity["videoId"]))
            
            results = get_video_from_db(videoId=activity["videoId"], subscriptionId=subs["id"])
            
            if len(results) == 0:
                add_to_playlist(credentials=credentials, channelId=channel["id"], playlistId=user_playlist["id"], subscriptionId=subs["id"], videoId=str(activity["videoId"]), videoTitle=str(activity["title"]))
                time.sleep(args.youtube_playlist_sleep)
            else:
                videos_skipped = videos_skipped + 1
                log.warning("%s - Video %s (%s) already in database or playlist %s" % (subs["title"], activity["title"], activity["videoId"], user_playlist["title"]))

            a=a + 1
            if args.youtube_activity_limit != 0 and a >= args.youtube_activity_limit:
                log.error("%s - YouTube activity limit reached! exiting activity loop" % (subs["title"]))
                break
        
        insert_subscription_to_db(subscriptionId=subs["id"], subscriptionTitle=subs["title"], subscriptionTimestamp=times["now_iso"])
        s=s + 1
        subscriptions_processed = s
        if args.youtube_subscription_limit != 0 and s >= args.youtube_subscription_limit:
            log.error("YouTube subscription limit reached! exiting subscription loop")
            break
        if len(sub_activity_refined) > 0:
            time.sleep(args.youtube_subscription_sleep)
    
    if errors == 0:
        set_last_run(timestamp=times["now_iso"])
    else:
        log.error("Last run timestamp not set because of errors! (%s)" % errors)
    

if __name__ == '__main__':
    try:
        main()
        exit_func()
    except KeyboardInterrupt:
        exit_func()
