from datetime import timezone, datetime, timedelta
from fuzzywuzzy import fuzz
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from logging.handlers import RotatingFileHandler
from tzlocal import get_localzone
from configargparse import ArgumentParser
import jq
import json
import logging
import os
import pickle
import re
import sqlite3
import time

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

api_calls=0
errors=0
subscriptions_processed=0
subscriptions_skipped=0
videos_added=0
videos_skipped_count=0
dist_count=0
dist_sum=0

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
ignore_subscriptions_list = list()

def get_arguments():
    parser = ArgumentParser(description='Add latest activity from your subscriptions on YouTube to a playlist of your choice.', default_config_files=['/etc/ysl/config.yml', '~/.ysl/config.yml'])
    parser.add('--config',                           env_var='CONFIG', is_config_file=True,                                      help='Path to config file in yaml format')
    parser.add('--pickle-file',                      env_var="PICKLE_FILE",                      default='credentials.pickle',   help='File to store access token once authenticated')
    parser.add('--credentials-file',                 env_var="CREDENTIALS_FILE",                 default='client_secret.json',   help='JSON file with credentials to oAuth2 account (https://console.developers.google.com/apis/credentials)')
    parser.add('--database-file',                    env_var="DATABASE_FILE",                    default='my.db',                help='Location of sqlite database file. Will be created if assigned file does not exists')
    parser.add('--local-json-files',                 env_var="LOCAL_JSON_FILES", action="store_true",                            help='Use local JSON files for testing instead of the YouTube API')
    parser.add('--compare-distance-number',          env_var="COMPARE_DISTANCE_NUMBER",          default=80, type=int,           help="Levenstein number to compare difference betwene existing videos and new to avoid adding similar titled videos.")
    parser.add('--published-after',                  env_var="PUBLISHED_AFTER",                  default=None,                   help='Add videos only after this timestamp. Timestamp in ISO8601 (YYYY-MM-DDThh:mm:ss.sZ) format.')
    parser.add('--reprocess-days',                   env_var="REPROCESS_DAYS",                   default=2, type=int,            help='Amount of days before subscription will be processed again. will skip subscription if within reprocess days.')
    parser.add('--youtube-channel',                  env_var="YOUTUBE_CHANNEL",                  default='',                     help='Name of your channel to add to playlist.')
    parser.add('--youtube-playlist',                 env_var="YOUTUBE_PLAYLIST",                 default='',                     help='Name of playlist to add videos to.')
    parser.add('--youtube-activity-limit',           env_var="YOUTUBE_ACTIVITY_LIMIT",           default='0', type=int,          help='How much activity to process pr. subscription')
    parser.add('--youtube-subscription-limit',       env_var="YOUTUBE_SUBSCRIPTION_LIMIT",       default='0', type=int,          help='How may subscriptions to process')
    parser.add('--youtube-subscription-ignore-file', env_var="YOUTUBE_SUBSCRIPTION_IGNORE_FILE", default=".subscription-ignore", help="File with newline separated list of subscriptions (channel names) to ignore when proccessing")
    parser.add('--youtube-video-ignore-file',        env_var="YOUTUBE_VIDEO_IGNORE_FILE",        default=".video-ignore",        help="File with newline separated list of video-ids to ignore when proccessing")
    parser.add('--youtube-words-ignore-file',        env_var="YOUTUBE_WORDS_IGNORE_FILE",        default=".ignore-words",        help="File with newline separated list of words to ignore when proccessing")
    parser.add('--youtube-playlist-sleep',           env_var="YOUTUBE_PLAYLIST_SLEEP",           default='10', type=int,         help='how long to wait betwene playlist API insert-calls')
    parser.add('--youtube-subscription-sleep',       env_var="YOUTUBE_SUBSCRIPTION_SLEEP",       default='30', type=int,         help='how long to wait betwene subscription API calls')
    parser.add('--youtube-minimum-length',           env_var="YOUTUBE_MINIMUM_LENGTH",           default='0s',                   help='Minimum lenght of tracks to add. in format "3m" or "6h" or "1d" or "0s" for no minimum')
    parser.add('--youtube-maximum-length',           env_var="YOUTUBE_MAXIMUM_LENGTH",           default='0s',                   help='Maximum lenght of tracks to add. in format "3m" or "6h" or "1d" or "0s" for no maximum')
    parser.add('--log-level',                        env_var="LOG_LEVEL",                        default='warning',              help='Set loglevel. debug,info,warning or error. when debug is selected, no database changes will be made')
    parser.add('--log-file',                         env_var="LOG_FILE", dest='log_file',        default='stream',               help='file to cast logs to. if you want all output to stdout use "stream" as value. steam is default value')
    
    return parser.parse_args()

def time_to_seconds(time_str):
    # Regular expression to match time expressions like "3m" or "6h"
    pattern = r'(\d+)([smhd])'
    
    # Dictionary to map units to seconds
    units = {
        's': 1,    # 1 minute = 60 seconds
        'm': 60,    # 1 minute = 60 seconds
        'h': 3600,  # 1 hour = 3600 seconds
        'd': 86400  # 1 day = 86400 seconds
    }
    
    # Find all matches in the input string
    matches = re.findall(pattern, time_str)
    
    total_seconds = 0
    
    # Iterate through matches and calculate the total seconds
    for match in matches:
        value, unit = int(match[0]), match[1]
        total_seconds += value * units[unit]
    
    return total_seconds

def iso8601_to_seconds(duration_str):
    # Regular expression to match time components
    pattern = r'P(?:(?P<days>\d+)D)?T?(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?'
    
    # Match the components in the duration string
    match = re.match(pattern, duration_str)
    
    if not match:
        raise ValueError("Invalid ISO 8601 duration format")
    
    # Extract the matched components and convert them to seconds
    days = int(match.group('days')) if match.group('days') else 0
    hours = int(match.group('hours')) if match.group('hours') else 0
    minutes = int(match.group('minutes')) if match.group('minutes') else 0
    seconds = int(match.group('seconds')) if match.group('seconds') else 0
    
    total_seconds = (
        days * 24 * 3600 +  # Convert days to seconds
        hours * 3600 +      # Convert hours to seconds
        minutes * 60 +      # Convert minutes to seconds
        seconds             # Add seconds
    )
    
    return total_seconds

def setup_logger():  
    global api_calls
    global args
    global errors
    global log
    global loggFormat
    global times

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

def db_connect(database_file=None):
    global args

    try:
        con = sqlite3.connect(database_file)

        return con
    except sqlite3.Error as err:
        log.error('db_connect: Error: {}'.format(err.args))
        return False

def init_db():
    global args
    
    con = db_connect(args.database_file)
    
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
    
    con = db_connect(args.database_file)
    
    log.debug("get_last_run: Checking last run in DB")
    with con:
        try:
            query = con.execute("SELECT timestamp FROM last_run WHERE id = 1 LIMIT 1")
        except sqlite3.Error as err:
            log.error('get_last_run: Sql error: {}'.format(err.args))
            return False
        
    data = query.fetchall()
    
    con.close()
    
    log.debug("get_last_run: Last run in DB %s" % data[0])
    
    return data[0]

def set_last_run(timestamp=None):
    global args
    
    if args.log_level != "debug":
        con = db_connect(args.database_file)
        
        sql = 'INSERT OR REPLACE INTO last_run (id, timestamp) VALUES(?, ?)'
        data = [(1, timestamp)]
        with con:
            try:
                con.executemany(sql, data)
                log.debug("set_last_run: Last run updated in DB: %s" % timestamp)
            except sqlite3.Error as err:
                log.error('set_last_run: Sql error: {}'.format(err.args))
                return False
        
        con.close()
    else:
        log.debug("set_last_run: NOT REALY! Last run updated in DB: %s" % (timestamp))

def distance(string1, string2):
    global dist_count
    global dist_sum

    if string1 is None or string2 is None:
        log.error('func: distance: string1 can not be nothing')
        return -1
    if string1 is string2:
        return 0
    if len(string1) == 0 or len(string2) == 0:
       log.error('func: distance: string1s content length is 0')
       return -1

    distance = fuzz.ratio(string1.lower(), string2.lower())

    return distance

def normalize_string(string):    
    pattern = r'[^a-zA-Z0-9\-\_]+'
    
    new_string = re.sub(pattern, ' ', string)
    
    # log.debug("normalize_string: old [%s] new [%s] ", string, new_string)
    
    return new_string

def compare_title_with_db_title(new_title=None):
    global args

    con = db_connect(args.database_file)

    new_title = normalize_string(new_title)
    
    log.debug("compare_title_with_db_title: Checking if %s or similar is in DB", new_title)
    with con:
        try:
            query = con.execute('SELECT videoId,title FROM videos')
        except sqlite3.Error as err:
            log.error('compare_title_with_db_title: Sql error: {}'.format(err.args))
            return False
        
    rows = query.fetchall()
    log.debug("compare_title_with_db_title: count on rows: %s" % len(rows))
    con.close()
    
    for row in rows:
        existing_title_before = row[1]
        existing_title = normalize_string(existing_title_before)

        dist = distance(existing_title, new_title)

        if dist > args.compare_distance_number:
            log.debug("compare_title_with_db_title: dist result: %s which is less than %s", dist, args.compare_distance_number)
            log.debug("compare_title_with_db_title: %s was to close %s (distance: %s <= %s). Not adding to playlist", new_title, existing_title, dist, args.compare_distance_number)
            return False

    return True

def insert_video_to_db(videoId=None, timestamp=None, title=None, subscriptionId=None):
    global args
    
    if args.log_level != "debug":
        con = db_connect(args.database_file)
        
        sql = 'INSERT OR REPLACE INTO videos (videoId, timestamp, title, subscriptionId) VALUES(?, ?, ?, ?)'
        data = [(videoId, timestamp, title, subscriptionId)]
        with con:
            try:
                con.executemany(sql, data)
            except sqlite3.Error as err:
                log.error('insert_video_to_db: Sql error: {}'.format(err.args))
                return False
        
        log.debug("insert_video_to_db: Video %s (%s) from %s added to database" % (title, videoId, subscriptionId))

        con.close()
    else:
        log.debug("insert_video_to_db: NOT REALY!! Video %s (%s) from %s added to database" % (title, videoId, subscriptionId))

def get_videoId_from_db(videoId=None):
    global args
    
    data = list()
    con = db_connect(args.database_file)
    
    log.debug("get_videoId_from_db: Checking %s in database" % (videoId))
    try:
        query = con.execute('SELECT videoId FROM videos WHERE videoId=\"%s\" LIMIT 1' % (videoId))
    except sqlite3.Error as err:
        log.error('get_videoId_from_db: Sql error: {}'.format(err.args))
        
        return 0
    
    rows = query.fetchall()
    log.debug("get_videoId_from_db: count of rows in DB: %s" % len(rows))
    con.close()
    
    if len(rows) > 0:
        for row in rows:
            data = "{ \"id\": \"%s\" }" % row[0]
            log.debug("get_videoId_from_db: content of data before json.loads(): {}".format(data))
            try:
                data = json.loads(data)
            except json.JSONDecodeError as err:
                log.error("get_videoId_from_db: json error: {}".format(err))
                return 0
        
        return len(rows)
    else:
        return 0

def insert_channel_to_db(channelId=None, channelTitle=None):
    global args
    
    if not args.local_json_files:
        con = db_connect(args.database_file)

        sql = 'INSERT OR REPLACE INTO channel (id, title) VALUES(?, ?)'
        data = [(channelId, channelTitle)]
        with con:
            try:
                con.executemany(sql, data)
                log.debug("insert_channel_to_db: Channel %s with ID %s inserted into DB", channelId, channelTitle)
            except sqlite3.Error as err:
                log.error('insert_channel_to_db: Sql error: {}'.format(err.args))
                return False
            
        con.close()
        
        log.debug("insert_channel_to_db: Channel %s (%s) inserted into DB", channelTitle, channelId)
        return True
    else:
        log.debug("insert_channel_to_db: INSERT OR REPLACE INTO channel (id, title) VALUES(%s, %s)", channelId, channelTitle)
        return True

def get_channel_from_db():
    global args
    
    con = db_connect(args.database_file)
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
    log.debug("get_channel_from_db: count: %s" % len(data))
    
    return data

def insert_playlist_to_db(playlistId=None, playlistTitle=None):
    global args
    
    con = db_connect(args.database_file)

    sql = 'INSERT OR REPLACE INTO playlist (id, title) VALUES(?, ?)'
    data = [(playlistId, playlistTitle)]
    with con:
        try:
            con.executemany(sql, data)
            log.debug("insert_playlist_to_db: Channel %s with ID %s inserted into DB", playlistTitle, playlistId)
        except sqlite3.Error as err:
            log.error('insert_playlist_to_db: Sql error: {}'.format(err.args))
            return False
        
    con.close()

def get_playlist_from_db():
    global args
    
    con = db_connect(args.database_file)
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
    log.debug("get_playlist_from_db: count: %s" % len(data))
    
    return data

def insert_subscription_to_db(subscriptionId=None, subscriptionTitle=None, subscriptionTimestamp=None):
    global args
    
    if args.log_level != "debug":
        con = db_connect(args.database_file)

        sql = 'INSERT OR REPLACE INTO subscription (id, title, timestamp) VALUES(?, ?, ?)'
        data = [(subscriptionId, subscriptionTitle, subscriptionTimestamp)]
        with con:
            try:
                con.executemany(sql, data)
                log.debug("insert_subscription_to_db: Subscription %s with ID %s and timestamp: %s inserted into DB", subscriptionTitle, subscriptionId, subscriptionTimestamp)
            except sqlite3.Error as err:
                log.error('insert_subscription_to_db: Sql error: {}'.format(err.args))
                return False
        
        con.close()
    else:
        log.debug("insert_subscription_to_db: NOT REALY!! Subscription %s with ID %s and timestamp: %s inserted into DB", subscriptionTitle, subscriptionId, subscriptionTimestamp)        

def get_subscription_from_db(subscriptionId=None):
    global args

    con = db_connect(args.database_file)
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
    log.debug("get_subscription_from_db: count: %s" % len(data))
    
    return data

def get_subscription_ignore_list(subscription_ignore_file=None):
    global args
    
    log.debug("get_subscription_ignore_list: loading subscription ignore-file '%s'", subscription_ignore_file)

    try:
        ignore_file = open(subscription_ignore_file, "r")

        ignore_data = ignore_file.read()
        ignore_list = ignore_data.split("\n")
        ignore_file.close()
        log.debug("get_subscription_ignore_list: ignore list loaded from subscription ignore-file {}".format(ignore_list))
        log.debug("get_subscription_ignore_list: Ignore file loaded successfully")

    except:
        log.error("get_subscription_ignore_list: could not open subscription ignore-file '%s'", subscription_ignore_file)
        ignore_list=[]

    return ignore_list

def get_video_ignore_list(video_ignore_file=None):
    global args

    log.debug("get_video_ignore_list: loading video ignore-file '%s'", video_ignore_file)

    try:
        ignore_file = open(video_ignore_file, "r")

        ignore_data = ignore_file.read()
        ignore_list = ignore_data.split("\n")
        ignore_file.close()
        log.debug("get_video_ignore_list: ignore list loaded from video ignore-file {}".format(ignore_list))
        log.debug("get_video_ignore_list: Video ignore file loaded successfully")

    except:
        log.error("get_video_ignore_list: could not open video ignore-file '%s'", video_ignore_file)
        ignore_list=[]

    return ignore_list

def get_word_ignore_list(word_ignore_file=None):
    global args

    log.debug("get_word_ignore_list: loading word ignore-file '%s'", word_ignore_file)

    try:
        ignore_file = open(word_ignore_file, "r")

        ignore_data = ignore_file.read()
        ignore_list = ignore_data.split("\n")
        ignore_file.close()
        log.debug("get_word_ignore_list: ignore list loaded from video ignore-file {}".format(ignore_list))
        log.debug("get_word_ignore_list: Video ignore file loaded successfully")

    except:
        log.error("get_word_ignore_list: could not open video ignore-file '%s'", word_ignore_file)
        ignore_list=[]

    return ignore_list

def exit_func():
    global errors
    global criticals
    global args
    global api_calls
    global subscriptions_processed
    global subscriptions_skipped
    global videos_added
    global videos_skipped_count

    try:
        avg_dist = (dist_sum // dist_count)
    except:
        avg_dist = 0

    
    log.debug("Number of API calls made: %s", api_calls)
    log.debug("Number of subscriptions processed: %s", subscriptions_processed)
    log.debug("Number of subscriptions skiped: %s", subscriptions_skipped)
    log.debug("Number of videos added to playlist: %s", videos_added)
    log.debug("Number of videos skipped: %s", videos_skipped_count)
    log.debug("Number of Errors: %s", errors)
    log.debug("Number of Distances calculated: %s", dist_count)
    log.debug("Total distance acumulated: %s", dist_sum)
    log.debug("Avarage distance: %s", avg_dist)

def authenticate(credentials_file=None, pickle_credentials=None, scopes=None):
    credentials = None

    if os.path.exists(pickle_credentials):
        log.debug("authenticate: Loading credentials from %s" % pickle_credentials)
        with open(pickle_credentials, "rb") as token:
            credentials = pickle.load(token)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            log.debug("authenticate: Refreshing access token")
            credentials.refresh(Request())
        else:
            log.debug("authenticate: Fetching new tokens")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, scopes=scopes)
            flow.run_local_server(port=8080, prompt='consent')
            credentials = flow.credentials

            with open(pickle_credentials, "wb") as f:
                log.debug("authenticate: Saving credentials to pickle file")
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
            log.debug("get_subscriptions: Getting all your subscription")
            subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical")
        else:
            subscriptions_request = subscriptions_youtube.subscriptions().list(part="snippet,contentDetails", maxResults=50, mine=True, order="alphabetical", pageToken=nextPage)

        try:
            subscriptions_response = subscriptions_request.execute()
            log.debug("get_subscriptions: subscriptions_response is of type %s and items count %s" % (type(subscriptions_response),len(subscriptions_response)))
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
    log.debug("get_subscriptions: sub_dict is of type %s and items count %s" % (type(sub_dict),len(sub_dict)))
    
    if "nextPageToken" in subscriptions_response:
        log.debug("get_subscriptions: nextPageToken detected!")
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
            log.debug("get_subscription_activity: Getting activity for channelId: %s" % channel)
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

def get_video_duration(credentials=None, videoId=None):
    global errors
    global criticals
    global args
    global api_calls

    if args.local_json_files:
        activity_response = json.loads(open('debug/video.json').read().strip())
    else:
        activity_youtube = build("youtube", "v3", credentials=credentials)
        
        video_request = activity_youtube.videos().list(part="contentDetails", maxResults=50, id=videoId)
            
        try:
            video_response = video_request.execute()
            api_calls = api_calls + 1
        except HttpError as err:
            errors = errors + 1
            if err.resp.status in criticals:
                log.critical("get_video_duration: Critical error encountered! {}".format(err))
                exit_func()
                raise SystemExit(-1)
            else:
                log.error("get_video_duration: Error: {}".format(err))
            return False
    
    if "duration" in video_response["items"][0]["contentDetails"]:
        video_time = iso8601_to_seconds(video_response["items"][0]["contentDetails"]["duration"])
        log.debug("get_video_duration: %s duration is %s" % (videoId, video_time))
    else:
        video_time = 0
        log.warning("get_video_duration: %s has no duration" % videoId)
    
    return video_time

def get_channel_id(credentials=None):
    global errors
    global criticals
    global args
    global api_calls
    
    if args.local_json_files:
        channel_response = json.loads(open('debug/channels_list.json').read().strip())
    else:
        log.debug("get_channel_id: Geting list of channels")
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
    log.debug("get_channel_id: Final channel list count: %s" % len(channel_list))

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
            log.debug("get_user_playlists: Getting all your playlists")
            user_playlists_request = user_playlists_youtube.playlists().list(part="snippet,contentDetails", channelId=channelId, maxResults=50)
        else:
            user_playlists_request = user_playlists_youtube.subscriptions().list(part="snippet,contentDetails", channelId=channelId, maxResults=50, pageToken=nextPage)

        try:
            user_playlists_response = user_playlists_request.execute()
            log.debug("get_user_playlists: playlists_response is of type %s and items count %s" % (type(user_playlists_response),len(user_playlists_response)))
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
        log.debug("get_user_playlists: Total amount of playlists: %s (from youtube API)" % user_playlists_response["pageInfo"]["totalResults"])

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
            log.debug("get_playlist: Getting playlist %s" % playlistId)
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
        log.debug("get_playlist: Total amount of playlists: %s (from youtube API)" % playlist_response["pageInfo"]["totalResults"])
    
    return playlist_dict

def add_to_playlist(credentials=None, channelId=None, playlistId=None, subscriptionId=None, videoId=None, videoTitle=None, videoType=None):
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
        if args.log_level != "debug":
            try:
                playlist_response = playlist_request.execute()
                log.debug("add_to_playlist: Playlist Insert respons: {}".format(json.dumps(playlist_response, indent=4)))
                log.info("add_to_playlist: %s (type: %s) added to %s in position %s" % (videoId, videoType, playlistId, playlist_response["snippet"].get("position")))
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
        else:
            log.debug("add_to_playlist: NOT REALY!!! %s (type: %s) added to %s in position None" % (videoId, videoType, playlistId))
            playlist_response = None
        
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
    global videos_skipped_count
    global subscriptions_processed
    global subscriptions_skipped
    
    args = get_arguments()
    
    setup_logger()
    
    log.debug("Arguments: {}".format(args))
    
    init_db()
        
    db_last_run = get_last_run()
            
    if args.published_after is not None:
        log.debug("using --published-after value")
        published_after = str(args.published_after)
        
        published_after = datetime.fromisoformat(published_after)
        published_after_iso = published_after.isoformat()
    else: 
        published_after = None
        published_after_iso = None

    youtube_minimum_length = time_to_seconds(args.youtube_minimum_length)
    youtube_maximum_length = time_to_seconds(args.youtube_maximum_length)

    
    log.debug("now_iso: %s" % times["now_iso"])
    log.debug("yesterday_iso: %s" % times["yesterday_iso"])
    log.debug("oneyearback_iso: %s" % times["oneyearback_iso"])
    log.debug("db_last_run: %s" % db_last_run)
    log.debug("args.published_after: %s" % args.published_after)
    log.debug("published_after_iso: %s" % published_after_iso)
    log.debug("youtube_minimum_length: %s" % youtube_minimum_length)
    log.debug("youtube_maximum_length: %s" % youtube_maximum_length)

    credentials = authenticate(credentials_file=args.credentials_file, pickle_credentials=args.pickle_file, scopes=scopes)

    channel_from_db = get_channel_from_db()
    if not channel_from_db:
        channels = get_channel_id(credentials=credentials)
        channel = jq.all('.[]|select(all(.title; contains("%s")))| { "id": .id, "title": .title }' % (args.youtube_channel), channels)[0]
        insert_channel_to_db(channelId=channel["id"], channelTitle=channel["title"])
    else:
        channel = channel_from_db[0]
    
    log.debug("Channel selected: %s (%s)" % (channel["title"], channel["id"]))

    playlist_from_db = get_playlist_from_db()
    if not playlist_from_db:
        user_playlists = get_user_playlists(credentials=credentials, channelId=channel["id"])
        user_playlists_refined = jq.all('.[] | { "title": .snippet.title, "id": .id }', user_playlists)
        log.debug("Playlists on channel: %s" % len(user_playlists_refined))
        user_playlist = jq.all('.[]|select(all(.title; contains("%s")))|{ "id": .id, "title": .title }' % (args.youtube_playlist), user_playlists_refined)[0]
        insert_playlist_to_db(playlistId=user_playlist["id"], playlistTitle=user_playlist["title"])
    else:
        user_playlist = playlist_from_db[0]
    
    log.debug("Playlist selected: %s (%s)" % (user_playlist["title"], user_playlist["id"]))
    
    subscriptions = get_subscriptions(credentials=credentials)
    subscriptions_refined = jq.all('.[] | { "title": .snippet.title, "id": .snippet.resourceId.channelId }', subscriptions)

    ignore_subscriptions_list = get_subscription_ignore_list(args.youtube_subscription_ignore_file)
    ignore_video_list = get_video_ignore_list(args.youtube_video_ignore_file)
    ignore_word_list = get_word_ignore_list(args.youtube_words_ignore_file)
    log.debug("Subscriptions on ignore-list: %s", len(ignore_subscriptions_list))
    log.debug("Subscripotions on ignore-list: {}".format(ignore_subscriptions_list))
    log.debug("Videos on ignore-list: %s", len(ignore_video_list))
    log.debug("Videos on ignore-list: {}".format(ignore_video_list))
    log.debug("Words on ignore-list: %s", len(ignore_word_list))
    log.debug("Words on ignore-list: {}".format(ignore_word_list))

    log.info("Last script run: %s" % (db_last_run))
    log.debug("Subscriptions: "+json.dumps(subscriptions_refined, indent=4, sort_keys=True))
    
    log.debug("Subscriptions on selected channel: %s" % len(subscriptions_refined))

    s=0
    for subs in subscriptions_refined:

        if subs["title"] in ignore_subscriptions_list:
            log.warning("Subscription %s is in ignore list. Skipping", subs["title"])
            continue

        log.debug("Processing subscription %s (%s), but sleeping for %s seconds first" % (subs["title"], subs["id"], args.youtube_subscription_sleep))
        subscription_from_db = get_subscription_from_db(subscriptionId=subs["id"])
        
        if len(subscription_from_db) == 0:
            log.debug("Adding subscription %s to db", subs["title"])
            insert_subscription_to_db(subscriptionId=subs["id"], subscriptionTitle=subs["title"], subscriptionTimestamp=times["oneyearback_iso"])
            subscription_last_run = times["oneyearback_iso"]
            log.debug("subscription_last_run was empty in DB, so setting it to one year back: %s", subscription_last_run)
        else:
            log.debug("subscription_from_db: {}".format(subscription_from_db))
            log.debug("subscription_from_db: timestamp: %s",subscription_from_db[0].get("timestamp"))
            if args.published_after is not None:
                log.debug("using --published-after value")
                subscription_last_run = published_after_iso
            else:
                log.debug("NOT using --published-after value")
                if not subscription_from_db[0].get("timestamp") and subscription_from_db[0].get("timestamp") == None:
                    subscription_last_run = times["oneyearback_iso"]
                    log.debug("subscription_last_run was empty in DB, and --published-after  was not set. Setting it to one year back: %s", subscription_last_run)
                else:
                    subscription_last_run = subscription_from_db[0].get("timestamp")
                    log.debug("subscription_last_run had value in DB: %s", subscription_last_run)
                    
        reprocess_days = times["now"] - timedelta(days=args.reprocess_days)
        
        if not datetime.fromisoformat(subscription_last_run) < reprocess_days:
            subscriptions_skipped = subscriptions_skipped + 1
            log.warning("This subscription was processed within %s. Skipping for now", reprocess_days)
            continue
        
        log.debug("This subscription was last processed %s" % (datetime.fromisoformat(subscription_last_run).strftime(times["date_format"])))
        
        sub_activity_refined = []
        sub_activity = get_subscription_activity(credentials=credentials, channel=subs["id"], publishedAfter=subscription_last_run)

        for item in sub_activity:
            if item["snippet"]["type"] in ["upload", "playlistItem"]:
                sub_activity_refined.append(
                    {
                        "title": item["snippet"]["title"],
                        "videoId": (item["snippet"]["type"] == "upload") and item["contentDetails"]["upload"]["videoId"] or item["contentDetails"]["playlistItem"]["resourceId"]["videoId"],
                        "publishedAt": item["snippet"]["publishedAt"],
                        "type": item["snippet"]["type"]
                    }
                )
        
        sub_activity_refined.sort(key = lambda x:x['publishedAt'], reverse=True) if sub_activity != False else []

        log.debug("sub_activity_refined: activity total count: %s" % (len(sub_activity_refined)))
        log.debug("sub_activity_refined: {}".format(json.dumps(sub_activity_refined, indent=4)))
        
        a=0
        for activity in sub_activity_refined:
            log.debug("%s - Processing %s (%s)" % (subs["title"], activity["title"], activity["videoId"]))
            minimum_length = False
            maximum_length = False

            for word in ignore_word_list:
                regex = re.compile(r'\b%s\b' % word, re.IGNORECASE)
                if regex.search(activity["title"]):
                    log.warning("Video %s (%s) title contains word '%s' which is in word-ignore list. Skipping", activity["title"], activity["videoId"], word)
                    videos_skipped.append(f"activity['title'] contains word '{word}' which is in word-ignore list. Skipping")
                    videos_skipped_count = videos_skipped_count + 1
                    continue
            
            results = get_videoId_from_db(videoId=activity["videoId"])
            if activity["videoId"] in ignore_video_list:
                log.warning("Video %s is in video ignore list. Skipping", activity["videoId"])
                continue
            
            if results == 0:
                compare = compare_title_with_db_title(activity["title"])
                log.debug("COMPARE RESULTS: %s" % compare)
                
                if compare:
                    video_length = get_video_duration(credentials=credentials, videoId=activity["videoId"])

                    if youtube_minimum_length != 0 and video_length >= youtube_minimum_length:

                        if youtube_maximum_length != 0 and video_length <= youtube_maximum_length:
                            add_to_playlist(credentials=credentials, channelId=channel["id"], playlistId=user_playlist["id"], subscriptionId=subs["id"], videoId=str(activity["videoId"]), videoTitle=str(activity["title"]), videoType=str(activity["type"]))
                            time.sleep(args.youtube_playlist_sleep)
                        
                        else:
                            videos_skipped_count = videos_skipped_count + 1
                            log.warning("Video maximum lenght is to long (duration: %s, maximum length: %s)" % (video_length, youtube_maximum_length))
                            continue
                        
                    else:
                        videos_skipped_count = videos_skipped_count + 1
                        log.warning("Video minimum lenght to short (duration: %s, minimum length: %s)" % (video_length, youtube_minimum_length))
                        continue
                    
                else:
                    videos_skipped_count = videos_skipped_count + 1
                    log.warning("COMPARE: %s - Video %s (%s) already in database or playlist" % (subs["title"], activity["title"], activity["videoId"]))
                    continue
            else:
                videos_skipped_count = videos_skipped_count + 1
                log.warning("get_videoId_from_db: %s - Video %s (%s) already in database or playlist %s" % (subs["title"], activity["title"], activity["videoId"], user_playlist["title"]))
                continue

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
        if len(sub_activity_refined) > 0:
            set_last_run(timestamp=times["now_iso"])
        else:
            log.warning("Last run timestamp not set because of no activity. (%s)" % errors)
    else:
        log.error("Last run timestamp not set because of errors! (%s)" % errors)
    

if __name__ == '__main__':
    try:
        main()
        exit_func()
    except KeyboardInterrupt:
        exit_func()
