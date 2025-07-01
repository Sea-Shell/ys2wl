# Youtube Subscription to playlist

This script will take activity from your subscribers and add them to a playlist.
If you want to add these videos to the special "watch-later" playlist you need to move them there yourself.  

The script uses pickle to save session betweene runs. it will create a picke file on first run.  
You need to create a application in youtube to be able to populate this picke-file
After some time this file will need to be re-created.


example config-file (config.yaml):
```yaml
pickle-file: ""                       # File to store access token once authenticated
credentials-file: ""                  # JSON file with credentials to oAuth2 account
database-file: ""                     # Location of sqlite database file. Will be created if not exists
local-json-files: ""                  # JSON file with credentials to oAuth2 account
compare-distance-number: 0            # Levenstein number to compare difference between existing videos and new
published-after: ""                   # Timestamp in ISO8601 (YYYY-MM-DDThh:mm:ss.sZ) format in which only videos from after this date will be added
reprocess-days: ""                    # Amount of days before subscription will be processed again
youtube-channel: ""                   # Name of your channel with destination playlist
youtube-playlist: ""                  # Name of playlist to add videos to
youtube-activity-limit: 0             # How much activity to process per subscription per run
youtube-subscription-limit: 0         # How many subscriptions to process per run
youtube-subscription-ignore-file: ""  # File with newline separated list of subscriptions to ignore when processing
youtube-video-ignore-file: ""         # File with newline separated list of video-ids to ignore when processing
youtube-words-ignore-file: ""         # File with newline separated list of words to ignore when processing
youtube-playlist-sleep: 0             # How long to wait between playlist API insert-calls
youtube-subscription-sleep: 0         # How long to wait between subscription API insert-calls
youtube-minimum-length: ""            # Minimum length of tracks to add. 1s, 2m, 1h format
youtube-maximum-length: ""            # Maximum length of tracks to add. 1s, 2m, 1h format
log-level: ""                         # Set loglevel: debug, info, warning, or error
log-file: ""                          # File to cast logs to. If you want all output to stdout type "stream"
```
