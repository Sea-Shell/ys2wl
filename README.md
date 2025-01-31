# Youtube Subscription to playlist

This script will take activity from your subscribers and add them to a playlist.
If you want to add these videos to watch-later you need to move them there yourself.  

The script uses pickle to save session betweene runs. it will create a picke file on first run.  
After some time this file will need to be re-created.


example config-file (config.yaml):
```
log-level: info
log-file: stream   # Will output all logs to stdout
pickle-file: /home/abc/.ys2wl/credentials.pickle   # if this file does not exist, it will be created when promted for authorization
credentials-file: /home/abc/.ys2wl/client_secret.json   # This file needs to contain credentials related to the app you create in youtube for this application
database-file: /home/abc/.ys2wl/my.db   # if this file does not exist, it will be created an empty database
reprocess-days: 0   # How many days betwene subscription will be processed
#published-after: 2023-11-01T00:00:00.000000+02:00   # dont add videos prior to this date
compare-distance-number: 97   # distance in "a number" a video title needs to be apart from another for it to be registered as a new video
youtube-channel: Bateau   # your channel name
youtube-playlist: TBL   # name of playlist to add videos to
youtube-playlist-sleep: 30   # delay betwene api calls for videos. can be good to avoid rate limiting
youtube-subscription-sleep: 60   # delay betwene api calls for subscriptions. can be good to avoid rate limiting
youtube-subscription-ignore-file: /home/abc/.ys2wl/.subscription-ignore   # new line delimited list of subscriptions to ignore when processing
youtube-video-ignore-file: /home/abc/.ys2wl/.video-ignore   # spesific video-ids to avoid. in case a video is acting up
youtube-minimum-length: 75s   # minimum length a video must be to be added to your playlist
youtube-maximum-length: 8m   # maximum length a video can be to be added to your playlist
```

