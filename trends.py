#!/usr/bin/python

#-----------------------------------------------------------------------
# twitter-trends
#  - lists the current global trending topics
#-----------------------------------------------------------------------

from twitter import *

def update_trends(cfg):
	spain_trends = get_trends(cfg, 23424950)
	madrid_trends = get_trends(cfg, 766273)
	barcelona_trends = get_trends(cfg, 753692)
	global_trends = get_trends(cfg, 1)

	return list(set(spain_trends+madrid_trends+barcelona_trends+global_trends))

def get_trends(cfg, id):
	twitter = Twitter(
		        auth = OAuth(config["access_key"], config["access_secret"], config["consumer_key"], config["consumer_secret"]))

	results = twitter.trends.place(_id = id)

	trends = []
	for location in results:
		print location
		print " "
		for trend in location["trends"]:
			trends.append(trend["name"])

	return trends

#-----------------------------------------------------------------------
# load our API credentials 
#-----------------------------------------------------------------------
config = {}
execfile("config.py", config)

#-----------------------------------------------------------------------
# create twitter API object
#-----------------------------------------------------------------------
twitter = Twitter(
		        auth = OAuth(config["access_key"], config["access_secret"], config["consumer_key"], config["consumer_secret"]))


#-----------------------------------------------------------------------
# retrieve global trends.
# other localised trends can be specified by looking up WOE IDs:
#   http://developer.yahoo.com/geo/geoplanet/
# twitter API docs: https://dev.twitter.com/docs/api/1/get/trends/%3Awoeid
#-----------------------------------------------------------------------
results = update_trends(config)

print "Spain Trends"

for r in results:
	print r