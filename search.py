#!/usr/bin/python

#-----------------------------------------------------------------------
# twitter-stream-format:
#  - ultra-real-time stream of twitter's public timeline.
#    does some fancy output formatting.
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
# import a load of external features, for text display and date handling
# you will need the termcolor module:
#
# pip install termcolor
#-----------------------------------------------------------------------
from time import strftime, localtime, mktime
from textwrap import fill
from termcolor import colored
from email.utils import parsedate

from twitter import *
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import re, sys, json

tweet_index_name = 'twitter-tweets'
trends_index_name = 'twitter-trends'
tweet_mapping = {
	'tweet': {
		"_ttl" : { "enabled" : True, "default" : "7d" },
		'properties': {
			'date': {
				'type' : 'date',
				'doc_values' : True
			},
			'text': {
				'type' : 'string',
				'index' : 'not_analyzed'
			},
			'user': {
				'type' : 'string',
				'index' : 'not_analyzed'
			},
			'topic': {
				'type' : 'string',
				'index' : 'not_analyzed'
			},
			'location': {
				'type' : 'geo_point'
			}
		}

	}
}

trends_mapping = {
	'trend' : {
		"_ttl" : { "enabled" : True, "default" : "30d" },
		'properties' : {
			'date': {
				'type' : 'date',
				'doc_values' : True
			},
			'topic': {
				'type' : 'string',
				'index' : 'not_analyzed'
			}
		}
	}
}

es = Elasticsearch()

def create_index(index_name, mapping):
	global es
	if es.indices.exists(index_name) == False:
		print "NUEVO INDICE CREADO => " + index_name
		print mapping
		es.indices.create(index_name, body={'mappings': mapping}, ignore=400)
	else:
		print "INFO: Index " + index_name + " already exists. No problem."

def index_new_trends(index_name, trends, t_time):
	global es

	actions = []

	for t in trends:
		source = {
			'date' : t_time*1000,
			'topic': t
		}

		action = {
			"_index" : index_name,
			"_type" : "trend",
			"_source" : source
		}

		actions.append(action)
	
	ret = helpers.bulk(es, actions)
	print "New Trends indexed ",
	print ret

def centroid(points):
    x_coords = [p[0] for p in points]
    y_coords = [p[1] for p in points]
    _len = len(points)
    centroid_x = sum(x_coords)/_len
    centroid_y = sum(y_coords)/_len
    return [centroid_x, centroid_y]


def get_trends(cfg, id):
	twitter = Twitter(
		        auth = OAuth(config["access_key"], config["access_secret"], config["consumer_key"], config["consumer_secret"]))

	results = twitter.trends.place(_id = id)

	trends = []
	for location in results:
		for trend in location["trends"]:
			trends.append(trend["name"])

	return trends

def get_topic_from_text(text, trends):
	for topic in trends:
		if topic in text:
			return topic
	return None

def update_trends(cfg):
	spain_trends = get_trends(cfg, 23424950)
	madrid_trends = get_trends(cfg, 766273)
	barcelona_trends = get_trends(cfg, 753692)
	global_trends = get_trends(cfg, 1)

	return list(set(spain_trends+madrid_trends+barcelona_trends+global_trends))

#-----------------------------------------------------------------------
# load our API credentials 
#-----------------------------------------------------------------------
config = {}
execfile("config.py", config)

create_index(tweet_index_name, tweet_mapping)
create_index(trends_index_name, trends_mapping)

trend_time = int(mktime(localtime()))

trends = update_trends(config)
index_new_trends(trends_index_name, trends, trend_time)

search_term = ', '.join(trends)

#print search_term


#-----------------------------------------------------------------------
# create twitter API object
#-----------------------------------------------------------------------
auth = OAuth(config["access_key"], config["access_secret"], config["consumer_key"], config["consumer_secret"])
stream = TwitterStream(auth = auth, secure = True)

#-----------------------------------------------------------------------
# iterate over tweets matching this filter text
#-----------------------------------------------------------------------
tweet_iter = stream.statuses.filter(track = search_term)

pattern = re.compile("%s" % search_term, re.IGNORECASE)

actions = []
actions_ctr = 0

for tweet in tweet_iter:
	#NEED TO CHECK NEW TRENDS?
	current_time = int(mktime(localtime()))
	if current_time-trend_time > 600:
		trend_time = current_time
		trends = update_trends(config)
		index_new_trends(trends_index_name, trends, trend_time)
		search_term = ', '.join(trends)

	try:
		source = {}
		coords = None

		if 'place' in tweet and tweet['place'] != None:
			# print tweet
			if 'bounding_box' in tweet['place'] and tweet['place']['bounding_box']['type'] == 'Polygon':
				# print "COORD ! --> ",
				coords = centroid(tweet['place']['bounding_box']['coordinates'][0])
				# print coords
			else:
				print "OTRA COSA ! --> ",

		# turn the date string into a date object that python can handle
		if 'created_at' in tweet:
			timestamp = parsedate(tweet["created_at"])
		else:
			timestamp = localtime()

		# now format this nicely into HH:MM:SS format
		timetext = strftime("%H:%M:%S", timestamp)

		# colour our tweet's time, user and text
		time_colored = colored(timetext, color = "white", attrs = [ "bold" ])
		user_colored = colored(tweet["user"]["screen_name"], "green")
		text_colored = tweet["text"]

		# replace each instance of our search terms with a highlighted version
		text_colored = pattern.sub(colored(search_term.upper(), "yellow"), text_colored)

		# add some indenting to each line and wrap the text nicely
		indent = " " * 11
		text_colored = fill(text_colored, 80, initial_indent = indent, subsequent_indent = indent)

		# now output our tweet
		# print "(%s) @%s" % (time_colored, user_colored)
		# print "%s" % (text_colored)

		
		source['date'] = mktime(timestamp)*1000
		if coords != None:
			source['location'] = coords
		source['text'] = tweet['text']
		source['user'] = tweet["user"]["screen_name"]
		source['topic'] = get_topic_from_text(tweet['text'], trends)
		
		action = {
			"_index" : tweet_index_name,
			"_type" : "tweet",
			"_source" : source
		}

		actions.append(action)
		actions_ctr += 1
		if actions_ctr >= 1000:
			ret = helpers.bulk(es, actions)
			timelog = str(strftime("%d/%m/%Y %H:%M:%S", localtime()))
			print timelog + " INDEXED: ",
			print ret
			del actions
			actions = []
			actions_ctr = 0

	except KeyError:
		pass
