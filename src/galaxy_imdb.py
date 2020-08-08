#!/usr/bin/env python

import sys, os
import threading
import rospy
import time
import Queue
import json
import pymongo

from optparse import OptionParser, OptionGroup
from pprint import pprint
from datetime import datetime, timedelta
from collections import defaultdict

from hs_utils import imdb_handler
from hs_utils import ros_node, logging_utils
from hs_utils import message_converter as mc
from hs_utils import json_message_converter as rj
from hs_utils.mongo_handler import MongoAccess
#from events_msgs.msg import WeeklyEvents

class ImdbCollector:
    
    def __init__(self, **kwargs):
        try:
            self.database           = None
            self.torrents_collection= None
            self.torrents_db        = None
            self.imdb_collection    = None
            self.imdb_db            = None
            self.imdb_handler       = None
            self.list_terms         = None
            
            for key, value in kwargs.iteritems():
                if "database" == key:
                    self.database = value
                elif "torrents_collection" == key:
                    self.torrents_collection = value
                elif "imdb_collection" == key:
                    self.imdb_collection = value
                elif "latest_collection" == key:
                    self.latest_collection = value
                elif "list_terms" == key:
                    self.list_terms = value

            ## Creating DB handler
            self.torrents_db = MongoAccess()
            connected       = self.torrents_db.Connect(
                                                self.database, 
                                                self.torrents_collection)
            ## Checking if DB connection was successful
            if not connected:
                raise Exception('DB [%s.%s] not available'%
                              (self.database, self.torrents_collection))
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.torrents_collection))
                
            ## Creating DB handler
            self.imdb_db = MongoAccess()
            connected       = self.imdb_db.Connect(
                                                self.database, 
                                                self.imdb_collection)
            ## Checking if DB connection was successful
            if not connected:
                raise Exception('DB [%s.%s] not available'%
                              (self.database, self.imdb_collection))
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.imdb_collection))
            
            ## Creating DB handler
            self.latest_db = MongoAccess()
            connected       = self.latest_db.Connect(
                                                self.database, 
                                                self.latest_collection)
            ## Checking if DB connection was successful
            if not connected:
                raise Exception('DB [%s.%s] not available'%
                              (self.database, self.latest_collection))
            else:
                rospy.loginfo("Created DB handler in %s.%s"%
                              (self.database, self.latest_collection))
            
            
            args = {
                'list_terms':self.list_terms,
                'imdb':      True
            }
            self.imdb_handler = imdb_handler.IMDbHandler(**args)
            rospy.loginfo("Created IMDb handler")
            
        except Exception as inst:
              ros_node.ParseException(inst)
    
    def find_imdb_data(self, imdb_id, galaxy_id):
        dict_row = None
        existed  = False
        try:
            ## updating DB records with new item
            condition = { 'imdb_id' : imdb_id }
            ## Inserting record if it would not exists
            posts = self.imdb_db.Find(condition)
            
            ## Checking for latest
            posts_items = posts.count()
            if posts_items>0:
                rospy.logdebug("Record [%s] has [%d] items"%(imdb_id, posts_items))
                dict_row = posts[0]
                if posts_items>1:
                    for p in posts:
                        pprint(p)
                        print "-- "*10
                    
                rospy.logdebug('  IMDB item [%s] was found locally'%imdb_id)
                
                ## TODO check if it is too old, renew it?
            else:
                rospy.loginfo("Collecting [%s]"%(imdb_id))
                dict_row = self.imdb_handler.get_info(imdb_id)
                
                ## inserting new record
                rospy.logdebug("Inserting in DB [%s]"%imdb_id)
                datetime_now = datetime.now() 
                dict_row.update({'last_updated' : datetime_now})
                result = self.imdb_db.Insert(dict_row)
                
                ## update latest timestamp that it was updated
                q = {"galaxy_id"    : galaxy_id}
                u = {"imdb_updated" : datetime_now }
                res = self.torrents_db.Update(condition=q, substitute=u)
                if not res: rospy.logdebug(' Not updated in [%s]'%galaxy_id)
                rospy.loginfo('Retrieved IMDB for [%s]'%imdb_id)
                
                ## double check if something went wrong while updating DB
                if not result: rospy.logwarn('Invalid DB update for [%s]'%dict_row['imdb_id'])
                existed  = True
        except Exception as inst:
              ros_node.ParseException(inst)
        finally:
            return dict_row, existed
            
    def search(self):
        try:
            query = { 
                'imdb_updated': False
            }
            
            sort_condition = [
                ("galaxy_id", pymongo.DESCENDING)
            ]
            posts  = self.torrents_db.Find(query, sort_condition=sort_condition)
            rospy.loginfo("Found [%d] torrents to update"%posts.count())
            
            for torrent in posts:
                
                ## searching for torrent in IMDB
                imdb_id = torrent['imdb_code']
                if not imdb_id:
                    rospy.logdebug("Missing IMDB id for [%s]"%(torrent['title']))
                    continue
                
                ## updating IMDB data
                dict_row, existed = self.find_imdb_data( imdb_id, torrent['galaxy_id'])
                if not dict_row:
                    rospy.logwarn("Invalid IMDB info collected")
                    continue
                
                ## setting update time if it was retrieved from IMDB web
                if not existed: 
                    datetime_now = datetime.now() 
                    torrent['imdb_updated'] = datetime_now

            rospy.loginfo("Finished searching IMDB info")
        except Exception as inst:
              ros_node.ParseException(inst)

    def merge_latest(self):
        '''
        Collecting torrents with IMDB info
        '''
        try:
            rospy.logdebug('Preparing infomration for latest torrents')
            date_latest = datetime.now() - timedelta(hours=12)
            query = { 
                '$and': 
                [
                    {'torrent_updated': { '$exists': True}}, 
                    {'torrent_updated': { '$gte':    date_latest}} 
                ]
            };
            
            sort = [
                ("torrent_updated", pymongo.DESCENDING)
            ]
            ## search for latest page only and be sure to
            ## review ones with latest set IMDB information
            posts  = self.torrents_db.Find(query, sort_condition=sort)
            rospy.loginfo("Found [%d] torrents to update"%posts.count())
            
            latest = defaultdict(lambda: {})
            
            for torrent in posts:

                ## assign given imdb code as index of torrent state
                galaxy_id    = torrent['galaxy_id']
                if 'imdb_code' not in torrent:
                    rospy.logdebug('Record [%s] without IMDB code'%galaxy_id)
                    continue
                
                ## validate imdb code
                t_imdb = torrent['imdb_code']
                if not t_imdb:
                    rospy.loginfo('Torrent [%s] has no IMDB'%galaxy_id)
                    continue
                
                ## check if imdb code is already seen in latest
                posts = self.latest_db.Find({'imdb_code': t_imdb})
                torrent_exists = False
                for found in posts:
                    for item in found['torrents']:
                        if galaxy_id == item['galaxy_id']:
                            torrent_exists = True
                            break
                
                if torrent_exists:
                    rospy.logdebug('-  Found torrent [%s] in [%s]'%(t_imdb, galaxy_id))
                    continue
                    
                ## getting torrent data to display
                torrent_record = {
                    'title':        torrent['title'],
                    'magnet':       torrent['magnet'],
                    'file':         torrent['file'],
                    'leechers':     torrent['leechers'],
                    'seeders':      torrent['seeders'],
                    'views':        torrent['views'],
                    'size':         torrent['size'],
                    'galaxy_id':    galaxy_id,
                }
                
                ## this element provides distinguishable 
                ## information about each torrent
                if 'torrent_info' in torrent:
                    torrent_record.update({'torrent_info': torrent['torrent_info']})

                ## prepare a collection with latest seen
                if posts.count()>0:
                    ## item already has a record and has to be updated
                    q = {'imdb_code': t_imdb};
                    u = {'torrents': torrent_record}
                    ok = self.latest_db.Update(condition=q, substitute=u, 
                            upsertValue=True, operator='$push')
            
                    ## double check if something went wrong while updating DB
                    if not ok: rospy.logwarn('Invalid DB update for [%s]'%t_imdb)
                    rospy.logdebug('-  Updated [%s] with [%s]'%(t_imdb, galaxy_id))
                    continue
                
                ## got first element of torrent IMDB
                if t_imdb:
                    imdb_info, e = self.find_imdb_data( t_imdb, galaxy_id )
                    if not imdb_info: rospy.logwarn("Invalid IMDB info collected")
        
                ## prepare record, use as index galaxy and IMDB ids
                new_record = {
                    'imdb_code':    t_imdb,
                    'torrents':    [torrent_record],
                    'imdb':         imdb_info,
                    'last_updated': datetime.now()
                }
        
                ## insert item as a latest seen torrent
                ok = self.latest_db.Insert(new_record)
                if not ok: rospy.logwarn('Item [%s] was not inserted'%(t_imdb))
                rospy.logdebug('+  Created [%s] with [%s]'%(t_imdb, galaxy_id))

        except Exception as inst:
            ros_node.ParseException(inst)

    def close(self):
        try:
            self.torrents_db.debug = 0
            self.imdb_db.debug     = 0
            self.latest_db.debug   = 0
            self.torrents_db.Close()
            self.imdb_db.Close()
            self.latest_db.Close()
        except Exception as inst:
              ros_node.ParseException(inst)
    
class GalaxyImdb(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            self.condition  = threading.Condition()
            self.queue      = Queue.Queue()
            self.list_terms = None
            self.rate       = 5000
            
            ## Initialising parent class with all ROS stuff
            super(GalaxyImdb, self).__init__(**kwargs)
            
            for key, value in kwargs.iteritems():
                if "rate" == key:
                    self.rate = value
                    rospy.logdebug('      Rate is [%d]'%self.rate)

            ## Initialise node activites
            self.Init()
        except Exception as inst:
              ros_node.ParseException(inst)

    def Init(self):
        try:
            
            self.list_terms = self.mapped_params['/galaxy_imdb/list_term'].param_value
            
            args = {
                'database':           'galaxy',
                'latest_collection':  'latest',
                'torrents_collection':'torrents',
                'imdb_collection':    'imdb',
                'list_terms':         self.list_terms
            }            
            self.crawler = ImdbCollector(**args)
            
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
        except Exception as inst:
              ros_node.ParseException(inst)

    def SubscribeCallback(self, msg, topic):
        try:

            ## Storing message for queue
            rospy.logdebug('Got query message')
            stored_items = (topic, msg)
            self.queue.put( stored_items )
            
            ## Notify data is in the queue
            with self.condition:
                self.condition.notifyAll()
            
        except Exception as inst:
              ros_node.ParseException(inst)
                    
    def ShutdownCallback(self):
        try:
            rospy.logdebug('+ Shutdown: Closing torrent parser')
            if self.crawler:
                self.crawler.close()
                self.crawler = None
        except Exception as inst:
              ros_node.ParseException(inst)
              
    def Run(self, event):
        ''' Run method '''
        try:
            rospy.logdebug('+ Starting run method')
            rate_sleep = rospy.Rate(1.0/self.rate)
            while not rospy.is_shutdown():

                rospy.logdebug('  Looking for information in IMDB')
                self.crawler.search()
                
                rospy.logdebug('  Merging latest torrents')
                self.crawler.merge_latest()

                rate_sleep.sleep()
            
        except Exception as inst:
              ros_node.ParseException(inst)

if __name__ == '__main__':
    usage       = "usage: %prog option1=string option2=bool"
    parser      = OptionParser(usage=usage)
    parser.add_option('--queue_size',
                type="int",
                action='store',
                default=1000,
                help='Topics to play')
    parser.add_option('--latch',
                action='store_true',
                default=False,
                help='Message latching')
    parser.add_option('--debug', '-d',
                action='store_true',
                default=True,
                help='Provide debug level')
    parser.add_option('--std_out', '-o',
                action='store_false',
                default=True,
                help='Allowing standard output')
    parser.add_option('--rate',
                action='store_false',
                default=14400.0,
                help='Period of time to crawl')

    (options, args) = parser.parse_args()
    
    args            = {}
    logLevel        = rospy.DEBUG if options.debug else rospy.INFO
    rospy.init_node('galaxy_imdb', anonymous=False, log_level=logLevel)

    ## Defining static variables for subscribers and publishers
    sub_topics     = [
#         ('/event_locator/weekly_events',  WeeklyEvents),
    ]
    pub_topics     = [
#         ('/event_locator/updated_events', WeeklyEvents)
    ]
    system_params  = [
        '/galaxy_imdb/list_term',
    ]
    
    ## Defining arguments
    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    args.update({'allow_std_out':   options.std_out})
    args.update({'rate':            options.rate})
    args.update({'system_params':   system_params})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = GalaxyImdb(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()

