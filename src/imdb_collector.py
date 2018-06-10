#!/usr/bin/env python

import sys, os
import threading
import rospy
import datetime
import time
import json
import logging

from hs_utils.mongo_handler import MongoAccess
from hs_utils import ros_node
from hs_utils import utilities
from hs_utils import imdb_handler
from optparse import OptionParser, OptionGroup
from pprint import pprint

from std_msgs.msg import Bool
from std_msgs.msg import String
from torrent_search.msg import torrentQuery
from torrent_search.msg import torrentInvoice

logging.getLogger('imdbpie').setLevel(logging.getLevelName('WARNING'))

class CollectIMDb(ros_node.RosNode):
    def __init__(self, **kwargs):
        try:
            
            ## Use lock to protect list elements from
            ##    corruption while concurrently access. 
            ##    Check Global Interpreter Lock (GIL)
            ##    for more information
            self.threats_lock     = threading.Lock()
            
            ## This variable has to be started before ROS
            ##   params are called
            self.condition        = threading.Condition()

            ## Initialising parent class with all ROS stuff
            super(CollectIMDb, self).__init__(**kwargs)
            
            ## Adding local variables
            self.database         = None
            self.collection       = None
            self.db_handler       = None
            self.invoice          = None
            self.search_type      = None
            self.imdb_handler     = None
            self.list_terms       = None
            self.data_database    = None
            self.data_collection  = None
            self.retrieved_limit  = None
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              utilities.ParseException(inst)
              
    def SubscribeCallback(self, msg, topic):
        try:
            if 'collect_imdb_data' in topic:
                ## Get incoming message
                self.data_database   = msg.database
                self.data_collection = msg.collection
                self.search_type     = msg.search_type
                self.retrieved_limit = msg.page_limit
                
                rospy.loginfo('+ Got query in [%s/%s] of [%s] elements'%
                               (self.data_database, self.data_collection, str(self.retrieved_limit)))
                ## Notify thread that data has arrived
                with self.condition:
                    self.condition.notifyAll()
            
        except Exception as inst:
              utilities.ParseException(inst)
              
    def Init(self):
        try:
            ## Getting environment variables
            self.database = self.mapped_params['/imdb_collector/database'].param_value
            rospy.logdebug('+ Set database [%s]'%str(self.database))
            self.collection = self.mapped_params['/imdb_collector/collection'].param_value
            rospy.logdebug('+ Set collection [%s]'%str(self.collection))
            
            ## Connecting to data collection
            rospy.logdebug("Connecting to [%s] with [%s] collections"% 
                                (self.database, self.collection))
            self.db_handler = MongoAccess()
            self.db_handler.Connect(self.database, self.collection)
            
            ## Creating IMDb handler
            rospy.logdebug("Creating IMDb handler")
            self.list_terms = self.mapped_params['/imdb_collector/list_term'].param_value
            args = {'list_terms':   self.list_terms}
            self.imdb_handler = imdb_handler.IMDbHandler(**args)
            
            ## Starting publisher thread
            rospy.loginfo('Initialising imdb_collector_node node')
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
        except Exception as inst:
              utilities.ParseException(inst)

    def Run(self, event):
        ''' Execute this method to... '''
        try:
            ## This sample produces calls every 250 ms (40Hz), 
            ##    however we are interested in time passing
            ##    by seconds
            rate_sleep = rospy.Rate(5) 
            
            while not rospy.is_shutdown():
                
                with self.condition:
                    rospy.logdebug('+ Waiting for incoming data')
                    self.condition.wait()

                ## Starting IMDb collector
                self.CrawlDbRecords()
                
                ## Clearing data base credentials
                self.data_database   = None
                self.data_collection = None
                self.retrieved_limit = None
                
        except Exception as inst:
              utilities.ParseException(inst)

    def GetInvoice(self, posted_items):
        try:
            self.invoice = {
                    'database':     self.database,
                    'collection':   self.collection,
                    'search_type':  self.search_type,
                    'result':  {
                        'posted_items': len(posted_items),
                    }
                    
            }
        except Exception as inst:
            utilities.ParseException(inst)
        finally:
            return self.invoice

    def CrawlDbRecords(self):
        records = None
        try:
            
            ## Establishing DB connection
            if self.data_collection is None:
                rospy.logwarn("Invalid collection name: [%s]"%self.data_collection)
                return
            if self.data_database is None:
                rospy.logwarn("Invalid database name: [%s]"%self.data_database)
                return
                
            rospy.logdebug("1.1 Establishing DB connection to [%s/%s]"%
                              (self.data_database, self.data_collection))
            db_handler      = MongoAccess()
            is_connected    = db_handler.Connect(self.data_database, self.data_collection)
            if not is_connected:
                rospy.logwarn("Invalid connection to [%s/%s] "%
                              (self.data_database, self.data_collection))
                return
            
            ## Querying torrents without IMDb data
            rospy.logdebug("1.2 Querying torrents without IMDb data")
            query           = None ## TODO: Search for IMDB identifier
            records         = db_handler.Find(query, timeout=True)
            records_found   = records.count()
            rospy.logdebug("      Found [%d] records"%records_found)
            if records_found<1:
                rospy.logdebug("   + No records require IMDb update")
                
            ## Looking into found records
            rospy.logdebug("1.3 Updating records")
            counter         = 1
            record_counter  = 0
            for record in records:
                record_counter += 1
                #title       = record['name']
                title       = u''.join(record['name']).encode('utf-8').strip()
                if 'Autobahnpolizei' in title:
                    print "===> THIS FAILED!!!"
                rospy.logdebug("  1.3.1 [%s] Getting cleaned movie title [%s]"%
                               (str(record_counter), title))
                clean_name  = self.imdb_handler.clean_sentence(title)
                if len(clean_name) < 1:
                    rospy.logdebug("   + Invalid cleaned title")
                    continue
                
                year_found, splitted = self.imdb_handler.skim_title(clean_name)
                if splitted is None:
                    rospy.logdebug("   + Invalid skimmed title")
                    continue
                elif len(splitted)<1:
                    rospy.logwarn('No title was given for [%s]'%title)
                    continue
                rospy.logdebug("   + Skimmed title: [%s]"%(splitted))
                
                ## Searching if item already exists
                rospy.logdebug("  1.3.2 Searching if item [%s] already exists"%splitted)
                title_exists = self.db_handler.Find({ "query_title": splitted})
                if title_exists.count():
                    rospy.logdebug("        Title [%s] already exists"%splitted)
                    continue
                
                ## Collecting IMDb data
                rospy.logdebug("  1.3.3 Collecting best title of IMDb data")
                items       = self.imdb_handler.get_imdb_best_title(splitted, year_found=year_found)
                item_keys   = items.keys()
                if len(items)<1:
                    rospy.logwarn("No IMDb info was found for [%s]"%splitted)
                    continue
                    
                ## Showing multiple items
                if "imdb_info" in item_keys and len(items["imdb_info"])<1:
                    rospy.logwarn("No IMDb info was found for [%s]"%splitted)
                    continue
                
                if "imdb_info" in item_keys and len(items["imdb_info"])>1:
                    rospy.loginfo("Items [%s] found"%(str(len(items["imdb_info"]))))
                    for item in items["imdb_info"]:
                        rospy.loginfo("   title [%s] with score [%s]"%
                                      (str(item['title']), str(item['score'])))

                ## Updating IMDb data
                query_title = None
                if 'query_title'in item_keys:
                    query_title = items['query_title']
                score_ = float(items['imdb_info'][0]['score'])
                rospy.loginfo("  1.3.4 Inserted [%d] [%s] into DB with score [%2.4f]"%
                              (counter, str(query_title), score_))
                post_id = self.db_handler.Insert(items)
                
                #pprint(items)
                counter += 1
                if counter > self.retrieved_limit:
                    break
            
            rospy.loginfo("Closing, mongo cursor as it has timed out")
            records.close()
        except Exception as inst:
              utilities.ParseException(inst)
        finally:
            if records is not None:
                rospy.loginfo("Re-closing mongo cursor...")
                records.close()

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
    parser.add_option('--debug',
                action='store_true',
                default=False,
                help='Provide debug level')
    parser.add_option('--do_ros_file',
                action='store_true',
                default=False,
                help='Provide debug level')

    db_parser = OptionGroup(parser, "Mongo DB options",
                "Configure Mongo connection")
    db_parser.add_option('--database',
                type="string",
                action='store',
                default=None,
                help='Provide a valid database name')
    db_parser.add_option('--collection',
                type="string",
                action='store',
                default=None,
                help='Provide a valid collection name')
    
    parser.add_option_group(db_parser)
    (options, args) = parser.parse_args()
    
    args            = {}
    logLevel        = rospy.DEBUG if options.debug else rospy.INFO
    rospy.init_node('imdb_collector', anonymous=False, log_level=logLevel)

#     ## Removing ROS file logging
#     if not options.do_ros_file:
#         root_handlers = logging.getLoggerClass().root.handlers
#         if isinstance(root_handlers, list) and len(root_handlers)>0:
#             handler = root_handlers[0]
#             if type(handler) == logging.handlers.RotatingFileHandler:
#                 rospy.logwarn("Shutting down file log handler")
#                 logging.getLoggerClass().root.handlers = []

    ## Defining static variables for subscribers and publishers
    sub_topics     = [
        ('~collect_imdb_data',  torrentQuery)
    ]
    pub_topics     = [
        ('~topic2',  Bool)
    ]
    system_params  = [
        '/imdb_collector/database',
        '/imdb_collector/collection'
    ]
    
    ## Defining arguments
    args.update({'queue_size':      options.queue_size})
    args.update({'latch':           options.latch})
    args.update({'sub_topics':      sub_topics})
    args.update({'pub_topics':      pub_topics})
    args.update({'system_params':   system_params})
    
    # Go to class functions that do all the heavy lifting.
    try:
        spinner = CollectIMDb(**args)
    except rospy.ROSInterruptException:
        pass
    # Allow ROS to go to all callbacks.
    rospy.spin()
