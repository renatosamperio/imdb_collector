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
            self.list_terms       = None
            self.search_type      = None

            ## Parsing arguments
            for key, value in kwargs.iteritems():
                if "list_term" == key:
                    if value is not None:
                        rospy.logdebug("  +   Loading list of terms")
                        self.list_terms = self.LoadTerms(value)
            
            ## Initialise node activites
            self.Init()
        except Exception as inst:
              utilities.ParseException(inst)
              
    def SubscribeCallback(self, msg, topic):
        try:
            ## Get incoming message
            with self.threats_lock:
                self.object_variable    = msg

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
            
            rospy.logdebug("Connecting to [%s] with [%s] collections"% 
                                (self.database, self.collection))
            self.db_handler = MongoAccess()
            self.db_handler.connect(self.database, self.collection)
            
            ## Starting publisher thread
            rospy.loginfo('Initialising imdb_collector_node node')
            rospy.Timer(rospy.Duration(0.5), self.Run, oneshot=True)
        except Exception as inst:
              utilities.ParseException(inst)
 
    def LoadTerms(self, fileName):
        try:
            with open(fileName, 'r') as file:
                return file.read().strip().split()
                
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
    rospy.init_node('imdb_collector_node', anonymous=False, log_level=logLevel)

    ## Removing ROS file logging
    if not options.do_ros_file:
        root_handlers = logging.getLoggerClass().root.handlers
        if isinstance(root_handlers, list) and len(root_handlers)>0:
            handler = root_handlers[0]
            if type(handler) == logging.handlers.RotatingFileHandler:
                rospy.logwarn("Shutting down file log handler")
                logging.getLoggerClass().root.handlers = []

    ## Defining static variables for subscribers and publishers
    sub_topics     = [
        ('~topic1',  String)
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
