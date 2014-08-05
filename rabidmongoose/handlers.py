'''
handler.py is responsible to translate requests from
rabid.mongoose worker into a MongoR command issued to the database
'''
# This file incorporates work covered by the following copyright and
# permission notice:  
#    Copyright 2009-2010 10gen, Inc.
#    
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from pymongo import ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure, AutoReconnect
from bson import json_util
from bson.objectid import ObjectId
from mongor import GlobalQuery
from time import time
import logging
from Queue import Queue
from threading import Thread, Lock
from datetime import datetime


FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
LOGGER = logging.getLogger(__name__)

try:
    import json
except ImportError:
    import simplejson as json

try:
    from fluent.sender import FluentSender
    from fluent.handler import FluentHandler
    HAS_FLUENT = True
except ImportError:
    HAS_FLUENT = False
    

class MongoHandler:
    mh = None
    _cursor_id = 0
    def __init__(self, mongos,
                 mongor_ssl = False,
                 cursor_timeout = 600, #10 Minutes
                 fluentd = "localhost:24224", 
                 logtag = "rabidmongoose"):
        self.connections = {}
        self.cursor_timeout = cursor_timeout
        self.mongor_uri  =  "".join(mongos)
        self.mongor_host = self.mongor_uri[10:].split(":")[0]
        self.mongor_port = int(self.mongor_uri[10:].split(":")[1])
        self.mongor_ssl = mongor_ssl
        LOGGER.debug('INIT - URI: %s, Host: %s, Port: %s' %(self.mongor_uri,
                                                            self.mongor_host,
                                                            self.mongor_port))
        if HAS_FLUENT:
            self.sender = FluentSender(host = fluentd.split(":")[0], 
                                       port = int(fluentd.split(":")[1]), 
                                       tag = logtag)
            LOGGER.addHandler(FluentHandler("%s.debug" %(logtag), 
                                            host=fluentd.split(":")[0], 
                                            port=int(fluentd.split(":")[1])))
        else:
            self.sender = FakeFluentSender()
    def has_cursor(self, cursor_id):
        '''public function for caller to check if this handler has the cursor
        '''
        has_cursor = False
        if hasattr(self, "cursors"): 
            if cursor_id in getattr(self, "cursors"):
                has_cursor = True
        return has_cursor
    def available_cursors(self):
        self._clean_cursors()
        if hasattr(self, "cursors"): 
            return getattr(self, "cursors")
        else:
            return []
        
    def _get_connection(self, db_type="", db_tags=None):
        ''' grabs the connection from the pool and uses it
        if the connection does not exist, it generates a new connection'''
        if not db_tags:
            db_tags = []
        LOGGER.debug("Using db_type: '%s', db_tags: '%s'", db_type, db_tags)
        unique_id = "%s_%s_%s" %(db_type,
                                 "_".join(sorted(db_tags)),
                                 datetime.now().timetuple().tm_yday)
        LOGGER.debug("Using unique ID: '%s'", unique_id)
        
        if unique_id not in self.connections:
            LOGGER.debug({"msg":'unknown MongoR Connection.%s %s %s %s'
                         %(self.mongor_host, self.mongor_port, 
                           db_type, db_tags)})
            try:
                self.connections[unique_id] = GlobalQuery(self.mongor_host,
                                                          config_port=self.mongor_port,
                                                          config_ssl=self.mongor_ssl,
                                                          db_type=db_type, 
                                                          db_tags=db_tags)
            except ConnectionFailure:
                self.connections[unique_id] = None
        return self.connections[unique_id]
       
    def _get_son(self, input_str, out):
        '''uses the standard bson json_util to convert json to a mongo query document
        '''
        try:
            obj = json.loads(input_str, object_hook=json_util.object_hook)
        except (ValueError, TypeError):
            out('{"ok" : 0, "errmsg" : "couldn\'t parse json: %s"}' % input_str)
            return None
        
        if getattr(obj, '__iter__', False) == False:
            out('{"ok" : 0, "errmsg" : "type is not iterable: %s"}' % input_str)
            return None
        return obj
    
    def _not_implemented(self, args, out, 
                         name = None, db = None, 
                         collection = None):
        '''return a standard message if the function is not implemented
        '''
        out('{"ok" : 0, "errmsg" : "this function is not implemented"}')
        return
    
    def _clean_cursors(self):
        '''removes cursors that are timed out'''
        if hasattr(self, "cursors"):
            cursors = getattr(self, "cursors")
            to_del = [] #throw it in a list so the dict doesnt change size during inspection
            for cursor_id in cursors:
                #enter stupid workaround to deal with python
                # offset-naive and offset-aware datetimes
                time_delta = ObjectId().generation_time - ObjectId(cursor_id).generation_time
                if time_delta.days > 0 or time_delta.seconds > self.cursor_timeout:
                    to_del.append(cursor_id)
            for cursor_id in to_del:
                del(cursors[cursor_id])
    
    def _cmd(self, args, out, name = None, 
                   db = None, collection = None):
        ''' Issue an arbitrary command to the database.
        Unsupported in Rabid.Mongoose at this time'''
        return self._not_implemented( args, out, name = None, 
                                      db = None, collection = None)
    
    def _hello(self, args, out, name = None, 
                     db = None, collection = None):
        '''test that the server is responsive
        does not open any MongoR handles to check this function
        Args:
            args: The query arguments.
            out: An instance of a callable which takes a single string parameter.
                 Used to send data back to the client.
            name: TBD (default None)
            db: TBD (default None)
            collection: TBD (default None)
        Returns:
            None
        '''
        LOGGER.debug({"msg":'Received _hello. Arguments: %s' %args})
        out('{"ok" : 1, "msg" : "Uh, we had a slight weapons malfunction, \
            but uh... everything\'s perfectly all right now. We\'re fine. \
            We\'re all fine here now, thank you. How are you?"}')
        return
        
    def _status(self, args, out, name = None, 
                      db = None, collection = None):
        '''not impemented in rabid.mongoose'''
        return self._not_implemented( args, out, name = None, 
                                      db = None, collection = None)
 
    def _authenticate(self, args, out, name = None, 
                            db = None, collection = None):
        '''authenticates to the database
        user level authentication must be implemented upstream 
        with authenticated username passed to rabid.mongoose'''
        return self._not_implemented( args, out, name = None, 
                                      db = None, collection = None)
         
    def _count(self, args, out, name = None, 
               db = None, collection = None):
        """
        Performs an aggregate count across all MongoR databases 
        based on the proivded criteria.
        """
        
        LOGGER.debug({"msg":'Received _count request. Arguments: %s' %args})
        
        # Check to make sure database and collection are defined prior to 
        # attempting to do anything.
        if db == None or collection == None:
            out('{"ok" : 0, "errmsg" : "db and collection must be defined"}')
            return
        
        db_type = db #leave the original interface to minimize change footprint for now
        
        # Parse the provided arguments into the expected keys
        try:
            (username, db_tags,
             criteria, fields,
             sort, limit, skip) = self.__parse_arguments(args, out)
        except:
            return
        
        # Get the database connection based on the type and tags.
        conn = self._get_connection(db_type, db_tags)
        if not conn:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo"}')
            return
        
        cursor_id = str(ObjectId())
        errors = []
        total_count = 0
            
        for host, query in conn.remote_databases.items():
            try:
                count = query.count(collection, criteria)
                total_count += count
            except AutoReconnect:
                errors.append({"host" : host, 
                               "errmsg" : "AutoReconnect, Try Again"})
            except OperationFailure, op_failure:
                errors.append({"host" : host, 
                               "errmsg" : "%s" % op_failure})
            except AttributeError:
                errors.append({"host" : host, 
                               "errmsg" : "inactive"})
            except StopIteration:
                # this is so stupid, there's no has_next?
                pass
                
        LOGGER.info({"msg":"Total count: '%s'" %total_count})
        # Invoke callable and send back structure containing count.
        out(json.dumps({"count" : total_count, 
                        "id" : cursor_id, 
                        "ok" : 1, 
                        "errors":errors}, default=json_util.default))
        
    def _log(self, command, args):
        """logs argument depending on what self.sender is
        Args:
            args: The query arguments.
        Returns:
            None
        Requires:
            self.sender has emit function that takes two <str> arguments
        """
        args['command'] = command
        return self.sender.emit("search", args)
        
    def _stats(self, args, out, name = None, db = None, collection = None):
        """Generates Basic Stats for the MongoR cluster
        Args:
            args: The query arguments.
            out: An instance of a callable which takes a single string parameter.
                 Used to send data back to the client.
            name: TBD (default None)
            db: TBD (default None)
            collection: TBD (default None)
        Returns:
            None
        """
        LOGGER.debug({"msg":'Received _stats request. Arguments: %s' %args})
        if db == None or collection == None:
            out('{"ok" : 0, "errmsg" : "db and collection must be defined"}')
            return
        db_type = db #leave the original interface to minimize change footprint for now
        # Parse the provided arguments into the expected keys
        (username, db_tags,
         criteria, fields,
         sort, limit, skip) = self.__parse_arguments(args, out)
        conn = self._get_connection(db_type, db_tags)
        if not conn:
            out('{"ok" : 0, "errmsg" : "couldn\'t get connection to mongo. Check config.py"}')
            return
        stats = []
        errors = []
        responsive = []
        unresponsive = []
        for host, query in conn.remote_databases.items():
            if not query:
                unresponsive.append(host)
            else:
                responsive.append(host)
                try:
                    stats.append({"host":host,
                                  "earliest":self.__earliest(host, 
                                                             query, 
                                                             collection)})
                except AutoReconnect:
                    errors.append({"host" : host, 
                                   "errmsg" : "AutoReconnect, Try Again"})
                except OperationFailure, op_failure:
                    errors.append({"host" : host, 
                                   "errmsg" : "%s" % op_failure})
                except StopIteration:
                    # this is so stupid, there's no has_next?
                    pass
                
        success = 1
        if errors:
            success = 0
        out(json.dumps({"results" : stats, 
                        "id" : 0, 
                        "ok" : success,
                        "responsive": responsive,
                        "unresponsive": unresponsive,
                        "errors":errors}, default=json_util.default))
        return
    def __earliest(self, host, query, collection):
        '''Returns the timestamp of the earliest record in the database'''
        reverse_databases = query.databases[::-1]
        for db in reverse_databases:
            try:
                doc = db[collection].find({}, {"_id":1}).sort([("_id", 1)])[0]
                if doc:
                     return doc['_id'].generation_time.isoformat()
            except IndexError:
                continue
        return "0"
    def _find(self, args, out, name = None, db = None, collection = None):
        """Generates MongoR cursors used for iterating the data
        Calls _more with its own cursorID 
        Args:
            args: The query arguments.
            out: An instance of a callable which takes a single string parameter.
                 Used to send data back to the client.
            name: TBD (default None)
            db: TBD (default None)
            collection: TBD (default None)
        Returns:
            None
        """
        
        LOGGER.debug({"msg":'Received _find request. Arguments: %s' %args})
        # Check to make sure database and collection are defined prior to 
        # attempting to do anything.
        if db == None or collection == None:
            out('{"ok" : 0, "errmsg" : "db and collection must be defined"}')
            return
        
        db_type = db #leave the original interface
        
        self._clean_cursors()
        
        # Parse the provided arguments into the expected keys
        (username, db_tags,
         criteria, fields,
         sort, limit, skip) = self.__parse_arguments(args, out)
        
        conn = self._get_connection(db_type, db_tags)
        if not conn:
            out('{"ok" : 0, "errmsg" : "No Connection, Check config.py"}')
            return
        
        docs = []
        errors = []
        cursor_id = str(ObjectId())
        args['id'] = [cursor_id]
        query_pointers = {"query":{}, "dereferenced":[]}
        
        if not hasattr(self, "cursors"):
            setattr(self, "cursors", {})
        
        cursors = getattr(self, "cursors")
        cursors[cursor_id] = query_pointers
        
        for host, query in conn.remote_databases.items():
            try:
                cursor = query.find(collection, criteria, 
                                    projection=fields, 
                                    limit=limit, 
                                    skip=skip, 
                                    sort=sort)
                query_pointers['query'][host] = cursor
            except AutoReconnect:
                errors.append({"host" : host, 
                               "errmsg" : "AutoReconnect, Try Again"})
            except OperationFailure, op_failure:
                errors.append({"host" : host, 
                               "errmsg" : "%s" % op_failure})
            except AttributeError:
                errors.append({"host" : host, 
                               "errmsg" : "inactive"})
            except StopIteration:
                # this is so stupid, there's no has_next?
                pass
        self._log("find", args)
        self._more(args, out)
        return None
    def __parallel_more(self, host, cursor, limit, results, errors, lock):
        '''should be used in a <threading.Thread>
        MUST be able to share the cursor <generator> with main thread
        '''
        count_per_host = 0
        lock.acquire()
        try:
            for doc in cursor:
                lock.release() #got the document, let the main thread pause us
                doc['uri'] = "mongodb://%s" % (host)
                #query_pointers['dereferenced'].append(doc)
                results.put(doc)
                count_per_host += 1
                if count_per_host >= limit:
                    break
                if not lock.acquire(False):#Lock while getting the next document
                    break #main thread acquired the lock
        except AutoReconnect:
            errors.put({"host" : host, "errmsg" : "AutoReconnect, Try Again"})
        except OperationFailure, op_failure: 
            #the cursor expired, deliver dereferenced values and close
            errors.put({"host" : host, "errmsg" : "%s" % op_failure})
        except AttributeError:
            errors.put({"host" : host, "errmsg" : "inactive"})
        except Exception as uncaught_exception:
            errors.put({"host" : host, 
                        "errmsg" : "uncaught exception",
                        "exception": str(uncaught_exception)})
            pass #should never see this in normal use cases
        try:
            lock.release() #double check the main thread will be able to acquire
        except ValueError: #lock released too many times, oh well
            pass
        return
    
    def _more(self, args, out, name = None, db = None, collection = None):
        '''gets more data from a pre-defined cursor
        Input
            args: The query arguments.
            out: An instance of a callable which takes a single string parameter.
                 Used to send data back to the client.
            name: TBD (default None)
            db: TBD (default None)
            collection: TBD (default None)
        Returns:
            1. None
        Ensures:
            1. out() is called with informative information (error or output)
        '''
        if type(args).__name__ != 'dict':
            out('{"ok" : 0, "errmsg" : "_more must be a GET request"}')
            return
        
        cursor_id = None
        if 'id' in args:
            cursor_id = args['id'][0]
            if cursor_id == None:
                out('{"ok" : 0, "errmsg" : "no cursor id given"}')
                return
        
        if not hasattr(self, "cursors"): 
            out('{"ok" : 0, "errmsg" : "cursor id does not exist"}')
            return
        
        cursors = getattr(self, "cursors")
        if cursor_id not in cursors:
            out('{"ok" : 0, "errmsg" : "cursor id does not exist"}')
            return
        
        query_pointers = cursors[cursor_id]
        
        limit = self.__parse_argument(args, out, "limit", 20, cast=int)
        timeout = self.__parse_argument(args, out, "timeout", 10, cast=int)
        
        errors = []
        if 'errors' in args:
            errors = args["errors"]
        
        end_time = time() + timeout
        responses = []
        responsive = []
        unresponsive = []
        for host, cursor in query_pointers['query'].items():
            if cursor:
                responsive.append(host)
                host_results = Queue()
                host_errors  = Queue()
                host_lock    = Lock()
                proc = Thread(target=self.__parallel_more, 
                              args=(host, cursor, 
                                    limit, host_results, 
                                    host_errors, host_lock))
                responses.append( {"process": proc,
                                   "results": host_results, 
                                   "errors":  host_errors,
                                   "host": host,
                                   "lock": host_lock})
                proc.start()
            else:
                unresponsive.append(host)
        errors.extend(self.__wait_for_data(responses, end_time))
        #moved from the mpq into the query_pointers var,
        #function modifies query_pointers and returns errors
        errors.extend(self.__unload_data(responses, query_pointers)) 
        self.__destroy_subprocesses(responses)
        success = 1
        if len(errors):
            success = 0
        #HOLY COW THIS IS INEFFICIENT, BUT IT WORKS
        sorted_docs = sorted(query_pointers['dereferenced'], 
                             key=lambda k: str(k['_id']), #id ~= time 
                             reverse=True) #return the most recent first
        return_docs = sorted_docs[:limit]
        query_pointers['dereferenced'] = sorted_docs[limit:]
        #end presumably inefficient section / whatever
        out(json.dumps({"results" : return_docs, 
                        "id" : cursor_id, 
                        "ok" : success,
                        "responsive": responsive,
                        "unresponsive": unresponsive,
                        "errors":errors}, default=json_util.default))
        return self._log("more", args)
    
    @staticmethod
    def __wait_for_data(responses, end_time):
        '''wait until after end_time for responses to populate data
        Input
            1. responses: <list> of the following dictionaries
                    {"process": pointer to threading.Thread,
                     "results": pointer to Queue.Queue, 
                     "errors":  pointer to Queue.Queue,
                     "host": str,
                     "lock": pointer to threading.Lock}
            2. end_time: <time.time> 
        Returns:
            errors <list>
        Ensures:
            1. Threads in responses are locked at end_time
        '''
        errors = []
        for response in responses:
            now_time = time()
            timeout = 1
            if now_time > end_time:
                timeout = 1
                LOGGER.error({"msg":"timeout wait: %s" %(response['host'])})
                errors.append("timeout %s" %(response['host']))
            else:
                timeout = end_time - now_time
            LOGGER.warning({"msg":'waiting for thread  %s' %response['host']})
            response['process'].join(timeout)
            LOGGER.warning({"msg":'taking lock on %s' %response['host']})
            response['lock'].acquire()
        return errors
    
    @staticmethod
    def __unload_data(responses, query_pointers):
        '''Takes a list of results from threads and merges them
        into a single <list> object accessable from the main thread
        Input
            1. responses: <list> of the following dictionaries
                    {"process": pointer to threading.Thread,
                     "results": pointer to Queue.Queue, 
                     "errors":  pointer to Queue.Queue,
                     "host": str,
                     "lock": pointer to threading.Lock}
            2. query_pointers: <dict> includes a field
                               query_pointers['dereferenced'] 
        Returns:
            1. errors <list>
        Ensures:
            1. Data from responses['results'] is placed into query_pointers
            2. Data from responses['errors'] merged for return
        '''
        errors = []
        for response in responses:
            #manual locking via thread.lock so .empty() should work
            while not response['results'].empty():
                result = response['results'].get() #one document from mongo
                query_pointers['dereferenced'].append(result)
            while not response['errors'].empty():
                errors.append(response['errors'].get())
        return errors
    
    @staticmethod
    def __destroy_subprocesses(responses):
        '''if there are any threads left alive in responses, 
        explicitly terminate them
        Input
            responses: <list> of the following dictionaries
                    {"process": pointer to threading.Thread,
                     "results": pointer to Queue.Queue, 
                     "errors":  pointer to Queue.Queue,
                     "host": str,
                     "lock": pointer to threading.Lock}
        Returns:
            None
        Ensures:
            All Threads in the responses list are terminated
            and resources freed.
        '''
        for response in responses:
            if response['process'].is_alive():
                LOGGER.warning({"err":'terminating %s' %response['host']})
                response['process'].join()
            
    def __parse_argument(self, args, out, 
                         key, default, 
                         cast=None):
        '''parse a single argument from the dictionary
        assume the only value that matters is the first object
        if the key doesnt exist, return default'''
        value = None
        if key in args:
            if cast:
                value = cast(args[key][0])
            else:
                value = self._get_son(args[key][0], out)
            if value == None:
                raise ValueError("Could not parse %s from args" %(key))
        else:
            value = default
        return value
    
    def __parse_arguments(self, args, out):
        """
        Parses the _find and _count arguments from the provided argument dictionary.
        
        Args:
            args: The query arguments in the form of a dictionary.
            out: An instance of a callable which takes a single string parameter.
                 Used to send data back to the client.
        Returns:
            username
            db_tags
            criteria
            fields
            sort
            limit
            skip
        """
        if type(args).__name__ != 'dict':
            LOGGER.error({"msg":'Invalid arg: did not provide a dictionary'})
            out('{"ok" : 0, "errmsg" : "_find must be a GET request"}')
            return
        
        if "db_tags" not in args:
            LOGGER.error({"msg":'Invalid arguments: db_tags not specified'})
            out('{"ok" : 0, "errmsg" : "_find must specify db_tags"}')
            return
        
        db_tags = self.__parse_argument(args, out, "db_tags", "", cast=str).split(",")
        LOGGER.debug({"msg":'Provided db_tags: %s' %db_tags})
        
        username = self.__parse_argument(args, out, "username", "", cast=str)
        
        # Parse out the criteria/query. User can use either 'criteria'
        # or 'query' keys in order to specify the Mongo query.
        criteria = self.__parse_argument(args, out, "criteria", {})
        criteria = self.__parse_argument(args, out, "query", criteria)
        LOGGER.debug({"msg":'Provided criteria: %s' %criteria})
        # Parse out the fields/projection. User can use either 'fields'
        # or 'projections' keys in order to specify the projection
        # onto the Mongo result set.
        fields = self.__parse_argument(args, out, "fields", None)
        fields = self.__parse_argument(args, out, "projection", fields)
        LOGGER.debug({"msg":'Provided fields: %s' %fields})
        # Parse out the sort information.
        # Sort needs to be provided as an array of tuples.
        # The key of the tuple is the field to sort on within Mongo.
        # The value of the tuple is the direction (1 or -1).
        sort_data = self.__parse_argument(args, out, "sort", None)
        sort = None
        if isinstance(sort_data, dict):
            sort = []
            # Iterate over the sort dictionary.  
            # The key is the value to sort. The value is the direction (1/-1)
            for sort_key, sort_value in sort_data.iteritems():
                LOGGER.debug({"msg":'Sort Key/Value: %s, %s' %(sort_key, 
                                                               sort_value)})
                # Sort is provided as an integer value.
                # A value of 1 indicates ASCENDING, -1 indicates descending.
                sort_order = ASCENDING if sort_value == 1 else DESCENDING
                sort.append((sort_key, sort_order))
        LOGGER.debug({"msg":'Provided sort: %s' %sort})
        limit = self.__parse_argument(args, out, "limit", 20, cast=int)
        LOGGER.debug({"msg":'Provided limit: %s' %limit})
        skip = self.__parse_argument(args, out, "skip", 0, cast=int)
        LOGGER.debug({"msg":'Provided Skip: %s' %skip})
        # Return a tuple of arguments
        return (username, db_tags, criteria, fields, sort, limit, skip)
        
    
    
class MongoFakeStream:
    def __init__(self):
        '''legacy class used by httpd.py
        not applicable to httpf.py (prefered)'''
        self.str = ""

    def ostream(self, content):
        '''legacy class used by httpd.py
        not applicable to httpf.py (prefered)'''
        self.str = self.str + content

    def get_ostream(self):
        '''legacy class used by httpd.py
        not applicable to httpf.py (prefered)'''
        return self.str

class MongoFakeFieldStorage:
    def __init__(self, args):
        '''legacy class used by httpd.py
        not applicable to httpf.py (prefered)'''
        self.args = args

    def getvalue(self, key):
        '''legacy class used by httpd.py
        not applicable to httpf.py (prefered)'''
        return self.args[key]

    def __contains__(self, key):
        '''legacy class used by httpd.py
        not applicable to httpf.py (prefered)'''
        return key in self.args

class FakeFluentSender:
    '''emulates fluent.sender.FluentSender class
    allows the caller to bail out into this class if 
    fluent is unavailable
    '''
    def __init__(self):
        '''emulates the emit function 
        of the fluent.sender.FluentSender class'''
        LOGGER.debug({"msg":'python fluent not installed, using logging'})
    
    @staticmethod
    def emit_with_time(label, timestamp, data):
        '''provides a 'fake' emit function similar
        to python-fluent that simply prints to stdout''' 
        return LOGGER.debug({"msg":"%s:\t%s\t%s" %(label, timestamp, data)})
    
    @staticmethod
    def emit(label, data):
        '''provides a 'fake' emit function similar
        to python-fluent that simply prints to stdout''' 
        return LOGGER.debug({"msg":"%s:\t%s" %(label, data)})
    