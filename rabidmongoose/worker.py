'''
rabid.mongoose worker
follows the Paranoid Pirate Pattern from ZeroMQ
'''
from random import randint
import time
import zmq
from rabidmongoose.handlers import MongoHandler
from rabidmongoose.config import MONGOR, TIMEOUT, FLUENT, LOGTAG, \
                                 HEARTBEAT_LIVENESS, HEARTBEAT_INTERVAL, \
                                 INTERVAL_INIT, INTERVAL_MAX
from StringIO import StringIO
import logging
from uuid import uuid4
try:
    from ujson import loads
except ImportError:
    from json import loads

FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
LOGGER = logging.getLogger(__name__)
try:
    from fluent.handler import FluentHandler
    LOGGER.addHandler(FluentHandler("%s.debug" %(LOGTAG), 
                                    host=FLUENT.split(":")[0], 
                                    port=int(FLUENT.split(":")[1])))
except ImportError:
    pass #oh well, stdout is ok

# Paranoid Pirate Protocol constants
PPP_READY = "\x01" # Signals worker is ready
PPP_HEARTBEAT = "\x02" # Signals worker heartbeat


class MongooseHandler:
    def __init__(self, name=None):
        self.name = name or str(uuid4())
        self.mongo_handler = MongoHandler(MONGOR, 
                             cursor_timeout = TIMEOUT,
                             fluentd = FLUENT,
                             logtag = LOGTAG)
        self.cursors = set()
        
    def close(self):
        return
        
    def handle_request(self, args, func_name, db_type, collection):
        response_content = StringIO()
        out = response_content.write
        func = getattr(self.mongo_handler, func_name, None)
        LOGGER.debug({"debug":"sending to %s" %(func_name)})
        if callable(func):
            func(args, out, 
                 name = "", #TODO: remove this, around since sleepy.mongoose
                 db = db_type, 
                 collection = collection)
        else:
            out('{"ok" : 0, "errmsg" : "%s not callable"}' %(func_name))
        current_cursors = set(self.mongo_handler.available_cursors())
        return response_content.getvalue(), current_cursors




def worker_socket(context, poller):
    """Helper function that returns a new configured socket
    connected to the Paranoid Pirate queue"""
    worker = context.socket(zmq.DEALER) # DEALER
    identity = "%04X-%04X" % (randint(0, 0x10000), randint(0, 0x10000))
    worker.setsockopt(zmq.IDENTITY, identity)
    poller.register(worker, zmq.POLLIN)
    worker.connect("tcp://localhost:8586")
    worker.send(PPP_READY)
    return worker

def handle_request(request_data):
    LOGGER.debug({"debug":"got request %s" %(request_data)})
    try:
        job_data = loads(request_data)
        (resp, cursors) = MH.handle_request(job_data['args'], 
                                            job_data['func_name'], 
                                            job_data['db'], 
                                            job_data['collection'])
    except ValueError:
        resp = '{"ok" : 0, "errmsg" : "cannot parse input JSON"}'
    except KeyError:
        resp =  '{"ok" : 0, "errmsg" : "required keys not provided"}'
    except:
        resp =  '{"ok" : 0, "errmsg" : "Unknown error in request worker"}'
    return resp

def start():
    global MH 
    MH = MongooseHandler()
    context = zmq.Context(1)
    poller = zmq.Poller()
    liveness = HEARTBEAT_LIVENESS
    interval = INTERVAL_INIT
    heartbeat_at = time.time() + HEARTBEAT_INTERVAL
    worker = worker_socket(context, poller)
    while True:
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
        # Handle worker activity on backend
        if socks.get(worker) == zmq.POLLIN:
            # Get message
            # - 3-part envelope + content -> request
            # - 1-part HEARTBEAT -> heartbeat
            frames = worker.recv_multipart()
            if not frames:
                break # Interrupted
            if len(frames) == 3:
                frames[2] = handle_request(frames[2])
                worker.send_multipart(frames)
                liveness = HEARTBEAT_LIVENESS
            elif len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
                liveness = HEARTBEAT_LIVENESS
            else:
                LOGGER.error( {"errmsg": "Invalid message: %s" % frames })
                interval = INTERVAL_INIT
        else:
            liveness -= 1
            if liveness == 0:
                LOGGER.error({"errmsg": "Heartbeat failure",
                              "reconnect": "Reconnect in %0.2fs" % interval})
                time.sleep(interval)
                
                if interval < INTERVAL_MAX:
                    interval *= 2
                poller.unregister(worker)
                worker.setsockopt(zmq.LINGER, 0)
                worker.close()
                worker = worker_socket(context, poller)
                liveness = HEARTBEAT_LIVENESS
        if time.time() > heartbeat_at:
            heartbeat_at = time.time() + HEARTBEAT_INTERVAL
            worker.send(PPP_HEARTBEAT)

if __name__ == "__main__":
    start()