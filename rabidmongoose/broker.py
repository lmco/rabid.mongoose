'''
rabid.mongoose broker
follows the Paranoid Pirate Pattern from ZeroMQ
'''
from time import time
import zmq
from collections import OrderedDict
from rabidmongoose.config import BROKERFRONT, BROKERBACK, LOGTAG, FLUENT, \
                                 HEARTBEAT_LIVENESS, HEARTBEAT_INTERVAL
import logging
try:
    import ujson as json
except ImportError:
    import json


FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.ERROR, format=FORMAT)
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
CURSOR_LIMIT = 1000


class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()
        self.cursors = OrderedDict()
        self.working = OrderedDict() #TODO: Keep Track of workers
        
    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker
        
    def purge(self):
        """Look for & kill expired workers."""
        now_time = time()
        expired = []
        for address, worker in self.queue.iteritems():
            if now_time > worker.expiry: # Worker expired
                expired.append(address)
        for address in expired:
            LOGGER.debug( "W: Idle worker expired: %s" % address )
            self.queue.pop(address, None)
    
    def next(self):
        address = None
        try:
            address, worker = self.queue.popitem(False)
        except KeyError:
            LOGGER.error({"errmsg":"No workers available"})
        return address
    
    def get_worker(self, cursor_id):
        LOGGER.debug("Looking for %s in %s" %(cursor_id, str(self.cursors)))
        address = self.cursors.get(cursor_id, None)
        if address:
            del(self.cursors[cursor_id])
        if not address:
            address = self.next()
        return address
    
    def add_cursor(self, cursor_id, address):
        if len(self.cursors) > CURSOR_LIMIT:
            self.cursors.popitem(False)
        #TODO: EXPIRE CURSORS HERE?
        self.cursors[cursor_id] = address

class Broker(object):
    def __init__(self, **kwargs):
        self.config = kwargs
        
    @staticmethod
    def heartbeat(heartbeat_at, backend, workers):
        if time() >= heartbeat_at:
            for worker in workers.queue:
                msg = [worker, PPP_HEARTBEAT]
                backend.send_multipart(msg)
            heartbeat_at = time() + HEARTBEAT_INTERVAL
        return heartbeat_at
    @staticmethod
    def log_id(workers, address, msg):
        resp_data = json.loads(msg[2])
        if "id" in resp_data:
            workers.add_cursor(resp_data["id"], address)
        return
    
    @staticmethod
    def is_more_cursor(frames):
        cursor_id = None
        try:
            req_data = json.loads(frames[2])
            if "_more" in req_data['func_name']:
                cursor_id = req_data["args"]['id'][0]
        except:
            cursor_id = None
        return cursor_id
    
    
    def handle_backend(self, frontend, backend, workers):
        frames = backend.recv_multipart()
        if not frames:
            raise TypeError
        address = frames[0]
        workers.ready(Worker(address))
        # Validate control message, or return reply to client
        msg = frames[1:]
        if len(msg) == 1:
            if msg[0] not in (PPP_READY, PPP_HEARTBEAT):
                LOGGER.error( "E: Invalid message: %s" % msg)
        else:
            self.log_id(workers, address, msg)
            frontend.send_multipart(msg)
    
    def handle_frontend(self, frontend, backend, workers):
        frames = frontend.recv_multipart()
        if not frames:
            raise TypeError
        cursor_id = self.is_more_cursor(frames)
        worker_id = None
        if cursor_id:
            worker_id = workers.get_worker(cursor_id)
        else:
            worker_id = workers.next()
        frames.insert(0, worker_id)
        backend.send_multipart(frames)
        
    def start(self):
        context = zmq.Context(1)
        frontend = context.socket(zmq.ROUTER) # ROUTER
        backend = context.socket(zmq.ROUTER) # ROUTER
        frontend.bind(BROKERFRONT) # For clients
        backend.bind(BROKERBACK) # For workers
        poll_workers = zmq.Poller()
        poll_workers.register(backend, zmq.POLLIN)
        poll_both = zmq.Poller()
        poll_both.register(frontend, zmq.POLLIN)
        poll_both.register(backend, zmq.POLLIN)
        workers = WorkerQueue()
        heartbeat_t = time() + HEARTBEAT_INTERVAL
        while True:
            if len(workers.queue) < 5:
                LOGGER.critical({"critical":"queue size low (%s)" %(len(workers.queue))})
            if len(workers.queue) > 0:
                poller = poll_both
            else:
                poller = poll_workers
            socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
            try:
                if socks.get(backend) == zmq.POLLIN:
                    self.handle_backend(frontend, backend, workers)
                    heartbeat_t = self.heartbeat(heartbeat_t, backend, workers)
                if socks.get(frontend) == zmq.POLLIN:
                    self.handle_frontend(frontend, backend, workers)
            except TypeError:
                break
            workers.purge()
            
def start():
    Broker().start()
    
if __name__ == "__main__":
    start()