import sys
import zmq
from json import dumps

req = dumps({"args":{"id":["53b17ec9cda237676a3cda52"],
                     "db_tags":["metadata"],
                     "criteria":['{"uID6" : {"$exists":true}}'],
                     "limit":["1"]},
             #"func_name": "_hello",
             "func_name": "_more",
             "func_name": "_find",
             "db":"open_metadata",
             "collection":"filesData"})

REQUEST_TIMEOUT = 30 * 1000#ms -> sec
REQUEST_RETRIES = 1
SERVER_ENDPOINT = "tcp://localhost:8585"

context = zmq.Context(1)
client = context.socket(zmq.REQ)
client.connect(SERVER_ENDPOINT)

poll = zmq.Poller()
poll.register(client, zmq.POLLIN)

sequence = 0
retries_left = REQUEST_RETRIES
sequence += 1
request = str(req)
print "I: Sending (%s)" % req
client.send(req)


socks = dict(poll.poll(REQUEST_TIMEOUT))
if socks.get(client) == zmq.POLLIN:
    reply = client.recv()
    print "I: Server replied OK (%s)" % reply


