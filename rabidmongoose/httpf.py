'''
Rabid.Mongoose Webservice/Client
follows the Paranoid Pirate Pattern from ZeroMQ
'''
from flask import Flask, request, make_response
from rabidmongoose.config import ZMQCLIENT, TIMEOUT
import logging
from json import dumps
import zmq

APP = Flask(__name__)
APP.config.from_object('config')
MIME_TYPE = "application/json; charset=UTF-8"
FORMAT = '%(asctime)s %(levelname)-8s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
LOGGER = logging.getLogger(__name__)
ERROR = 404
SUCCESS = 200

def send_to_worker(args, func_name, db_type, collection):
    '''packages data and sends it to the broker
    Input
        1. args: <dict> 
        2. func_name: <dict>
        3. db_type: <str>
        4. collection <str> 
    Returns:
        mongoose_response <str> (JSON)
    '''
    #create client
    context = zmq.Context(1)
    client = context.socket(zmq.REQ)
    client.connect(ZMQCLIENT)
    poll = zmq.Poller()
    poll.register(client, zmq.POLLIN)
    #serialize request to JSON
    req = dumps({"args":args,
                 "func_name": func_name,
                 "db":db_type,
                 "db_type":db_type,
                 "collection":collection})
    #get the response from broker
    client.send(req)
    socks = dict(poll.poll(TIMEOUT))
    mongoose_response = ""
    if socks.get(client) == zmq.POLLIN:
        mongoose_response = client.recv()
    #destroy client
    client.setsockopt(zmq.LINGER, 0)
    client.close()
    poll.unregister(client)
    context.term()
    return mongoose_response

@APP.route('/<db_type>/<collection>/<func_name>', methods=['POST', 'GET'])
def handle_request(db_type, collection, func_name):
    '''
    Handles the request
    Uses the URL formatting from Sleepy.Mongoose
    https://github.com/10gen-labs/sleepy.mongoose
    '''
    try: 
        username = str(request.environ['REMOTE_USER']).lower()
    except:
        username = ""
    args = {} 
    for k in request.args: #make this backward compatible with httpd.py
        args[k] = request.args.getlist(k) 
    args["username"] = [username] #every other arg is a list, make this list
    resstr = send_to_worker(args, func_name, db_type, collection)
    if "callback" in args:
        response = make_response("%s(%s)" %(args['callback'][0], resstr))
    else:
        response = make_response(resstr)
    response.headers['Content-Type'] = MIME_TYPE
    return response, SUCCESS


if __name__ == '__main__':
    APP.run(debug=True,
            processes=5, 
            use_reloader=False)
