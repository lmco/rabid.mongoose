MONGOR = "mongodb://localhost:27018"
MONGOR_SSL = False
ZMQCLIENT = "tcp://127.0.0.1:8585"
BROKERFRONT = "tcp://0.0.0.0:8585"
BROKERBACK = "tcp://0.0.0.0:8586"
FLUENT = "localhost:24224"
LOGTAG = "rabidmongoose"
HEARTBEAT_LIVENESS = 3
HEARTBEAT_INTERVAL = 60 # Seconds
INTERVAL_INIT = 1
INTERVAL_MAX = 32
TIMEOUT = 600 * 1000 #seconds

# Paranoid Pirate Protocol constants
PPP_READY = "\x01" # Signals worker is ready
PPP_HEARTBEAT = "\x02" # Signals worker heartbeat
CURSOR_LIMIT = 1000

