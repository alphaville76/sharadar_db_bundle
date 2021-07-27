from sharadar.live.brokers.ib_broker import TWSConnection
import os
import sys
from os.path import basename

def checkhealth(port):
    tws = TWSConnection('localhost:'+port+':123')
    tws.bind()

    if tws.isConnected():
        tws.disconnect()
        return os.EX_OK
    else:
        return os.EX_IOERR

if __name__ == "__main__":

    if len(sys.argv) == 2:
        print("Algo name:", sys.argv[1])
        ex = checkhealth(sys.argv[1])
        os._exit(ex)
    else:
        print("Usage:", basename(sys.argv[0]), 'PORT')
        sys.exit(1)











