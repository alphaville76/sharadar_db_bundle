from sharadar.live.brokers.ib_broker import TWSConnection
import sys
import os

tws = TWSConnection('localhost:4002:123')
tws.bind()

if tws.isConnected():
    sys.exit(os.EX_OK)
else:
    sys.exit(os.EX_IOERR)










