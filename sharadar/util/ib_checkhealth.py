from sharadar.live.brokers.ib_broker import TWSConnection
import os

tws = TWSConnection('localhost:4002:123')
tws.bind()

if tws.isConnected():
    tws.disconnect()
    os._exit(os.EX_OK)
else:
    os._exit(os.EX_IOERR)









