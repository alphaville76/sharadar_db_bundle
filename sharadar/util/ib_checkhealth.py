"""Interactive Brokers TWS/Gateway health check utility.

Verifies that the IB Gateway or TWS is running and accepting
connections on the specified port for a given account.
"""
from sharadar.live.brokers.ib_broker import TWSConnection
import os
import sys
from os.path import basename

def checkhealth(port, account_id):
    """Check if IB Gateway/TWS is running and connectable.

    Args:
        port: TCP port number as string (e.g., '4001').
        account_id: IB account identifier.

    Returns:
        int: os.EX_OK if connected successfully, os.EX_IOERR otherwise.
    """
    tws = TWSConnection('localhost:'+port+':123', account_id)
    tws.bind()

    if tws.isConnected():
        tws.disconnect()
        print("IB Gateway running")
        return os.EX_OK
    else:
        print("IB Gateway NOT running!")
        return os.EX_IOERR

if __name__ == "__main__":

    if len(sys.argv) == 3:
        print("Check if IB gateway is running at port %s for account %s." % (sys.argv[1], sys.argv[2]))
        ex = checkhealth(sys.argv[1], sys.argv[2])
        os._exit(ex)
    else:
        print("Usage:", basename(sys.argv[0]), 'PORT ACCOUNT_ID')
        sys.exit(1)











