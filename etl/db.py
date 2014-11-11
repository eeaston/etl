'''
Created on Apr 2, 2014

@author: Edward Easton
'''
import socket
import logging

import rethinkdb as r

log = logging.getLogger(__name__)


def connect(host, port, db):
    """ Return Rethink Connection for given host, port and db name
    """
    host = socket.gethostbyname(host)
    port = int(port)
    log.info("Connecting to ReThink at {}:{}@{}".format(host, port, db))
    return r.connect(host=host, port=port, db=db)
