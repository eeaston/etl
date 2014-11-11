'''
Created on Apr 11, 2014

@author: Edward Easton
'''
from mock import Mock, patch, sentinel, call, create_autospec

from etl import db


def test_connect():
    with patch('socket.gethostbyname') as sock, patch('rethinkdb.connect') as conn:
        db.connect(sentinel.host, '12345', sentinel.db)
    sock.assert_called_once_with(sentinel.host)
    conn.asssert_called_once_with(host=sock.return_value, port=12345, db=sentinel.db)
