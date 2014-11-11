'''
Created on Apr 11, 2014

@author: Edward Easton
'''
from mock import Mock, patch, sentinel, call, create_autospec

from etl import admin


def test_add_collector():
    conn = Mock()
    type_ = sentinel.type
    spec = Mock()
    with patch('rethinkdb.db') as db:
        res = admin.add_collector(conn, type_, spec)
    spec.validate.assert_called_once_with()
    db.assert_called_once_with(conn.db)
    db.return_value.table.assert_called_once_with(type_)
    db.return_value.table.return_value.insert.assert_called_once_with(spec.to_primitive.return_value,
                                                                      upsert=True)
    assert res == db.return_value.table.return_value.insert.return_value.run.return_value


