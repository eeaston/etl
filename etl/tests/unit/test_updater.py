'''
Created on Apr 10, 2014

@author: Edward Easton
'''
import logging

from mock import Mock, patch, sentinel, call, create_autospec
import rethinkdb as r

from etl import updater

logging.basicConfig(level=logging.DEBUG)


def test_constructor():
    query = Mock()
    self = create_autospec(updater.Updater, items_query=query)
    with patch('rethinkdb.db'):
        updater.Updater.__init__(self, Mock())
    query.filter.assert_called_once_with(r.row['updating'] != None)


def test_needs_update():
    query = Mock()
    table = Mock()
    cfg = Mock()
    i1 = Mock()
    i2 = Mock()
    i3 = Mock()
    self = create_autospec(updater.Updater, items_query=query, chunk_size=sentinel.chunk, conn=Mock(),
                           table=table, config_type=cfg)
    with patch('uuid.uuid4', return_value='test-uuid'):
        self.table.filter.return_value.run.return_value = [i1, i2, i3]
        items = list(updater.Updater.needs_update(self))

    query.filter.return_value.limit.assert_called_once_with(sentinel.chunk)
    query.filter.return_value.limit.return_value.update.assert_called_once_with({'updating': 'test-uuid'})
    self.table.filter.assert_called_once_with(r.row['updating'] == 'test-uuid')
    assert items == [cfg.return_value, cfg.return_value, cfg.return_value]
    assert cfg.mock_calls == [call(i1), call(i2), call(i3)]


def test_do_updates():
    table = Mock()
    dt = Mock()
    dt.datetime.now.return_value = sentinel.now
    i1 = Mock(needs_update=True, updating=True, last_updated=None)
    i2 = Mock(needs_update=True, updating=True, last_updated=None)
    i3 = Mock(needs_update=True, updating=True, last_updated=None)
    self = create_autospec(updater.Updater, table=table, conn=Mock())
    self.needs_update.return_value = [i1, i2, i3]
    with patch.object(updater, 'datetime', dt):
        updater.Updater.do_updates(self)
    assert self.update_one.mock_calls == [call(i1), call(i2), call(i3)]
    assert i1.last_updated == sentinel.now
    assert i1.needs_update == False
    assert i1.updating == None
    assert i2.last_updated == sentinel.now
    assert i2.needs_update == False
    assert i2.updating == None
    assert i3.last_updated == sentinel.now
    assert i3.needs_update == False
    assert i3.updating == None
    assert call(i1.to_primitive.return_value) in table.mock_calls
    assert call(i2.to_primitive.return_value) in table.mock_calls
    assert call(i3.to_primitive.return_value) in table.mock_calls


def test_do_updates_kaboom():
    table = Mock()
    dt = Mock()
    dt.datetime.now.return_value = sentinel.now
    i1 = Mock(needs_update=True, updating=True, last_updated=None)
    i2 = Mock(needs_update=True, updating=True, last_updated=None)
    i3 = Mock(needs_update=True, updating=True, last_updated=None)
    self = create_autospec(updater.Updater, table=table, conn=Mock())
    self.needs_update.return_value = [i1, i2, i3]
    self.update_one.side_effect = [None, ValueError('kaboom'), None]
    with patch.object(updater, 'datetime', dt):
        updater.Updater.do_updates(self)
    assert self.update_one.mock_calls == [call(i1), call(i2), call(i3)]
    assert i1.last_updated == sentinel.now
    assert i1.needs_update == False
    assert i1.updating == None
    assert i2.last_updated == None
    assert i2.needs_update == True
    assert i2.updating == None
    assert i3.last_updated == sentinel.now
    assert i3.needs_update == False
    assert i3.updating == None
    assert call(i1.to_primitive.return_value) in table.mock_calls
    assert call(i2.to_primitive.return_value) in table.mock_calls
    assert call(i3.to_primitive.return_value) in table.mock_calls