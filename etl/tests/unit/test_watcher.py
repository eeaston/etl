import logging

from mock import Mock, patch, sentinel, call, create_autospec

from etl import watcher

logging.basicConfig(level=logging.DEBUG)


def test_watch():
    table = Mock()
    conn = Mock()
    self = create_autospec(watcher.Watcher, table=table, conn=conn)
    i1 = Mock(needs_update=False)
    i2 = Mock(needs_update=True)
    i3 = Mock(needs_update=False)

    self.needs_update.return_value = [i1, i2, i3]
    watcher.Watcher.watch(self)
    assert i1.needs_update == True
    assert i2.needs_update == True
    assert i3.needs_update == True
    assert call(i1.to_primitive.return_value) in self.table.update.mock_calls
    assert call(i2.to_primitive.return_value) not in self.table.update.mock_calls
    assert call(i3.to_primitive.return_value) in self.table.update.mock_calls


def test_watch_kaboom():
    table = Mock()
    conn = Mock()
    self = create_autospec(watcher.Watcher, table=table, conn=conn)
    self.needs_update.side_effect = ValueError("kaboom")
    with patch('etl.watcher.log') as log:
        watcher.Watcher.watch(self)
    assert "Couldn't determine sources to update" in log.error.mock_calls[0][1][0]