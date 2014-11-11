'''
Created on Apr 10, 2014

@author: Edward Easton
'''
from mock import Mock, patch, sentinel, call, create_autospec
import rethinkdb as r

from etl import collector, model


class TestConfig(model.CollectorConfig):
    primary_key = 'foo'


class TestColl(collector.Collector):
    collector_type = sentinel.ctype
    config_type = TestConfig


def test_constructor():
    conn = Mock()
    with patch('rethinkdb.db') as db:
        c = TestColl(conn, refresh_interval=sentinel.refresh)
    db.assert_called_with(conn.db)
    assert db.return_value.table_create.mock_calls[0] == call(sentinel.ctype, primary_key='foo')
    assert c.table == db.return_value.table(sentinel.ctype)
    assert c.refresh_interval == sentinel.refresh


def test_constructor_table_exists():
    conn = Mock()
    class Err(r.RqlRuntimeError):
        def __init__(self):
            pass
        def __str__(self):
            return "kaboom"
    with patch('rethinkdb.db') as db, patch('etl.collector.log') as log:
        db.return_value.table_create.side_effect = Err
        TestColl(conn, refresh_interval=sentinel.refresh)
    assert "Table already exists" in log.info.mock_calls[0][1][0]


def test_items():
    conn = Mock()
    cfg = Mock()

    class TestColl(collector.Collector):
        collector_type = sentinel.ctype
        config_type = cfg

    with patch('rethinkdb.db') as db:
        db.return_value.table.return_value.run.return_value = [sentinel.i1, sentinel.i2, sentinel.i3]
        c = TestColl(conn)
    items = list(c.items)
    assert items == [cfg.return_value] * 3
    assert cfg.mock_calls == [call.get_pk(),
                              call(sentinel.i1),
                              call(sentinel.i2),
                              call(sentinel.i3),
                              ]

def test_run_forever():
    self = create_autospec(collector.Collector, refresh_interval=sentinel.refresh, halt=False)
    fn = Mock()
    with patch('os.getppid') as pid, patch('time.sleep') as sleep:
        pid.side_effect = [100, 100, 1, ValueError("Shouldn't get here!")]
        collector.Collector.run_forever(self, fn)
    assert fn.mock_calls == [call(), call(), call()]
    assert sleep.mock_calls == [call(sentinel.refresh), call(sentinel.refresh)]


def test_run_collectors():
    def sleep(n):
        raise ZeroDivisionError()

    with patch('time.sleep', sleep), patch('etl.collector.run_collector') as run:
        try:
            collector.run_collectors(sentinel.host, sentinel.port, sentinel.name,
                                     [{'watcher': sentinel.watcher1,
                                       'watcher_kwargs': sentinel.watcher_kwargs1,
                                       'updater': sentinel.updater1,
                                       'updater_kwargs': sentinel.updater_kwargs1,
                                       'num_updaters': sentinel.num1,
                                       },
                                      {'watcher': sentinel.watcher2,
                                       'watcher_kwargs': sentinel.watcher_kwargs2,
                                       'updater': sentinel.updater2,
                                       'updater_kwargs': sentinel.updater_kwargs2,
                                       'num_updaters': sentinel.num2,
                                       },
                                      ])
        except ZeroDivisionError:
            pass

    assert run.mock_calls == [call(sentinel.host, sentinel.port, sentinel.name,
                                   sentinel.watcher1, sentinel.watcher_kwargs1,
                                   sentinel.updater1, sentinel.updater_kwargs1,
                                   sentinel.num1),
                              call(sentinel.host, sentinel.port, sentinel.name,
                                   sentinel.watcher2, sentinel.watcher_kwargs2,
                                   sentinel.updater2, sentinel.updater_kwargs2,
                                   sentinel.num2)
                              ]

def test_run_collector_im_the_watcher():
    with patch('os.fork') as fork, patch('etl.db.connect') as connect:
        fork.return_value = 0
        watcher = Mock()
        watcher_kwargs = {'a': 1, 'b': 2}
        try:
            collector.run_collector(sentinel.host, sentinel.port, sentinel.name,
                                    watcher, watcher_kwargs,
                                    sentinel.updater, sentinel.updater_kwargs)
        except SystemExit:
            pass
        connect.assert_called_once_with(sentinel.host, sentinel.port, sentinel.name)
        watcher.assert_called_with(connect.return_value, a=1, b=2)
        watcher.return_value.run_forever.assert_called_with(watcher.return_value.watch)


def test_run_collector_im_the_updater():
    with patch('os.fork') as fork, patch('etl.db.connect') as connect:
        fork.side_effect = [100, 100, 0]
        updater = Mock()
        updater_kwargs = {'a': 1, 'b': 2}
        try:
            collector.run_collector(sentinel.host, sentinel.port, sentinel.name,
                                    sentinel.watcher, sentinel.watcher_kwargs,
                                    updater, updater_kwargs, num_updaters=2)
        except SystemExit:
            pass
        connect.assert_called_once_with(sentinel.host, sentinel.port, sentinel.name)
        updater.assert_called_with(connect.return_value, a=1, b=2)
        updater.return_value.run_forever.assert_called_with(updater.return_value.do_updates)
        assert fork.mock_calls == [call(), call(), call()]


