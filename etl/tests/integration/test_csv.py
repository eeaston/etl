'''
Created on Mar 31, 2014

@author: Edward Easton
'''
import uuid
import logging

import pytest
import path
from mock import patch, call

from etl import admin, constants
from etl.plugins import csv

logging.basicConfig(level=logging.DEBUG)

pytest_plugins = ['pkglib_testing.fixtures.workspace']
TEST_FILE = path.path(__file__).parent / 'data' / 'test_data.csv'


def test_watcher_needs_update(csv_watcher, workspace):
    test_path = workspace.workspace / 'test.csv'

    # empty db, nothing to update
    assert list(csv_watcher.needs_update()) == []

    admin.add_collector(csv_watcher.conn, constants.CSV_TYPE, csv.CSVConfig({'path': test_path}))

    # No file, nothing to update still
    assert list(csv_watcher.needs_update()) == []

    # Copy in the file, we should now need to update the file
    TEST_FILE.copy(test_path)

    expected = csv.CSVConfig(dict(needs_update=False,
                                  last_updated=None,
                                  updating=None,
                                  path=test_path,
                                  ))
    assert [i.to_primitive() for i in csv_watcher.needs_update()] == [expected.to_primitive()]


def test_updater_needs_update(csv_updater, csv_watcher, workspace):
    test_path = workspace.workspace / 'test.csv'

    # empty db, nothing to update
    assert list(csv_updater.needs_update()) == []

    # Add in a csv entry
    admin.add_collector(csv_updater.conn, constants.CSV_TYPE, csv.CSVConfig({'path': test_path}))

    # No file, nothing to update still
    assert list(csv_updater.needs_update()) == []

    # Copy in the file, and kick off the watcher which should mark the record as needing update
    TEST_FILE.copy(test_path)
    csv_watcher.watch()

    test_uuid = uuid.uuid4()
    expected = csv.CSVConfig(dict(needs_update=True,
                                  last_updated=None,
                                  updating=str(test_uuid),
                                  path=test_path,
                                  ))
    with patch('uuid.uuid4', return_value=str(test_uuid)):
        needs_update = [i.to_primitive() for i in csv_updater.needs_update()]

    assert needs_update == [expected.to_primitive()]


@pytest.mark.skipif(True, reason="TODO: write a test for multiple updaters reading from the same queue")
def test_updater_needs_update_queue(csv_updater, csv_watcher, workspace):
    pass


def test_updater_do_updates(csv_updater, csv_watcher, workspace):
    test_path = workspace.workspace / 'test.csv'
    TEST_FILE.copy(test_path)
    admin.add_collector(csv_updater.conn, constants.CSV_TYPE, csv.CSVConfig({'path': test_path}))
    csv_watcher.watch()
    with patch.object(csv_updater, 'write_one_row') as writer:
        csv_updater.do_updates()

    calls = writer.mock_calls
    assert calls[0] == call(['Bob', 'Sprocket', 'bob@example.com'])
    assert calls[1] == call(['Jill', 'Jamison', 'jill@example.com'])


