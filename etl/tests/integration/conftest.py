'''
Created on Mar 31, 2014

@author: Edward Easton
'''
import pytest

from etl.plugins import csv

pytest_plugins = ['pkglib_testing.fixtures.server.rethink']

@pytest.fixture(scope='module')
def rethink_tables():
    """ Defines which tables get created at module scope and
        emptied at the start of each test.
    """
    return [(csv.CSVCollector.collector_type, csv.CSVConfig.primary_key)]


@pytest.fixture(scope='function')
def conn(rethink_tables, rethink_empty_db):
    """ Provides a handle to a RethinkDB server with a unique
        database for the current test module, and tables created as
        specified in rethink_tables
    """
    return rethink_empty_db


@pytest.fixture()
def csv_watcher(rethink_tables, rethink_empty_db):
    return csv.CSVWatcher(rethink_empty_db)


@pytest.fixture()
def csv_updater(rethink_tables, rethink_empty_db):
    return csv.CSVUpdater(rethink_empty_db)
