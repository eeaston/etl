"""  Watcher baseclass
"""
import os
import sys
import logging
from abc import ABCMeta
import time

import rethinkdb as r

from . import db

log = logging.getLogger(__name__)


class Collector(object):
    """ Base class for watchers and collectors

        Parameters
        ----------
        db: Database object
            database handle
        type: `str`
        refresh_interval: `int`
            Poll refresh interval for reading the collector database


        Notes
        -----
        All collectors have the following class attributes:
        * collector_type: String identifier for this type of collector.
                          This will be used as the table name
        * db_keys: List of unique keys. Primary key is the first one in the list.
                   Keys can be a string (single) or tuple (compound).
                   In the case of compound keys, the first item is the name of the
                   key and the rest are the key components
    """
    __metaclass__ = ABCMeta

    collector_type = None
    config_type = None

    def __init__(self, conn, refresh_interval=5):
        self.halt = False
        self.conn = conn
        db = r.db(conn.db)
        try:
            db.table_create(self.collector_type,
                            primary_key=self.config_type.get_pk()).run(self.conn)
            log.info("Created collectors table: {}".format(self.collector_type))
        except r.RqlRuntimeError as e:
            log.info("Table already exists: {}".format(e))
            pass

        self.table = db.table(self.collector_type)
        self.refresh_interval = refresh_interval

    def run_forever(self, fn):
        """ Run a watch loop forever
        """
        while True:
            fn()
            # When our parent PID is 1, the controller process has died and so should we
            if self.halt or os.getppid() == 1:
                break
            time.sleep(self.refresh_interval)

    @property
    def items_query(self):
        """ Query that returns all the CollectorConfig items for this collector.
            By default we return all the entries in the table with the same name
            as this collector's collector_type attribute.
        """
        return self.table

    @property
    def items(self):
        """ All config items for this collectors type
        """
        return (self.config_type(i) for i in self.items_query.run(self.conn))

    def diff_dict(self, a_dict, b_dict):
        """ Compare two data dictionaries. Returns True if they are different.
            Used for determining if a record should be updated in the database.
        """
        if set(a_dict.keys()) != set(b_dict.keys()):
            return True
        for k in a_dict:
            # TODO: numeric precision for floats, nested dictionaries etc etc
            if a_dict[k] != b_dict[k]:
                return True
        return False


def run_collectors(db_host, db_port, db_name, collectors):
    """ Run up instances of the collectors with the configured watcher and updaters.

        Parameters
        ----------
        collectors: list of `dict`
            List of of ::
                { watcher: watcher class
                  watcher_kwargs:  watcher kwargs,
                  updater: updater class
                  updater_kwargs:  updater kwargs,
                  num_updaters: number of updaters
                }
    """
    for collector in collectors:
        run_collector(db_host, db_port, db_name,
                      collector['watcher'], collector['watcher_kwargs'],
                      collector['updater'], collector['updater_kwargs'],
                      collector['num_updaters']
                      )
    while True:
        # TODO: do some signal handling here?
        time.sleep(5)


def run_collector(db_host, db_port, db_name,
                  watcher_cls, watcher_kwargs, updater_cls, updater_kwargs,
                  num_updaters=1):
    """ Run up instances of the watcher and updaters with the configured watcher and updaters.
    """
    num_updaters = int(num_updaters)
    log.info("Starting {} watcher and {} {} updaters".format(watcher_cls, num_updaters, updater_cls))

    pid = os.fork()
    if pid == 0:
        # We're now the watcher
        log.info("Starting watcher PID {}".format(os.getpid()))
        conn = db.connect(db_host, db_port, db_name)
        watcher = watcher_cls(conn, **watcher_kwargs)
        watcher.run_forever(watcher.watch)
        log.info("Watcher shutting down PID {}".format(os.getpid()))
        sys.exit(0)

    for i in xrange(num_updaters):
        pid = os.fork()
        if pid == 0:
            # We're now the updater
            log.info("Starting updater {} of {} PID: {}".format(i + 1, num_updaters, os.getpid()))
            conn = db.connect(db_host, db_port, db_name)
            updater = updater_cls(conn, **updater_kwargs)
            updater.run_forever(updater.do_updates)
            log.info("Updater shutting down PID {}".format(os.getpid()))
            sys.exit(0)
