from __future__ import absolute_import

import glob
import itertools
import datetime
import logging
import csv

import path
from schematics.types import StringType

from ..model import CollectorConfig
from ..watcher import Watcher
from ..updater import Updater
from .. import constants


log = logging.getLogger(__name__)


class CSVConfig(CollectorConfig):
    path = StringType(required=True)
    primary_key = 'path'


class CSVCollector(object):
    collector_type = constants.CSV_TYPE
    config_type = CSVConfig


class CSVWatcher(CSVCollector, Watcher):
    """ Watches CSV files for changes, then parses them for updates.
    """

    def __init__(self, conn, refresh_interval=5):
        super(CSVWatcher, self).__init__(conn, refresh_interval)

    def _needs_update(self, item):
        """ Returns all the files paths that need updating
        """
        files = (path.path(i) for i in glob.glob(item.path))
        return [i for i in files
                if item.last_updated is None
                or datetime.datetime.fromtimestamp(i.getmtime()) > item.last_updated
                ]

    def needs_update(self):
        """ Returns all items where their files exist and have mtime > last_updated
        """
        return itertools.ifilter(self._needs_update, self.items)


class CSVUpdater(CSVCollector, Updater):
    """ Watches CSV files for changes, then parses them for updates.
    """
    def __init__(self, conn, refresh_interval=5, has_header=True, **reader_kwargs):
        super(CSVUpdater, self).__init__(conn, refresh_interval)
        self.has_header = has_header
        self.reader_kwargs = reader_kwargs

    def update_one(self, item):
        """ Update a single item from CSV
        """
        for filepath in (path.path(i) for i in glob.glob(item.path)):
            with filepath.open('rb') as fp:
                reader = csv.reader(fp, **self.reader_kwargs)
                if self.has_header:
                    reader.next()
                for row in reader:
                    self.write_one_row(row)

    def write_one_row(self, row):
        """ Write out a single row. Child classes should implement this. The reference
            example just prints them out.
        """
        log.info("Write row: {}".format(row))




