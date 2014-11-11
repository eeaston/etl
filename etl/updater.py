'''
Updater base class

Created on Apr 1, 2014

@author: Edward Easton
'''
from abc import ABCMeta, abstractmethod
import logging
import datetime
import traceback
import uuid
import pprint

import rethinkdb as r

from . import collector

log = logging.getLogger(__name__)


class Updater(collector.Collector):
    """ The purpose of the updater is to watch the collector database
        for config items that have been flagged as needing an update,
        and then collecting the data from the upstream source and
        storing it in the collector's datastore.

        Notes
        -----

        * chunk_size:  The number of records processed in each update run
    """
    __metaclass__ = ABCMeta
    chunk_size = 1

    def __init__(self, conn, refresh_interval=5):
        super(Updater, self).__init__(conn, refresh_interval)

        # Make sure that there's no hanging updates lying around from crashed process
        (self.items_query
             .filter(r.row['updating'] != None)
             .update({'updating': None})
             .run(self.conn)
         )

    def needs_update(self):
        """ Return all the items for this type that are flagged as needing update.
            Atomically sets the needs_update flag to False as they are returned
            so that multiple updaters won't pick up the same records.
        """
        # HACK: rethink doesn't support multiple results using 'return_vals', so we store
        #       a uuid on the record as a kind of lock that allows us to find them again.
        # TODO: Make this better..
        update_uuid = str(uuid.uuid4())
        (self.items_query
             .filter((r.row['needs_update'] != False) & (r.row['updating'] == None))
             .limit(self.chunk_size)
             .update({'updating': update_uuid})
             .run(self.conn)
        )
        return (self.config_type(i)
                for i in self.table.filter(r.row['updating'] == update_uuid).run(self.conn))

    def do_updates(self):
        """ Update all the items that need updating
        """
        for item in self.needs_update():
            log.info("Updating item: {}".format(pprint.pformat(item.to_primitive())))
            try:
                self.update_one(item)
                item.last_updated = datetime.datetime.now()
                item.needs_update = False
                item.updating = None
                self.table.update(item.to_primitive()).run(self.conn)
            except:
                log.error("Update failed for item {}".format(item.to_primitive()))
                log.error(traceback.format_exc())
                item.needs_update = True
                item.updating = None
                self.table.update(item.to_primitive()).run(self.conn)

    @abstractmethod
    def update_one(self, item):
        """ Update a single item from its source.
        """
