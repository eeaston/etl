"""  Watcher baseclass
"""
import logging
from abc import ABCMeta, abstractmethod
import pprint
import traceback

from . import collector

log = logging.getLogger(__name__)


class Watcher(collector.Collector):
    """ The purpose of a watcher is to watch an upstream datasource and set a flag on
        the collector's config that it needs to go and collect some more data.

        Parameters
        ----------
        db: Database object
            database handle
        type: `str`
            String identifier for this type of collector
    """
    __metaclass__ = ABCMeta

    def watch(self):
        """ Flag all the items needing update
        """
        try:
            for item in self.needs_update():
                if item.needs_update:
                    # No need to re-flag here
                    continue
                log.info("Flagging data item for update:")
                log.info(pprint.pformat(item.to_primitive()))
                item.needs_update = True
                self.table.update(item.to_primitive()).run(self.conn)
        except:
            log.error("Couldn't determine sources to update")
            log.error(traceback.format_exc())

    @abstractmethod
    def needs_update(self):
        """ Child classes use this method to tell the watcher if the upstream source
            requires updating.

            Returns
            -------
            result: `list` of db items to update
        """