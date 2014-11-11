'''
CRUD and other admin interface for collector config

Created on Mar 31, 2014
@author: Edward Easton
'''
import logging

import rethinkdb as r

import constants as const

log = logging.getLogger(__name__)


def add_collector(conn, collector_type, spec):
    """ Add or update a collector in our collector config store

    Parameters
    ----------
    conn:
        ReThink connection
    collector_type: `str`
        Collector type string
    spec: `model.CollectorConfig'
        Config object

    Returns
    -------
    res:
        New item
    """
    spec.validate()
    res = r.db(conn.db).table(collector_type).insert(spec.to_primitive(), upsert=True).run(conn)
    log.info(res)
    return res