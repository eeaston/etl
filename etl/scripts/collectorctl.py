"""
Usage: collectorctl [--db_host=DB_HOST] [--db_port=DB_PORT] [--db_name=DB_NAME]
                    (add|remove) csv <path>


Manage collector database

Options:
  -h --help
  -H --db_host=DB_HOST    Database host [default: localhost]
  -p --db_port=DB_PORT    Database port [default: 28015]
  --db_name=DB_NAME       Database name [default: collectors]
"""
import logging

import docopt

from .. import admin, db, constants
from ..plugins import csv


def ctl_csv(conn, args):
    if args['add']:
        admin.add_collector(conn, constants.CSV_TYPE, csv.CSVConfig({'path': args['<path>']}))
    if args['remove']:
        admin.remove_collector(conn, constants.CSV_TYPE, args['<path>'])


# Maps collector type to the above functions to act on that type
CTL_MAP = {"csv": ctl_csv,
           }


def main(argv=None):
    logging.basicConfig(level=logging.DEBUG)
    args = docopt.docopt(__doc__, argv)
    conn = db.connect(args['--db_host'], args['--db_port'], args['--db_name'])
    [CTL_MAP[ctl](conn, args) for ctl in CTL_MAP if args[ctl]]


if __name__ == '__main__':
    main()
