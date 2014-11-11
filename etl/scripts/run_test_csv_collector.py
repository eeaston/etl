"""
Usage: test_csv_collector [--db_host=DB_HOST] [--db_port=DB_PORT] [--db_name=DB_NAME]
                          [--updaters=UPDATERS]

Run up a simple CSV collector that watches changes to CSV files.

Options:
  -h --help
  -H --db_host=DB_HOST    Database host [default: localhost]
  -p --db_port=DB_PORT    Database port [default: 28015]
  --db_name=DB_NAME       Database name [default: collectors]
  --updaters=UPDATERS     Number of updaters to start [default: 1]
"""
import logging

import docopt

from ..plugins import csv
from ..collector import run_collector


def main(argv=None):
    logging.basicConfig(level=logging.DEBUG)
    args = docopt.docopt(__doc__, argv)
    run_collector(db_host=args['--db_host'],
                  db_port=args['--db_port'],
                  db_name=args['--db_name'],
                  watcher_cls=csv.CSVWatcher, watcher_kwargs={},
                  updater_cls=csv.CSVUpdater, updater_kwargs={},
                  num_updaters=args['--updaters'])

if __name__ == '__main__':
    main()