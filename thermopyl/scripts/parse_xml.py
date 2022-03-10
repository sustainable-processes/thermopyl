#!/usr/bin/env python
"""
Parse ThermoML XML files in the local ThermoML Archive mirror.

"""
import pandas as pd
import glob, os, os.path
from thermopyl import Parser
import argparse
import logging

def main():
    logger = logging.getLogger(__name__)

    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description='Build a Pandas dataset from local ThermoML Archive mirror.')
    parser.add_argument('--journalprefix', dest='journalprefix', metavar='JOURNALPREFIX', action='store', type=str, default=None,
                        help='journal prefix to use in globbing *.xml files')
    parser.add_argument('--path', dest='path', metavar='path', action='store', type=str, default=None,
                        help='path to local ThermoML Archive mirror')
    args = parser.parse_args()

    # Get location of local ThermoML Archive mirror.
    XML_PATH = os.path.join(os.environ["HOME"], '.thermoml') # DEFAULT LOCATION
    if args.path != None:
        XML_PATH = args.path
    elif 'THERMOML_PATH' in os.environ:
        XML_PATH = os.environ["THERMOML_PATH"]

    # Get path for XML files.
    if args.journalprefix != None:
        filenames = glob.glob("%s/%s*.xml" % (XML_PATH, args.journalprefix))
    else:
        filenames = glob.glob("%s/*.xml" % XML_PATH)

    # Process data.
    from thermopyl.utils import build_pandas_dataframe
    [data, compound_dict] = build_pandas_dataframe(filenames)
    data.to_parquet("%s/data.pq" % XML_PATH)
    compound_dict.to_parquet("%s/compound_name_to_formula.pq" % XML_PATH)

    return

if __name__ == '__main__':
    main()
