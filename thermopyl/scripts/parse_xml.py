#!/usr/bin/env python
"""
Parse ThermoML XML files in the local ThermoML Archive mirror.

"""
import pandas as pd
import glob, os, os.path
from thermopyl import Parser
import argparse

def main():
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
        XML_PATH = args['path']
    elif 'THERMOML_PATH' in os.environ:
        XML_PATH = os.environ["THERMOML_PATH"]

    # Get path for XML files.
    if args.journalprefix != None:
        filenames = glob.glob("%s/%s*.xml" % (XML_PATH, args.journalprefix))
    else:
        filenames = glob.glob("%s/*.xml" % XML_PATH)

    # Process data.
    data = []
    compound_dict = {}
    for filename in filenames:
        print(filename)
        try:
            parser = Parser(filename)
            current_data = parser.parse()
            current_data = pd.DataFrame(current_data)
            data.append(current_data)
            compound_dict.update(parser.compound_name_to_formula)
        except Exception as e:
            print(e)

    data = pd.concat(data, copy=False, ignore_index=True)  # Because the input data is a list of DataFrames, this saves a LOT of memory!  Ignore the index to return unique index.
    data.to_hdf("%s/data.h5" % XML_PATH, 'data')

    compound_dict = pd.Series(compound_dict)
    compound_dict.to_hdf("%s/compound_name_to_formula.h5" % XML_PATH, 'data')

    return

if __name__ == '__main__':
    main()
