import os
from pkg_resources import resource_filename
import logging

def get_fn(name):
    """Get the full path to one of the reference files shipped for testing

    In the source distribution, these files are in ``thermopyl/data``,
    but on installation, they're moved to somewhere in the user's python
    site-packages directory.

    Parameters
    ----------
    name : str
        Name of the file to load (with respect to the reference/ folder).
    """

    fn = resource_filename('thermopyl', os.path.join('data', name))

    if not os.path.exists(fn):
        raise ValueError('Sorry! %s does not exists. If you just '
            'added it, you\'ll have to re install' % fn)

    return fn

def make_path(filename):
    try:
        path = os.path.split(filename)[0]
        os.makedirs(path)
    except OSError:
        pass

def build_pandas_dataframe(filenames):
    """
    Build pandas dataframe for property data and compounds.

    Parameters
    ----------
    filenames : list
        List of ThermoML filenames to process.

    Returns
    -------
    data : pandas DataFrame
        Compiled ThermoML dataframe
    compounds : pandas Dataframe
        Compounds dataframe

    """
    logger = logging.getLogger(__name__)
    import pandas as pd
    from thermopyl import Parser

    data = []
    compound_dict = {}
    for filename in filenames:
        logger.info(f"Parsing {filename}")
        try:
            parser = Parser(filename)
            current_data = parser.parse()
            current_data = pd.DataFrame(current_data)
            data.append(current_data)
            compound_dict.update(parser.compound_name_to_formula)
        except Exception as e:
            logger.error(e)

    data = pd.concat(data, copy=False, ignore_index=True)  # Because the input data is a list of DataFrames, this saves a LOT of memory!  Ignore the index to return unique index.
    compounds = pd.Series(compound_dict).to_frame().rename(columns={0: "formula"})
    return [data, compounds]

def pandas_dataframe(thermoml_path=None):
    """Read the ThermoPyL dataset into a Pandas dataframe.

    Parameters
    ----------
    thermoml_path : str, optional, default=None
        If specified, search here for the `data.h5` file compiled by `thermoml-build-pandas`.
        If None, will try environment variable `THERMOML_PATH` followed by `$HOME/.thermopyl`

    Returns
    -------
    df : pandas.core.frame.DataFrame
        pandas dataframe containing ThermoML data

    """
    import os, os.path
    if thermoml_path is None:
        # Try to obtain the path to the local ThermoML Archive mirror from an environment variable.
        if 'THERMOML_PATH' in os.environ:
            # Check THERMOML_PATH environment variable
            parquet_filename = os.path.join(os.environ["THERMOML_PATH"], 'data.pq')
        else:
            # Use default path of ~/.thermoml
            parquet_filename = os.path.join(os.environ["HOME"], '.thermoml', 'data.pq')
    else:
        parquet_filename = os.path.join(thermoml_path, 'data.pq')

    if not os.path.exists(parquet_filename):
        if thermoml_path is None:
            msg  = 'Could not find `data.h5` in either $THERMOML_PATH or ~/.thermoml\n'
            msg += 'Make sure you have run `thermoml-build-pandas` and it has completed successfully'
        else:
            msg  = 'Could not find `data.pq` in specified path `%s`' % thermoml_path
        raise Exception(msg)

    import pandas as pd
    df = pd.read_parquet(parquet_filename)

    return df
