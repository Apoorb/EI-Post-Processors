"""General utility functions."""
from io import StringIO
import cProfile
import pstats
from functools import wraps
import pandas as pd
import numpy as np
import datetime as dt
import logging as lg
import sys
from pathlib import Path
from sqlalchemy import create_engine
import pkg_resources
import yaml
from pint import UnitRegistry

package_name = "ttionroadei"
try:
    # Get the path to the settings.yaml file within the package
    settings_yaml_path = pkg_resources.resource_filename(package_name, "settings.yaml")
    # Read and parse the YAML file
    with open(settings_yaml_path, "r") as yaml_file:
        settings = yaml.safe_load(yaml_file)
except Exception as e:
    print(f"Error: {e}")

log_console = settings.get("log_console")
log_file = settings.get("log_file")
log_filename = settings.get("log_filename")
log_level = settings.get("log_level")
mvs4defaultdb = settings.get("MOVES4_Default_DB")
valid_units = settings.get("valid_units")


def profile(
    output_file=None, sort_by="cumulative", lines_to_print=10, strip_dirs=False
):
    """A time profiler decorator.
    Inspired by and modified the profile decorator of Giampaolo Rodola:
    http://code.activestate.com/recipes/577817-profile-decorator/
    Args:
        output_file: str or None. Default is None
            Path of the output file. If only name of the file is given, it's
            saved in the current directory.
            If it's None, the name of the decorated function is used.
        sort_by: str or SortKey enum or tuple/list of str/SortKey enum
            Sorting criteria for the Stats object.
            For a list of valid string and SortKey refer to:
            https://docs.python.org/3/library/profile.html#pstats.Stats.sort_stats
        lines_to_print: int or None
            Number of lines to print. Default (None) is for all the lines.
            This is useful in reducing the size of the printout, especially
            that sorting by 'cumulative', the time consuming operations
            are printed toward the top of the file.
        strip_dirs: bool
            Whether to remove the leading path info from file names.
            This is also useful in reducing the size of the printout
    Returns:
        Profile of the decorated function
    """

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _output_file = output_file or func.__name__ + ".prof"
            pr = cProfile.Profile()
            pr.enable()
            retval = func(*args, **kwargs)
            pr.disable()
            # pr.dump_stats(_output_dir_file)

            with open(_output_file, "w") as f:
                ps = pstats.Stats(pr, stream=f)
                if strip_dirs:
                    ps.strip_dirs()
                if isinstance(sort_by, (tuple, list)):
                    ps.sort_stats(*sort_by)
                else:
                    ps.sort_stats(sort_by)
                ps.print_stats(lines_to_print)
            return retval

        return wrapper

    return inner


def ts(style="datetime", template=None):
    """
    Get current timestamp as string.

    Parameters
    ----------
    style : string {"datetime", "date", "time"}
        format the timestamp with this built-in template
    template : string
        if not None, format the timestamp with this template instead of one of
        the built-in styles

    Returns
    -------
    ts : string
        the string timestamp
    """
    if template is None:
        if style == "datetime":
            template = "{:%Y-%m-%d %H:%M:%S}"
        elif style == "date":
            template = "{:%Y-%m-%d}"
        elif style == "time":
            template = "{:%H:%M:%S}"
        else:  # pragma: no cover
            raise ValueError(f'unrecognized timestamp style "{style}"')

    ts = template.format(dt.datetime.now())
    return ts


def _add_handler(logger, dir, filename=log_filename, log_level=log_level):
    """
    Create a logger or return the current one if already instantiated.

    Parameters
    ----------
    log_level : int
        one of Python's logger.level constants
    logger : lg.Logger
        The logger
    filename : string
        name of the log file, without file extension

    Returns
    -------
    logger : logging.logger
    """
    # if a logger with this name is not already set up
    if not getattr(logger, "handler_set", None):
        # get today's date and construct a log filename
        log_filename = Path(dir) / f'{filename}_{ts(style="date")}.log'
        # if the logs folder does not already exist, create it
        log_filename.parent.mkdir(parents=True, exist_ok=True)
        # create file handler and log formatter and set them up
        handler = lg.FileHandler(log_filename, encoding="utf-8")
        formatter = lg.Formatter(
            "%(asctime)s %(levelname)s %(module)s %(funcName)s %(lineno)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(log_level)
        logger.handler_set = True
    return logger


def get_labels(
    database_nm=mvs4defaultdb,
    user="moves",
    password="moves",
    host="127.0.0.1",
    port=3306,
):
    db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_nm}"
    engine = create_engine(db_url)
    conn = engine.connect()
    county = pd.read_sql(
        "SELECT countyID FIPS, countyName county FROM county WHERE stateID = 48", conn
    ).assign(
        FIPS=lambda df: df.FIPS.astype(int),
        county=lambda df: df.county.str.split(" County", expand=True)[0].str.strip(),
    )
    # Emission process labels
    emisprc = pd.read_sql(
        "SELECT processID, processName, shortName processABB  FROM emissionprocess WHERE "
        "isAffectedByOnroad = 1",
        conn,
    )
    # Pollutant labels
    pollutants = pd.read_sql(
        "SELECT pollutantID, shortName pollutant FROM pollutant WHERE isAffectedByOnroad = 1",
        conn,
    )
    # Road type labels
    rdlab = pd.DataFrame(
        {
            "mvsRoadTypeID": [1, 2, 3, 4, 5],
            "mvsRoadLab": ["offNet", "rurRes", "rurUnRes", "urbRes", "urbUnRes"],
        }
    )
    moves_roadtypes = pd.read_sql(
        "SELECT roadTypeID mvsRoadTypeID, roadDesc mvsRoadType "
        "FROM roadtype WHERE isAffectedByOnroad = 1",
        conn,
    ).merge(rdlab, on="mvsRoadTypeID")
    # Source use type and fuel type labels
    sut_lab = pd.read_csv(
        StringIO(
            """sourceUseTypeID,sutLab
        11,MC
        21,PC
        31,PT
        32,LCT
        41,OBus
        42,TBus
        43,SBus
        51,RT
        52,SuShT
        53,SuLhT
        54,MH
        61,CShT
        62,CLhT
    """
        )
    )
    ft_lab = pd.read_csv(
        StringIO(
            """fuelTypeID,ftLab
        1,G
        2,D
        3,CNG
        4,LPG
        5,E85
        9,ELEC
    """
        )
    )
    moves_sut = pd.read_sql(
        "SELECT sourceTypeID sourceUseTypeID , sourceTypeName sourceUseType FROM sourceusetype",
        conn,
    ).merge(sut_lab, on="sourceUseTypeID")
    moves_ft = pd.read_sql(
        "SELECT fuelTypeID, fuelTypeDesc fuelType FROM fueltype",
        conn,
    ).merge(ft_lab, on="fuelTypeID")
    conn.close()
    # Activity labels
    act_lab = pd.DataFrame(
        {
            "actTypeABB": [
                "VMT",
                "VHT",
                "Speed",
                "AdjSHP",
                "ONI",
                "Starts",
                "SHEI",
                "APU",
            ],
            "actType": [
                "VMT",
                "VHT",
                "Speed",
                "Adjusted SHP",
                "ONI",
                "Starts",
                "Extended Idle Hours",
                "APU Hours",
            ],
        }
    )
    return {
        "county": county,
        "emisprc": emisprc,
        "pollutants": pollutants,
        "moves_roadtypes": moves_roadtypes,
        "moves_sut": moves_sut,
        "moves_ft": moves_ft,
        "act_lab": act_lab,
    }


def unit_converter(in_unit, out_unit):
    ureg = UnitRegistry()
    ureg.define("MBTU = 1e6 BTU")
    ureg.define("Kilojoules = kilojoule")
    # Define the source and target units
    source_unit = ureg(in_unit)
    target_unit = ureg(out_unit)
    # Convert to base units and calculate the conversion factor
    conversion_factor = (
        source_unit.to_base_units() / target_unit.to_base_units()
    ).magnitude
    return conversion_factor


@profile()
def profile_test():
    a, b = 1, 2
    a + b


if __name__ == "__main__":
    ts("date")
    get_labels(port=3308)
    get_labels()
    profile_test()
    z = 1
