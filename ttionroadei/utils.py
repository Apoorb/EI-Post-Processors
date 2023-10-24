"""General utility functions."""
from io import StringIO
import cProfile
import pstats
from functools import wraps
import pandas as pd
import datetime
import datetime as dt
import logging as lg
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
            "%(asctime)s | %(levelname)s | %(module)s | %(funcName)s | %(lineno)s | %(message)s",
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
    """
    Retrieve labels and information from a MOVES default database.

    Parameters
    ----------
    database_nm : str, optional
        The name of the MOVES default database to connect to. Default is 'mvs4defaultdb' from settings.YAML.
    user : str, optional
        The username for the database connection. Default is 'moves'.
    password : str, optional
        The password for the database connection. Default is 'moves'.
    host : str, optional
        The host or IP address of the database server. Default is '127.0.0.1'.
    port : int, optional
        The port number to use for the database connection. Default is 3306.

    Returns
    -------
    dict
        A dictionary containing various dataframes with labels and information for use in MOVES:
        - 'county': Dataframe with county labels.
        - 'emisprc': Dataframe with emission process labels.
        - 'pollutants': Dataframe with pollutant labels.
        - 'moves_roadtypes': Dataframe with MOVES road type labels.
        - 'moves_sut': Dataframe with source use type labels.
        - 'moves_ft': Dataframe with fuel type labels.
        - 'act_lab': Dataframe with activity labels.
    """
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
    """
    Convert a quantity from one unit to another using the Pint library. This function
    uses the Pint library to perform unit conversions. It defines unit relationships and
    then calculates the conversion factor to transform a quantity from the input unit to
    the target unit.

    Parameters
    ----------
    in_unit : str
        The input unit for conversion, e.g., 'MBTU' (million British Thermal Units).
    out_unit : str
        The target unit for conversion, e.g., 'Kilojoules' (kilojoules).

    Returns
    -------
    float
        The conversion factor to convert from the input unit to the target unit.


    Example usage:
    ```
    conversion_factor = unit_converter('MBTU', 'Kilojoules')
    value_in_mbtu = 10  # Example value in MBTU
    value_in_kilojoules = value_in_mbtu * conversion_factor
    ```
    """
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


def delete_old_log_files(log_directory, max_age_in_days):
    """
    This function iterates through the log files in the specified directory, checks their
    timestamps, and deletes log files that are older than the specified maximum age. It is
     useful for managing log files and ensuring that old log data is regularly cleaned up.

    Parameters
    ----------
    log_directory : str
        The directory where log files are located.
    max_age_in_days : int
        The maximum age, in days, for log files to be retained. Log files older than this threshold will be deleted.

    Returns
    -------
    None

    Example usage:
    ```python
    log_directory = '/path/to/log/files'
    max_age_in_days = 30  # Delete log files older than 30 days.
    delete_old_log_files(log_directory, max_age_in_days)
    ```
    """
    logger = lg.getLogger(name=__file__)
    logger = _add_handler(dir=log_directory, logger=logger)
    # Get the current date and time.
    current_datetime = datetime.datetime.now()

    # Calculate the threshold date based on the maximum age.
    threshold_date = current_datetime - datetime.timedelta(days=max_age_in_days)

    # Iterate through the log files and delete files with old timestamps.
    for log_file in Path(log_directory).iterdir():
        # Check if the file is a regular file (not a directory).
        if log_file.is_file():
            file_timestamp = datetime.datetime.fromtimestamp(log_file.stat().st_mtime)

            # Compare the file's timestamp with the threshold date.
            if file_timestamp < threshold_date:
                # If the file is older than the threshold date, delete it.
                log_file.unlink()
                print(f"Deleted old log file: {log_file.name}")

    logger.info(msg="Log files with old timestamps have been deleted.")


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
