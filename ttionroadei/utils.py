"""


"""

# 1. Logging function

# 2. Timing function

# 3. Get MOVES table

# 4. QA Log


"""General utility functions."""
import cProfile
import pstats
from functools import wraps
import datetime as dt
import logging as lg
import os
import sys
import unicodedata
import warnings
from contextlib import redirect_stdout
from pathlib import Path
import mysql.connector as mariadb
import pkg_resources
import yaml

package_name = "ttionroadei"
try:
    # Get the path to the settings.yaml file within the package
    settings_yaml_path = pkg_resources.resource_filename(package_name, "settings.yaml")

    # Read and parse the YAML file
    with open(settings_yaml_path, "r") as yaml_file:
        data = yaml.safe_load(yaml_file)

    # Access and use the data from settings.yaml as needed
    print(data)

except Exception as e:
    print(f"Error: {e}")

log_console = data.get("log_console")
log_file = data.get("log_file")
log_filename = data.get("log_filename")
log_level = data.get("log_level")


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


def connect_to_server_db(
    database_nm=None, user="moves", password="moves", host="127.0.0.1", port=3306
):
    """
    Function to connect to a particular database on the server.
    Returns
    -------
    conn_: mariadb.connection
        Connection object to access the data in MariaDB Server.
    """
    try:
        conn_ = mariadb.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database_nm,
        )
    except mariadb.Error as e:
        sys.exit(1)
    return conn_


@profile()
def profile_test():
    a, b = 1, 2
    a + b


if __name__ == "__main__":
    ts("date")
    conn = connect_to_server_db()
    cur = conn.cursor()
    cur.execute("SHOW DATABASES")
    dbs = cur.fetchall()
    conn.close()
    profile_test()
    z = 1
