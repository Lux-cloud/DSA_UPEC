#
# This file is part of dsa_tdb (see https://code.europa.eu/dsa/transparency-database/dsa-tdb).
#
# SPDX-License-Identifier: EUPLv1.2
# Copyright (C) 2024 European Union
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the EUROPEAN UNION PUBLIC LICENCE v. 1.2 as
# published by the European Union.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# EUROPEAN UNION PUBLIC LICENCE v. 1.2 for further details.
#
# You should have received a copy of the EUROPEAN UNION PUBLIC LICENCE v. 1.2.
# along with this program.
#
# If not, see < https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12 >.#
import email.utils
import hashlib
import logging
import logging.handlers
import os
from datetime import date, datetime, timezone
from glob import glob
from multiprocessing import BoundedSemaphore, Manager, Process
from pathlib import Path
from typing import List, Tuple, Union

import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from slugify import slugify

import dsa_tdb
from dsa_tdb.types import (
    CHUNKED_FILE_SUCCESS_NAME,
    CHUNKED_FILES_SUBFOLDER_NAME,
    DAILY_FILE_CHECKSUM_EXTENSION,
    DAILY_FILE_DATE_FORMAT,
    DAILY_FILE_NAME_TEMPLATE,
    DAILY_FILES_SUBFOLDER_TEMPLATE,
    TDB_START_DATE,
    AggregateFileFormat,
    AggregateWriteMode,
    TDB_chunkFormat,
    TDB_dailyDumpsVersion,
    coalesce_platforms_names,
    defaultSparkConf,
    territorial_scopes,
)

logger = logging.getLogger(dsa_tdb.__name__)


def vals2arra(s: Union[str, None], as_set: bool = False) -> Union[set, list]:
    """Function to port the array string encoding into a set of values.
    Assumes the form "[AAA,BBB]" or "AAA".

    Parameters
    ----------
    s : str
        The string to parse.
        The format is "[AAA,BBB]" or "["AAA","BBB"]" or "AAA",
        spaces and double quotes will be removed.
    as_set : bool, optional
        Whether to return a set or a list, by default False

    Returns
    -------
    [set,list]
        The set of values. If the object is not a string, it is a string ==`<NA>`
        or `None` is found then an empty list (set) is returned.
    """
    if isinstance(s, str):
        if s in ("<NA>", "", "[]") or s is None:
            vals = []
        else:
            s = s.strip().replace(" ", "").replace('"', "")
            if "[" in s:
                if "]" not in s:
                    raise ValueError(f'Missing closing bracket "]" in string {s}')
                s = s.replace("[", "").replace("]", "")
                if s == "":
                    vals = []

            vals = s.split(",")
    else:
        vals = []

    if as_set:
        vals = set(vals)
    return vals


def arra2vals(arr: Union[set, list]) -> Union[str, None]:
    """Function to port the array into a string.

    Parameters
    ----------
    arr : [set,list]
        The set of values.

    Returns
    -------
    str, None
        The string representation of the set.
        The format is '["AAA","BBB"]'.
        If the input is None or an empty list, it returns None.
    """
    if arr is None or len(arr) == 0:
        return None
    if isinstance(arr, set):
        arr = list(arr)
    arr = sorted(arr)
    arr_str = "[" + ",".join([f'"{e}"' for e in arr]) + "]"
    return arr_str


def sanitize_platform_name(platform_name: str, warn_on_change: bool = False) -> str:
    """Function to sanitize the platform name.

    Parameters
    ----------
    platform_name : str
        The platform name.

    Returns
    -------
    str
        The sanitized platform name.
    """
    platform_sanitized = slugify(platform_name, replacements=[(".", ""), ("@", "at")], separator="-")
    if warn_on_change and platform_sanitized != platform_name:
        msg = "Sanitized (slugified) platform name from %s to %s" % (platform_name, platform_sanitized)
        logger.info(msg)
        RuntimeWarning(msg)
    return platform_sanitized


@F.udf(returnType=T.ArrayType(T.StringType()))
def _vals2arra_udf(col: str):
    return vals2arra(col)


@F.udf(returnType=T.StringType())
def _coalesce_platforms_names_udf(platform_name: str):
    return coalesce_platforms_names.get(platform_name, platform_name)


def spark_session_factory(
    app_name: str = "dsa_tdb",
    memory_limit: Union[str, None] = None,
    n_workers: Union[int, None] = None,
    spark_local_dir: Union[str, None] = None,
    options: Union[dict, None] = None,
) -> SparkSession:
    """Function to create a Spark session.
    Note that the Spark master URL can be set using the environment variable `SPARK_MASTER_URL`.
    For instance, to use the local mode, you can set `SPARK_MASTER_URL=local[*]` before calling this function.
    ```python
    import os
    os.environ['SPARK_MASTER_URL'] = 'local[*]'
    ```

    Parameters
    ----------
    app_name : str, optional
        The name of the application, by default 'dsa_tdb'
    memory_limit : str, optional
        The memory limit to use, by default None
    n_workers : int, optional
        The number of workers to use, by default None
        If None or <= 0, it will use all the available cores.
    spark_local_dir : str, optional
        The local directory to use, by default None
    options : dict, optional
        Additional options to pass to the Spark session, by default None.
        Must be a dictionary with the key-value pairs.

    Returns
    -------
    SparkSession
        The Spark session.
    """
    spark_conf = defaultSparkConf
    if memory_limit is not None and memory_limit != "":
        spark_conf.set("spark.driver.memory", memory_limit)
    if n_workers and n_workers > 0:
        spark_conf.set("spark.cores.max", str(int(n_workers)))
    if spark_local_dir and spark_local_dir != "":
        logger.debug(f"Setting the spark local directory to {spark_local_dir}.")
        spark_conf.set("spark.local.dir", spark_local_dir)
        os.environ["SPARK_LOCAL_DIRS"] = spark_local_dir

    if options:
        for k, v in options.items():
            spark_conf.set(k, v)

    # Create the builder: I check for the `SPARK_MASTER_URL` in the environment
    # TODO: we should move all the spark-configuration to the environment variables,
    # possibly using a .env file
    spark_master = os.environ.get("SPARK_MASTER_URL")
    if spark_master is not None:
        logging.info("Using Spark master %s", spark_master)
        builder = SparkSession.builder.master(spark_master)
        spark_conf.setMaster(spark_master)
    else:
        logging.info("No Spark master found. Will use the default configuration.")
        builder = SparkSession.builder
    spark = (
        builder.config(conf=spark_conf)  # type: ignore
        .appName(app_name)
        .getOrCreate()
    )
    logger.info("Spark instance details:\n%s", spark)
    return spark


def read_sha1_file(file_path: Union[str, Path]) -> str:
    """Function to get the sha1 signature from a .sha1 file.

    Parameters
    ----------
    file_path : str
        The path of the file.

    Returns
    -------
    str
        The sha1 of the file.
    """
    with open(file_path) as f:
        sha1 = _get_sha1_from_str(f.read())
    return sha1


def _get_sha1_from_str(string: str) -> str:
    """Function to get the sha1 signature from a string.

    Parameters
    ----------
    string : str
        The string to parse.

    Returns
    -------
    str
        The sha1 of the string.
    """
    return string.strip().split(" ")[0]


def territories2label(ts: str) -> str:
    """Function to transform the countries to a single string."""
    extracted_ts = vals2arra(ts, as_set=True)
    for k, v in territorial_scopes.items():
        if extracted_ts == v:
            return k
    extracted_ts = "_".join(sorted(extracted_ts))
    return extracted_ts


@F.udf(returnType=T.StringType())
def _territories2label_udf(ts: str):
    """Function to transform the countries to a single string."""
    return territories2label(ts)


def countWithNans(g):
    return pd.DataFrame((~g.isna()).sum(axis=0)).T


def _delete_daily_zipfile(file_path: Union[str, Path], delete_sha1: bool = False):
    """Function to delete the daily zip file and its sha1.
    If found, it will delete the sha1 file as well (the
    file with additional `.sha1` extension).

    Parameters
    ----------
    file_path : str
        The path of the file to delete.
    delete_sha1 : bool
        Whether to also delete the sha1 file or not (default to false).
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    logger.info("Deleting original file %s", file_path)
    os.remove(file_path)
    if delete_sha1:
        sha1_csv_zip_in = file_path.with_suffix(DAILY_FILE_CHECKSUM_EXTENSION)
        if os.path.exists(sha1_csv_zip_in):
            logger.info("Deleting original file %s", sha1_csv_zip_in)
            os.remove(sha1_csv_zip_in)


def _compare_last_modified_file_url(file_path: str, url: str):
    """
    Function to compare last modification time of a file (locally) with Last-Modified
    HTTP header of the URL used to download the file.

    Parameters
    ----------
    file_path : str
        The path of the downloaded file.
    url : str
        The URL used to download the file.

    Returns
    -------
    bool
        True if the URL is newer than the file, False otherwise.
    """
    r = requests.head(url, timeout=10)
    last_modified_url = email.utils.parsedate_to_datetime(r.headers.get("Last-Modified"))
    last_modified_file = datetime.fromtimestamp(os.stat(file_path).st_mtime, tz=timezone.utc)
    return last_modified_url > last_modified_file  # type: ignore


def check_sha1_sum(
    local_filename: Union[str, Path], local_sha1_filename: Union[str, Path, None] = None, BUF_SIZE: int = 65536
):
    """Function to check the sha1 of a file.

    Parameters
    ----------
    local_filename : str
        The path of the file to check.
    local_sha1_filename : str, optional
        The path of the sha1 file.
        If None, it will be the `local_filename` + '.sha1', by default None
    BUF_SIZE : int, optional
        The buffer size in bits to use when computing sha1, by default 65536

    Raises
    ------
    AssertionError
        If the sha1 is not correct.
    ValueError
        If the sha1 file does not exist.
    """
    if isinstance(local_filename, str):
        local_filename = Path(local_filename)

    if isinstance(local_sha1_filename, str):
        local_sha1_filename = Path(local_sha1_filename)
    elif local_sha1_filename is None:
        local_sha1_filename = local_filename.with_suffix(DAILY_FILE_CHECKSUM_EXTENSION)

    if local_sha1_filename.exists():
        logger.info("Found sha1 file %s", local_sha1_filename)
        tmp_hash = read_sha1_file(local_sha1_filename)
        logger.info("Found sha1 %s in %s", tmp_hash, os.path.basename(local_sha1_filename))
        logger.info("Checking sha1 for %s", os.path.basename(local_filename))
        sha1 = hashlib.sha1(usedforsecurity=False)
        with open(local_filename, "rb") as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                sha1.update(data)
        tmp_digest = sha1.hexdigest()
        if tmp_digest != tmp_hash:
            logger.error("sha1 %s does not match %s", tmp_digest, tmp_hash)
            raise AssertionError(f"sha1 {tmp_digest} does not match {tmp_hash}")

        logger.info(f"sha1 {os.path.basename(local_filename)} OK")
        return True
    else:
        logger.info("Could not find sha1 file %s", local_sha1_filename)
        raise ValueError(f"Could not find sha1 file {local_sha1_filename}")


def compute_start_date(start_date: Union[str, None, datetime, date]) -> date:
    if start_date is None:
        logger.info("Empty start date. Will use the default start date.")
        start_date_fix = TDB_START_DATE.date()
    elif isinstance(start_date, str):
        if len(start_date) == 0:
            logger.info("Empty start date. Will use the default start date.")
            start_date_fix = TDB_START_DATE.date()
        else:
            start_date_fix = datetime.strptime(start_date, DAILY_FILE_DATE_FORMAT).date()
    elif isinstance(start_date, datetime):
        start_date_fix = start_date.date()
    elif isinstance(start_date, date):
        start_date_fix = start_date
    else:
        # Should not happen
        raise ValueError("Invalid start date format. Must be a string in the format YYYY-MM-DD or a date/datetime.")
    return start_date_fix


def compute_end_date(end_date: Union[str, None, datetime, date]) -> date:
    if end_date is None:
        logger.info("Empty end date. Will use the current date.")
        end_date_fix = datetime.now().date()
    elif isinstance(end_date, str):
        if len(end_date) == 0:
            logger.info("Empty end date. Will use the current date.")
            end_date_fix = datetime.now().date()
        else:
            end_date_fix = datetime.strptime(end_date, DAILY_FILE_DATE_FORMAT).date()
    elif isinstance(end_date, datetime):
        end_date_fix = end_date.date()
    elif isinstance(end_date, date):
        end_date_fix = end_date
    else:
        # Should not happen
        raise ValueError("Invalid end date format. Must be a string in the format YYYY-MM-DD or a date/datetime.")
    return end_date_fix


def get_files_to_load(
    root_folder: str,
    platform: str,
    version: TDB_dailyDumpsVersion = TDB_dailyDumpsVersion.full,
    local_chunked_subfolder: str = CHUNKED_FILES_SUBFOLDER_NAME,
    input_format: TDB_chunkFormat = TDB_chunkFormat.parquet,
    start_date: Union[str, date, datetime, None] = None,
    end_date: Union[str, datetime, None] = None,
) -> List[str]:
    start_date_fix = compute_start_date(start_date)
    end_date_fix = compute_end_date(end_date)
    platform = sanitize_platform_name(platform, warn_on_change=True)

    # Compose the folder containing chunked files' folders:
    directory_daily_chunked_folders = os.path.join(
        root_folder, DAILY_FILES_SUBFOLDER_TEMPLATE.format(platform=platform, version=version), local_chunked_subfolder
    )

    if not os.path.exists(directory_daily_chunked_folders) or not os.path.isdir(directory_daily_chunked_folders):
        raise FileNotFoundError(f"Folder {directory_daily_chunked_folders} not found.")

    input_files_pattern = os.path.join(
        directory_daily_chunked_folders,
        DAILY_FILE_NAME_TEMPLATE.format(date="*", platform=platform, version=version, extension=""),
        "part-*." + input_format,
    )
    logger.debug(f"Input files pattern for: {input_files_pattern}")

    files_to_process = sorted(glob(input_files_pattern))

    # Filter the files checking that their parent folder contains a SUCCESS file
    files_to_process = [
        f for f in files_to_process if os.path.exists(os.path.join(os.path.dirname(f), CHUNKED_FILE_SUCCESS_NAME))
    ]

    # Filter the files by the initial and final date:
    if start_date is not None:
        logger.info(f"Filtering files by the start date: {start_date}")
        files_to_process = [
            f
            for f in files_to_process
            if datetime.strptime(
                os.path.split(os.path.dirname(f))[-1],
                DAILY_FILE_NAME_TEMPLATE.format(
                    platform=platform, version=version, date=DAILY_FILE_DATE_FORMAT, extension=""
                ),
            ).date()
            >= start_date_fix
        ]
    if end_date is not None:
        logger.info(f"Filtering files by the end date: {end_date}")
        files_to_process = [
            f
            for f in files_to_process
            if datetime.strptime(
                os.path.split(os.path.dirname(f))[-1],
                DAILY_FILE_NAME_TEMPLATE.format(
                    platform=platform, version=version, date=DAILY_FILE_DATE_FORMAT, extension=""
                ),
            ).date()
            <= end_date_fix
        ]
    return files_to_process


def compute_files_to_process(
    root_folder: str,
    out_file_name: str,
    platform: str,
    version: TDB_dailyDumpsVersion = TDB_dailyDumpsVersion.full,
    input_format: TDB_chunkFormat = TDB_chunkFormat.parquet,
    output_format: AggregateFileFormat = AggregateFileFormat.parquet,
    local_chunked_subfolder: str = CHUNKED_FILES_SUBFOLDER_NAME,
    write_mode: AggregateWriteMode = AggregateWriteMode.overwrite,
    start_date: Union[str, None] = None,
    end_date: Union[str, None] = None,
    step_name: str = "analysis",
) -> Tuple[list, AggregateWriteMode, str, str, str, pd.DataFrame]:
    """Function to compute the files to process.

    Parameters
    ----------
    root_folder : str
        The root folder where the `platform___version` folders are stored.
    local_chunked_subfolder : str
        The subfolder where the chunked files are stored.
    out_file_name : str
        The name of the file to write, in the format 'nameof_file.{}'.
    platform : str
        The platform name.
    version : str
        The version of the files.
    input_format : str
        The input format.
    output_format : str
        The output format.
    write_mode : AggregateWriteMode, optional
        The write mode, by default AggregateWriteMode.overwrite
    start_date : str, optional
        The start date. The format is 'YYYY-MM-DD'.
        Default to None, which means no filtering.
    end_date : str, optional
        The end date. The format is 'YYYY-MM-DD'.
        Default to None, which means no filtering.
    step_name : str
        The name of the step, usually 'aggregate' or 'filter'.

    Returns
    -------
    List[str], AggregateWriteMode, str, str, str, pd.DataFrame
        The list of files to process, the write mode, the output file name, the dates files filename,
        and the output configuration filename and the df of the dates files to remove.
    """

    dates_files_filename = out_file_name.format("dates_files.csv")
    out_configuration_filename = out_file_name.format("configuration.yaml")
    out_file_dump = out_file_name.format(output_format)

    original_files = get_files_to_load(
        root_folder=root_folder,
        platform=platform,
        version=version,
        local_chunked_subfolder=local_chunked_subfolder,
        input_format=input_format,
        start_date=start_date,
        end_date=end_date,
    )

    dates_to_remove = []
    dates_files_to_remove = pd.DataFrame()
    if write_mode == AggregateWriteMode.append:
        try:
            dates_files_to_remove = pd.read_csv(dates_files_filename, parse_dates=["date"])
            dates_to_remove = sorted(dates_files_to_remove["date"].dt.date.unique())
        except FileNotFoundError:
            logger.info(f"No dates file found. Will {step_name} all the files.")
            if os.path.exists(out_file_dump):
                logger.warning(f"No dates found but {out_file_dump} present. Will overwrite it.")
                out_file_mode = AggregateWriteMode.overwrite
            else:
                out_file_mode = AggregateWriteMode.overwrite
        else:
            # New in 0.6.5: we use only relative paths to make the deployment portable
            # We check for any abs path and cast a warning.
            if dates_files_to_remove["file"].apply(lambda p: os.path.isabs(p)).any():
                logger.warning(
                    f"Legacy version of absolute path files found in {dates_files_filename},"
                    f" will cast to relative path w.r.t. {root_folder}"
                )
                dates_files_to_remove["file"] = dates_files_to_remove["file"].apply(
                    lambda p: os.path.relpath(p, start=root_folder)
                )

            logger.info(f"Found {len(dates_to_remove)} dates already {step_name}ed.")
            if os.path.exists(out_file_dump):
                logger.info(f"Found {out_file_dump}. Will append to it.")
                out_file_mode = AggregateWriteMode.append
            else:
                logger.info(f"WARNING: No {out_file_dump} found. Will create a new file.")
                out_file_mode = AggregateWriteMode.overwrite
    else:
        out_file_mode = write_mode

    if len(dates_to_remove) > 0:
        logger.info(f"Will skip {len(dates_to_remove)} days from the {step_name} step.")
        logger.info(f"From the date range: {dates_to_remove[0]} - {dates_to_remove[-1]}")

        # Now, we remove the files already processed:
        files_to_process = [
            f
            for f in original_files
            if os.path.relpath(f, start=root_folder) not in dates_files_to_remove["file"].values
        ]

        if len(files_to_process):
            logger.info(f"Will process {len(files_to_process)} files out of {len(original_files)} original files.")
    else:
        files_to_process = original_files

    return (
        files_to_process,
        out_file_mode,
        out_file_dump,
        dates_files_filename,
        out_configuration_filename,
        dates_files_to_remove,
    )


def compare_sha1_file_content(filename_a: Union[str, Path], filename_b: Union[str, Path]) -> bool:
    """Function to compare the sha1 of two files.

    Parameters
    ----------
    filename_a : str
        The path of the first file.
    filename_b : str
        The path of the second file.

    Returns
    -------
    bool
        True if the sha1 of the two files is the same, False otherwise.
    """
    sha1_a = read_sha1_file(filename_a)
    sha1_b = read_sha1_file(filename_b)
    return sha1_a == sha1_b


def _logger_thread(queue):
    """This is the thread managing the logger from multiple workers.
    Using the logging queue handler as in
    https://docs.python.org/3/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes

    For every processes needing access to the log, pass the common queue `q` and
    put this initial configuration:

    ```
    if q is not None:
        qh = logging.handlers.QueueHandler(q)
        root = logging.getLogger() # <- or existing module logger
        root.addHandler(qh)
        root.setLevel(logging.DEBUG) # or any desired level
    ```
    """
    # Setup logger
    while True:
        record = queue.get()
        if record is None:
            break
        logger = logging.getLogger(record.name)
        logger.handle(record)


# Implement the worker and fifo-like pool as in https://stackoverflow.com/questions/74448892
def _template_worker(semaphore, comm_queue, except_queue, logging_level, foo, *args):
    """
    Function to be executed in a process.

    Parameters
    ----------
    semaphore : BoundedSemaphore
        The semaphore to use to acquire the semaphore
    comm_queue : Queue
        The queue to use for logging
    exception_queue : Queue
        The queue to use for exception propagation
    foo : function
        The function to execute
    args : tuple
        The arguments to pass to the function

    Returns
    -------
    Any
        The return value of the function, None if an exception is raised


    Example
    -------
    semaphore = BoundedSemaphore(num_processes)
    semaphore.acquire()
    _template_worker(*args)

    """
    if comm_queue:
        qh = logging.handlers.QueueHandler(comm_queue)
        logger.addHandler(qh)
        logger.propagate = False  # Don't propagate to the root logger and
        # avoid duplicated logs
        logger.setLevel(logging_level)
    try:
        r = foo(*args)
    except Exception as e:
        # Log the exception and re-raise it to propagate it to the main process
        msg_err = str(e)
        logger.exception(f"An error occured in the worker: \n{msg_err}")
        except_queue.put(e)
        r = None
    finally:
        semaphore.release()
    return r


def fifo_pool(num_processes: int, foo, args_list, logging_level: int = logging.INFO):
    semaphore = BoundedSemaphore(num_processes)
    manager = Manager()
    comm_queue = manager.Queue(-1)
    except_queue = manager.Queue()
    logger_process = Process(target=_logger_thread, args=(comm_queue,))
    logger_process.start()
    processes = []
    logger.info("Starting all processes")
    for tmp_args in args_list:
        semaphore.acquire()
        submit_args = [semaphore, comm_queue, except_queue, logging_level, foo] + list(tmp_args)
        processes.append(Process(target=_template_worker, args=submit_args))
        processes[-1].start()

        # Clean possibly finished processes
        while processes and not processes[0].is_alive():
            processes[0].join()
            # Check if any exception occurred
            terminate_if_failure(processes, except_queue, logger, logger_process, comm_queue)
            processes.pop(0)

    # Join all the remaining processes
    for p in processes:
        p.join()
        # Check if any exception occurred
        terminate_if_failure(processes, except_queue, logger, logger_process, comm_queue)

    # If I survived until the end, close the logger
    close_logger(logger, logger_process, comm_queue)


def terminate_if_failure(processes, except_queue, logger, logger_process, comm_queue):
    """
    Terminates all remaining processes and closes the logger if an exception occurred in a worker process.

    Parameters
    ----------
    processes : list
        The list of running processes.
    except_queue : Queue
        The queue where exceptions are stored.
    logger : Logger
        The logger instance.
    logger_process : Process
        The process managing the logger.
    comm_queue : Queue
        The queue used for logging.

    Raises
    ------
    Exception
        The exception that occurred in a worker process.
    """
    if not except_queue.empty():
        e = except_queue.get()
        logger.error(f"Exception occurred in a worker process: {e}")
        # Terminate all remaining processes
        for p in processes:
            p.terminate()
        # Close the logger
        close_logger(logger, logger_process, comm_queue)
        # Raise the exception
        raise e


def close_logger(logger, logger_process, comm_queue):
    """
    Closes the logger process and waits for it to finish.

    Parameters
    ----------
    logger : Logger
        The logger instance.
    logger_process : Process
        The process managing the logger.
    comm_queue : Queue
        The queue used for logging.
    """
    logger.info("Sending close signal to logger process")
    comm_queue.put(None)
    logger.info("Waiting for logger process to finish")
    logger_process.join()
    logger.info("All processes finished")
