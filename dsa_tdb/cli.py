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
import argparse
import logging
import os
import re
import shutil
from datetime import datetime
from typing import Union

import pandas as pd
import yaml

import dsa_tdb
from dsa_tdb.etl import loadFile
from dsa_tdb.fetch import prepare_daily_dumps
from dsa_tdb.types import (
    CHUNKED_FILES_SUBFOLDER_NAME,
    DAILY_CHUNKED_FOLDER_NAME_REGEX,
    AggregationConfig,
    FilteringConfig,
    TDB_chunkFormat,
    TDB_dailyDumpsVersion,
)
from dsa_tdb.utils import compute_files_to_process, sanitize_platform_name

logger = logging.getLogger(dsa_tdb.__name__)
loggerDask = logging.getLogger("distributed")
loggerDask.setLevel(logging.ERROR)

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="command", help="sub-command help")

# Preprocessing command:
parserPreprocess = subparsers.add_parser(
    "preprocess",
    description="Download and chunk files from the webpage. Useful for the first download to save some disk space.",
)
parserPreprocess.add_argument(
    "-o",
    "--output",
    type=str,
    help="Output folder. This is the root folder where the files will be downloaded in the `platform___version` subfolder.",
    required=True,
)
parserPreprocess.add_argument(
    "-v", "--version", type=str, help="Version of the files to download. [full|light]", default="full"
)
parserPreprocess.add_argument(
    "-p",
    "--platform",
    type=str,
    help="Platform to download the files from. [global|facebook|...|tiktok|X]",
    default="global",
)
parserPreprocess.add_argument(
    "-x",
    "--exclude",
    type=str,
    help="The comma separated list of the platforms to exclude, if `global` platform is specified. Use the original platform names withouth sanitization.",
    default="",
)
parserPreprocess.add_argument(
    "--skip_sha1",
    action="store_true",
    help="Whether to skip the check of the sha1 of the newly downloaded file.",
    default=False,
)
parserPreprocess.add_argument(
    "-r",
    "--force_sha1",
    action="store_true",
    help="Whether to force the sha1 of the already downloaded files. If the check fails, it will download the file again.",
    default=False,
)
parserPreprocess.add_argument(
    "-i",
    "--from_date",
    type=str,
    help="Date from which to start downloading the files. Format YYYY-MM-DD.",
    default=None,
)
parserPreprocess.add_argument(
    "-f", "--to_date", type=str, help="Date until which to download the files. Format YYYY-MM-DD.", default=None
)
parserPreprocess.add_argument("-n", "--nprocs", type=int, help="Number of processes.", default=1)
parserPreprocess.add_argument(
    "--skip_chunking",
    action="store_true",
    help="Whether to skip chunking the files after downloading them.",
    default=False,
)
parserPreprocess.add_argument(
    "--chunk_size",
    type=int,
    help="The number of SoRs to put in each chunk. Do not change it unless you know what you are doing.",
    default=1000000,
)
parserPreprocess.add_argument(
    "--format",
    type=str,
    help='The format of the output files ("csv" or "parquet"), default "parquet".',
    default="parquet",
)
parserPreprocess.add_argument(
    "-d",
    "--delete_original",
    action="store_true",
    help="Whether to delete the original files, default False.",
    default=False,
)
parserPreprocess.add_argument(
    "--override_chunked_subfolder",
    type=str,
    help="Override the default chunked subfolder name. Do not change/use it unless you know what you are doing.",
    default=CHUNKED_FILES_SUBFOLDER_NAME,
)
parserPreprocess.add_argument(
    "--loglevel", type=str, help="The logging level. [DEBUG|INFO|WARNING|ERROR|CRITICAL]", default="INFO"
)
parserPreprocess.add_argument(
    "--no_raise_on_sha1_check",
    dest="raise_on_error",
    action="store_false",
    help="Whether to raise on sha1 check failures (if not provided, default). If provided, it will try to re-download the dump file again / chunk it, depending on the failure.",
    default=True,
)

# Aggregate command:
parserAggregate = subparsers.add_parser(
    "aggregate",
    description="""Aggregates files into a single file containing the counts for each combination of the attributes.
It will also make a copy of the configuration file used in the same folder of the output file
with the same name and the `configuration.yaml` file for later reference.
If the aggregation mode is set to `append` in the configuration, it will load only the dates that
are not already in the (possibly existing) dates auxiliary file and will append the aggregated data
to the (possibly already existing) file. Note that the `append` mode only works if:

- the schema of the aggregated data is the same of the existing file
- **and** the input files are in the same relative or global path as found in the dates auxiliary file.
- **and** the `parquet` or `csv` output format is used.

.. warning::
    Also note that, if using the `created_at` column to group, all the files produced with the `append` mode
    will have to be aggregated again on the desired keys as **there is no guarantee** that all the SoR from one
    day are in the corresponding daily dump file.
""",
)
parserAggregate.add_argument(
    "-d",
    "--input",
    type=str,
    help="Root folder containing the different platforms / version directories.",
    required=True,
)
parserAggregate.add_argument(
    "-p",
    "--platform",
    type=str,
    help="Platform to aggregate the files from. [global|facebook|...|tiktok|X]",
    default="global",
)
parserAggregate.add_argument(
    "-v", "--version", type=str, help="Version of the files to aggregate. [full|light]", default="full"
)
parserAggregate.add_argument(
    "-o",
    "--output",
    type=str,
    help="Output file name in the format `name.{}`. The placeholder is for the extension.",
    required=True,
)
parserAggregate.add_argument(
    "-c",
    "--config",
    type=str,
    help="The YAML file containing the aggregation configuration. See :class:`dsa_tdb.types.AggregationConfig` for more details.",
    required=True,
)
parserAggregate.add_argument(
    "-i",
    "--from_date",
    type=str,
    help="Date from which (included) to look at dump files. Format YYYY-MM-DD. If not specified, will start from the first date found in the files.",
    default=None,
)
parserAggregate.add_argument(
    "-f",
    "--to_date",
    type=str,
    help="Date until (included) which to look at dump files. Format YYYY-MM-DD. If not specified, will end at the last date found in the files.",
    default=None,
)
parserAggregate.add_argument(
    "-n",
    "--n_workers",
    type=int,
    help="Number of workers to use for the aggregation. If <=0, will use all the available CPUs.",
    default=0,
)
parserAggregate.add_argument(
    "-m",
    "--memory_limit",
    type=str,
    help="The driver memory of Spark. See https://spark.apache.org/docs/latest/configuration.html#memory-management for more details. Do not change it unless you know what you are doing.",
    default=None,
)
parserAggregate.add_argument(
    "--spark_local_dir",
    type=str,
    help="The local directory where to save the temporary files. If None/empty uses the Spark's default. Do not change it unless you know what you are doing.",
    default=None,
)
parserAggregate.add_argument(
    "--override_chunked_subfolder",
    type=str,
    help="Override the default chunked subfolder name. Do not change/use it unless you know what you are doing.",
    default=CHUNKED_FILES_SUBFOLDER_NAME,
)
parserAggregate.add_argument(
    "--loglevel", type=str, help="The logging level. [DEBUG|INFO|WARNING|ERROR|CRITICAL]", default="INFO"
)

# Filterin command:
parserFiltering = subparsers.add_parser(
    "filter",
    description="""Filter the SoRs from the chunked daily dumps.
It will save the filtered SoRs in a custom folder in either csv, parquet or pickle format,
along with the configuration file used for the filtering for later reference.
SoRs can also be appended to an existing file if the `append` mode is set in the configuration.
If the filtering mode is set to `append` in the configuration, it will load and filter **only** the dates that
are not already in the (possibly existing) dates auxiliary file and will append the filtered data
to the (possibly already existing) file. Note that the `append` mode only works if:

- the schema of the aggregated data is the same of the existing file
- **and** the input files are in the same relative or global path as found in the dates auxiliary file.
- **and** the `parquet` or `csv` output format is used.

.. warning::
    Also note that, if using the `created_at` column to group, all the files produced with the `append` mode
    will have to be aggregated again on the desired keys as **there is no guarantee** that all the SoR from one
    day are in the corresponding daily dump file.
""",
)
parserFiltering.add_argument(
    "-d",
    "--input",
    type=str,
    help="Root folder containing the different platforms / version directories.",
    required=True,
)
parserFiltering.add_argument(
    "-p",
    "--platform",
    type=str,
    help="Platform to filter the files from. [global|facebook|...|tiktok|X]",
    default="global",
)
parserFiltering.add_argument(
    "-v", "--version", type=str, help="Version of the files to filter. [full|light]", default="full"
)
parserFiltering.add_argument(
    "-o",
    "--output",
    type=str,
    help="Output file name in the format `name.{}`. The placeholder is for the extension.",
    required=True,
)
parserFiltering.add_argument(
    "-c",
    "--config",
    type=str,
    help="The YAML file containing the filtering configuration. See :class:`dsa_tdb.types.FilteringConfig` for more details.",
    required=True,
)
parserFiltering.add_argument(
    "-i",
    "--from_date",
    type=str,
    help="Date from which (included) to look at dump files. Format YYYY-MM-DD. If not specified, will start from the first date found in the files.",
    default=None,
)
parserFiltering.add_argument(
    "-f",
    "--to_date",
    type=str,
    help="Date until (included) which to look at dump files. Format YYYY-MM-DD. If not specified, will end at the last date found in the files.",
    default=None,
)
parserFiltering.add_argument(
    "-n",
    "--n_workers",
    type=int,
    help="Number of workers to use for the filtering. If <=0, will use all the available CPUs.",
    default=0,
)
parserFiltering.add_argument(
    "-m",
    "--memory_limit",
    type=str,
    help="The driver memory of Spark. See https://spark.apache.org/docs/latest/configuration.html#memory-management for more details. Do not change it unless you know what you are doing.",
    default=None,
)
parserFiltering.add_argument(
    "--spark_local_dir",
    type=str,
    help="The local directory where to save the temporary files. If None/empty uses the Spark's default. Do not change it unless you know what you are doing.",
    default=None,
)
parserFiltering.add_argument(
    "--override_chunked_subfolder",
    type=str,
    help="Override the default chunked subfolder name. Do not change/use it unless you know what you are doing.",
    default=CHUNKED_FILES_SUBFOLDER_NAME,
)
parserFiltering.add_argument(
    "--loglevel", type=str, help="The logging level. [DEBUG|INFO|WARNING|ERROR|CRITICAL]", default="INFO"
)


def _filterFiles(
    input_root_folder: str,
    out_file_name: str,
    platform: str,
    version: str,
    config: Union[str, FilteringConfig],
    start_date: Union[str, None] = None,
    end_date: Union[str, None] = None,
    n_workers: int = 0,
    memory_limit: Union[str, None] = None,
    spark_local_dir: Union[str, None] = None,
    override_chunked_subfolder: str = CHUNKED_FILES_SUBFOLDER_NAME,
):
    """Filter the daily chunked files.
    Will save the filtered rows using the format and schema specified in the
    config file. Will also write an auxiliary csv file (with the same name of
    the `out_file_name`) containing the dates of the daily dumps used for the
    filtering.  It will also make a copy of the configuration file used in the
    same folder of the output file with the same name and the
    `filtering.yaml` file for later reference.
    If the fitlering write mode is set to `append` in the configuration, it will
    load only the dates that are not already in the (possibly existing) dates
    auxiliary file and will append the filtered data to the (possibly already
    existing) file.
    Note that the `append` mode only works if:
    - the schema of the filtered data is the same of the existing file
    - **and** the input files are in the same relative or global path as
      found in the dates auxiliary file.
    - **and** the `parquet` output format is used.

    Parameters
    ----------
    input_root_folder : str
        The root folder containing the different platforms / version directories.
    out_file_name : str
        Output file name in the format name.{}. If the `.{}` is not present, it will be added.
        The placeholder is for the extension.
    platform : str
        Platform to filter the SoRs from. [global|facebook|...|tiktok|X]
        The name of the platform will be sanitized to be used as a folder name using
        the :func:`dsa_tdb.utils.sanitize_platform_name` function.
    version : str
        Version of the files to filter. [full|light]
    start_date : str, optional
        Date from which (included) to look at dump files.
        Format YYYY-MM-DD. If not specified, will start from the first date
        found in the files.
    end_date : str, optional
        Date until (included) which to look at dump files.
        Format YYYY-MM-DD. If not specified, will end at the last date found
        in the files.
    config : str, FilteringConfig
        If a string, the YAML file containing the filtering configuration.
        If a :class:`dsa_tdb.types.FilteringConfig` object, the configuration itself.
        See :class:`dsa_tdb.types.FilteringConfig` for more details.
    n_workers : int, optional
        Number of workers to use for the filtering.
        If <=0, will use all the available CPUs.
    memory_limit : str, optional
        The memory limit of the driver of Spark. See https://spark.apache.org/docs/latest/configuration.html#memory-management for more details.
        Do not change it unless you know what you are doing.
    spark_local_dir : str, optional
        The local directory where to save the temporary files.
        If None/empty uses the Spark's default.
        Do not change it unless you know what you are doing.
    override_chunked_subfolder : str, optional
        Override the default chunked subfolder name.
        Do not change/use it unless you know what you are doing.

    Returns
    -------
    None
    """
    if isinstance(config, str):
        config_dict = yaml.safe_load(open(config))
        config_validated = FilteringConfig(**config_dict)
    else:
        config_validated = config

    input_format = config_validated.input_format
    output_format = config_validated.output_format

    if out_file_name[-3:] != ".{}":
        out_file_name += ".{}"

    platform = sanitize_platform_name(platform, warn_on_change=True)

    local_chunked_subfolder = (
        CHUNKED_FILES_SUBFOLDER_NAME if override_chunked_subfolder is None else override_chunked_subfolder
    )

    (
        files_to_process,
        out_file_mode,
        out_file_dump,
        dates_files_filename,
        out_configuration_filename,
        dates_files_to_remove,
    ) = compute_files_to_process(
        root_folder=input_root_folder,
        local_chunked_subfolder=local_chunked_subfolder,
        out_file_name=out_file_name,
        platform=platform,
        version=TDB_dailyDumpsVersion[version],
        input_format=TDB_chunkFormat[input_format],
        output_format=output_format,
        write_mode=config_validated.write_mode,
        start_date=start_date,
        end_date=end_date,
        step_name="filter",
    )

    # Empty list of files barrier
    if len(files_to_process) == 0:
        logger.info("No files to process. Exiting.")
        return
    else:
        logger.info(f"Will process {len(files_to_process)} remaining files:")
        logger.info(f"from {files_to_process[0]} to {files_to_process[-1]}")

    logging.info("Starting the filtering process.")
    spark = dsa_tdb.utils.spark_session_factory(
        n_workers=n_workers, memory_limit=memory_limit, spark_local_dir=spark_local_dir
    )
    df = loadFile(
        dump_files_pattern=files_to_process,
        columns_to_import=config_validated.columns_to_import,
        platforms_to_exclude=config_validated.platforms_to_exclude,
        columns_datetime=config_validated.columns_datetime,
        spark=spark,
        content_date_range=config_validated.content_date_range,
        decision_date_range=config_validated.decision_date_range,
        created_at_date_range=config_validated.created_at_date_range,
        input_format=input_format,
        del_original=config_validated.delete_original_columns,
        explode_cols=config_validated.horizontally_explode_columns,
        fillna_str=config_validated.fillna_str_value,
        fillna_bool=config_validated.fillna_bool_value,
        columns_to_fill_str=config_validated.columns_to_fill_str,  # type: ignore
        columns_to_fill_bool=config_validated.columns_to_fill_bool,  # type: ignore
        compute_time_to_action=False,
        normalize_platform_name=config_validated.normalize_platform_name,
    )
    # Do the actual filtering:
    df = dsa_tdb.etl._filter_sors(df=df, config=config_validated, spark=spark)

    ################
    # Save results #
    ################
    try:
        if output_format == TDB_chunkFormat.pickle:
            result_ts = df.toPandas()
            pd.to_pickle(result_ts, out_file_dump)
        elif output_format == TDB_chunkFormat.parquet:
            df.write.parquet(out_file_dump, mode=out_file_mode)
        elif output_format == TDB_chunkFormat.csv:
            df.write.csv(out_file_dump, mode=out_file_mode)
        else:
            # Should never happen as we type check the config file:
            raise NotImplementedError(f"Output format {output_format} not implemented.")
    except (KeyboardInterrupt, SystemExit):
        logger.info("Interrupted. Cleaning up.")
        spark.stop()
        if os.path.exists(out_file_dump):
            shutil.rmtree(out_file_dump)
        raise

    # Save the dates and the files used for the aggregation:
    logger.info("Saving the dates and the files used for the filtering.")
    tmp_files = []
    for f in files_to_process:
        try:
            tmp_date = re.search(DAILY_CHUNKED_FOLDER_NAME_REGEX, f).group("date")  # type: ignore
            tmp_date = datetime.strptime(tmp_date, "%Y-%m-%d")
        except Exception as e:
            logger.warning(f"Could not parse the date from the file {f}.")
            logger.warning(e)
            continue

        # New in 0.6.5: we save the relative path
        tmp_files.append({"date": tmp_date, "file": os.path.relpath(f, start=input_root_folder)})
    tmp_files = pd.DataFrame(
        tmp_files,
    )
    tmp_files = tmp_files.sort_values("date")

    if dates_files_to_remove is not None:
        tmp_files = pd.concat([dates_files_to_remove, tmp_files], sort=True, ignore_index=True)  # type: ignore
    # Save the dates and the files used for the aggregation:
    tmp_files.to_csv(dates_files_filename, index=False)

    # Save the configuration file:
    logger.info("Saving the configuration file.")
    with open(out_configuration_filename, "w") as fconfig:
        yaml.safe_dump(config_validated.model_dump(), fconfig)


def _aggregateFiles(
    input_root_folder: str,
    out_file_name: str,
    platform: str,
    version: str,
    config: Union[str, AggregationConfig],
    start_date: Union[str, None] = None,
    end_date: Union[str, None] = None,
    n_workers: int = 0,
    memory_limit: Union[str, None] = None,
    spark_local_dir: Union[str, None] = None,
    override_chunked_subfolder: Union[str, None] = CHUNKED_FILES_SUBFOLDER_NAME,
):
    """Aggregate the files.
    Will save the aggregated file using the format and schema specified in the
    config file. Will also write an auxiliary csv file (with the same name of
    the `out_file_name`) containing the dates of the daily dumps used for the
    aggregation.  It will also make a copy of the configuration file used in the
    same folder of the output file with the same name and the
    `configuration.yaml` file for later reference.
    If the aggregation mode is set to `append` in the configuration, it will
    load only the dates that are not already in the (possibly existing) dates
    auxiliary file and will append the aggregated data to the (possibly already
    existing) file.
    Note that the `append` mode only works if:
    - the schema of the aggregated data is the same of the existing file
    - **and** the input files are in the same relative or global path as
      found in the dates auxiliary file.
    - **and** the `parquet` output format is used.

    .. warning::
        Note that, if using the `created_at` column to group, all the files
        produced with the `append` mode will have to be aggregated again on the
        desired keys as **there is no guarantee** that all the SoR from one day
        are in the corresponding daily dump file.

    Parameters
    ----------
    input_root_folder : str
        The root folder containing the different platforms / version directories.
    out_file_name : str
        Output file name in the format `name.{}` (if the `.{}` is missing it will be added).
        The placeholder is for the extension.
    platform : str
        Platform to aggregate the files from. [global|facebook|...|tiktok|X].
        The name will be sanitized to be used as a folder name using the
        :func:`dsa_tdb.utils.sanitize_platform_name` function.
    version : str
        Version of the files to aggregate. [full|light]
    start_date : str, optional
        Date from which (included) to look at dump files.
        Format YYYY-MM-DD. If not specified, will start from the first date
        found in the files.
    end_date : str, optional
        Date until (included) which to look at dump files.
        Format YYYY-MM-DD. If not specified, will end at the last date found
        in the files.
    config : str, AggregationConfig
        The YAML file containing the aggregation configuration if a string.
        If a :class:`dsa_tdb.types.AggregationConfig` object, the configuration itself.
        See :class:`dsa_tdb.types.AggregationConfig` for more details.
    n_workers : int, optional
        Number of workers to use for the aggregation.
        If <=0, will use all the available CPUs.
    memory_limit : str, optional
        The memory limit of the driver of Spark. See https://spark.apache.org/docs/latest/configuration.html#memory-management for more details.
        Do not change it unless you know what you are doing.
    spark_local_dir : str, optional
        The local directory where to save the temporary files.
        If None/empty uses the Spark's default.
        Do not change it unless you know what you are doing.
    override_chunked_subfolder : str, optional
        Override the default chunked subfolder name.
        Do not change/use it unless you know what you are doing.

    Returns
    -------
    None
    """

    if isinstance(config, str):
        config_dict = yaml.safe_load(open(config))
        config_validated = AggregationConfig(**config_dict)
    else:
        config_validated = config

    input_format = config_validated.input_format
    output_format = config_validated.output_format

    platform = sanitize_platform_name(platform, warn_on_change=True)

    if out_file_name[-3:] != ".{}":
        out_file_name += ".{}"

    local_chunked_subfolder = (
        CHUNKED_FILES_SUBFOLDER_NAME if override_chunked_subfolder is None else override_chunked_subfolder
    )

    (
        files_to_process,
        out_file_mode,
        out_file_dump,
        dates_files_filename,
        out_configuration_filename,
        dates_files_to_remove,
    ) = compute_files_to_process(
        root_folder=input_root_folder,
        local_chunked_subfolder=local_chunked_subfolder,
        out_file_name=out_file_name,
        platform=platform,
        version=TDB_dailyDumpsVersion[version],
        input_format=TDB_chunkFormat[input_format],
        output_format=output_format,
        write_mode=config_validated.write_mode,
        start_date=start_date,
        end_date=end_date,
        step_name="aggregation",
    )

    # Empty list of files barrier
    if len(files_to_process) == 0:
        logger.info("No files to process. Exiting.")
        return
    else:
        logger.info(f"Will process {len(files_to_process)} remaining files:")
        logger.info(f"from {files_to_process[0]} to {files_to_process[-1]}")

    logging.info("Starting the aggregation process.")
    spark = dsa_tdb.utils.spark_session_factory(
        n_workers=n_workers, memory_limit=memory_limit, spark_local_dir=spark_local_dir
    )
    df = loadFile(
        dump_files_pattern=files_to_process,
        columns_to_import=config_validated.columns_to_import,
        platforms_to_exclude=config_validated.platforms_to_exclude,
        columns_datetime=config_validated.columns_datetime,
        spark=spark,
        content_date_range=config_validated.content_date_range,
        decision_date_range=config_validated.decision_date_range,
        created_at_date_range=config_validated.created_at_date_range,
        input_format=input_format,
        del_original=config_validated.delete_original_columns,
        explode_cols=config_validated.horizontally_explode_columns,
        fillna_str=config_validated.fillna_str_value,
        fillna_bool=config_validated.fillna_bool_value,
        columns_to_fill_str=config_validated.columns_to_fill_str,  # type: ignore
        columns_to_fill_bool=config_validated.columns_to_fill_bool,  # type: ignore
        compute_time_to_action=config_validated.compute_time_to_action,
        compute_restriction_duration=config_validated.compute_restriction_duration,
        normalize_platform_name=config_validated.normalize_platform_name,
    )

    # Doing the actual aggregation:
    result_ts = dsa_tdb.etl._aggregate_sors(df=df, config=config_validated, spark=spark)

    try:
        if output_format == TDB_chunkFormat.pickle:
            result_ts = result_ts.toPandas()
            pd.to_pickle(result_ts, out_file_dump)
        elif output_format == TDB_chunkFormat.parquet:
            result_ts.write.parquet(out_file_dump, mode=out_file_mode)
        elif output_format == TDB_chunkFormat.csv:
            result_ts.write.csv(out_file_dump, mode=out_file_mode)
        else:
            # Should never happen as we type check the config file:
            raise NotImplementedError(f"Output format {output_format} not implemented.")
    except (KeyboardInterrupt, SystemExit):
        logger.info("Aggregation interrupted. Cleaning up.")
        spark.stop()
        if os.path.exists(out_file_dump):
            shutil.rmtree(out_file_dump)
        raise

    # Save the dates and the files used for the aggregation:
    logger.info("Saving the dates and the files used for the aggregation.")
    tmp_files = []
    for f in files_to_process:
        try:
            tmp_date = re.search(DAILY_CHUNKED_FOLDER_NAME_REGEX, f).group("date")  # type: ignore
            tmp_date = datetime.strptime(tmp_date, "%Y-%m-%d")
        except Exception as e:
            logger.warning(f"Could not parse the date from the file {f}.")
            logger.warning(e)
            continue

        # New in 0.6.5: we save the relative path
        tmp_files.append({"date": tmp_date, "file": os.path.relpath(f, start=input_root_folder)})
    tmp_files = pd.DataFrame(
        tmp_files,
    )
    tmp_files = tmp_files.sort_values("date")

    if dates_files_to_remove is not None:
        tmp_files = pd.concat([dates_files_to_remove, tmp_files], sort=True, ignore_index=True)  # type: ignore
    # Save the dates and the files used for the aggregation:
    tmp_files.to_csv(dates_files_filename, index=False)

    # Save the configuration file:
    logger.info("Saving the configuration file.")
    with open(out_configuration_filename, "w") as fconfig:
        yaml.safe_dump(config_validated.model_dump(), fconfig)


def _process_platf_to_exclude(args):
    platforms_to_exclude = args.exclude
    if platforms_to_exclude is None or platforms_to_exclude == "":
        platforms_to_exclude = None
    else:
        if "platform" in args and args.platform != "global":
            raise ValueError('--exclude can only be used with "global" platform')
        platforms_to_exclude = platforms_to_exclude.split(",")
        logger.info(f"Excluding platforms: {platforms_to_exclude}")
    return platforms_to_exclude


def main():
    # Create a parser of the arguments:

    # Parse the arguments:
    args = parser.parse_args()

    if args.command == "preprocess":
        # Run the main function:
        logger.setLevel(args.loglevel)
        loglevelname = logging.getLevelName(args.loglevel)
        platforms_to_exclude = _process_platf_to_exclude(args)
        prepare_daily_dumps(
            dump_files_root_folder=args.output,
            version=args.version,
            platform=args.platform,
            platforms_to_exclude=platforms_to_exclude,
            check_sha1=(not args.skip_sha1),
            force_sha1=args.force_sha1,
            from_date=args.from_date,
            to_date=args.to_date,
            n_processes=args.nprocs,
            do_chunking=not args.skip_chunking,
            chunk_size=args.chunk_size,
            chunk_format=args.format,
            delete_original=args.delete_original,
            loglevel=loglevelname,
            override_chunked_subfolder=args.override_chunked_subfolder,
            raise_on_error=args.raise_on_error,
        )
    elif args.command == "aggregate":
        logger.setLevel(args.loglevel)
        loglevelname = logging.getLevelName(args.loglevel)
        # Run the aggregate function:
        _aggregateFiles(
            input_root_folder=args.input,
            platform=args.platform,
            version=args.version,
            start_date=args.from_date,
            end_date=args.to_date,
            out_file_name=args.output,
            config=args.config,
            n_workers=args.n_workers,
            memory_limit=args.memory_limit,
            override_chunked_subfolder=args.override_chunked_subfolder,
            spark_local_dir=args.spark_local_dir,
        )
    elif args.command == "filter":
        logger.setLevel(args.loglevel)
        loglevelname = logging.getLevelName(args.loglevel)
        # Run the filter function:
        _filterFiles(
            input_root_folder=args.input,
            platform=args.platform,
            version=args.version,
            start_date=args.from_date,
            end_date=args.to_date,
            out_file_name=args.output,
            config=args.config,
            n_workers=args.n_workers,
            memory_limit=args.memory_limit,
            override_chunked_subfolder=args.override_chunked_subfolder,
            spark_local_dir=args.spark_local_dir,
        )
    else:
        # Show help of the parser:
        parser.print_help()


if __name__ == "__main__":
    main()
