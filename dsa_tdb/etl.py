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
import logging
import os
from datetime import datetime
from glob import glob
from typing import List, Union

import pandas as pd
import pyarrow as pa
import pyspark.sql.functions as F
import yaml
from pyspark.sql import DataFrame, SparkSession

import dsa_tdb
import dsa_tdb.types as T
from dsa_tdb.types import (
    CONTENT_TYPE_OTHER_NORMALIZATION,
    TDB_AGGREGATED_RAW_SCHEMA,
    TDB_PD_TO_PA_LOOKUP,
    LoadFileArguments,
    RawAndExplodedColumn,
    TDB_columnsFull,
    TDB_datetimeColumns,
    accepted_chunk_formats,
    columns_to_explode,
)
from dsa_tdb.utils import _coalesce_platforms_names_udf, _territories2label_udf, spark_session_factory

logger = logging.getLogger(dsa_tdb.__name__)


def loadSparkDataset(
    files_pattern: str,
    spark: Union[SparkSession, None] = None,
    sql_view_name: str = "dsa_tdb_dataset",
    memory_limit: Union[str, None] = None,
    n_workers: int = 0,
    spark_local_dir: Union[str, None] = None,
) -> tuple[SparkSession, DataFrame]:
    """Initialize a spark session with default configuration.

    Parameters
    ----------
    files_pattern: str
        Path to the dataset to load with spark.
    spark: SparkSession, optional
        An optional spark session to use.
    sql_view_name: str, optional
        An optional name for the created SQL view on the dataset. Defaults to 'dsa_tdb_dataset'.
    memory_limit : str, optional
        Spark memory limit.
    n_workers: int, optional
        Number of spark workers.
    spark_local_dir: str, optional
        Spark local dir value.

    Returns
    -------
    tuple[SparkSession, DataFrame]
        A tuple of the spark session and the loaded spark dataframe.
    """
    if spark is None:
        spark = spark_session_factory(n_workers=n_workers, memory_limit=memory_limit, spark_local_dir=spark_local_dir)
    sparkDf = spark.read.parquet(files_pattern)
    sparkDf.createOrReplaceTempView(sql_view_name)
    return spark, sparkDf


def loadFile(
    dump_files_pattern: Union[str, List[str]],
    spark: SparkSession,
    platforms_to_exclude: Union[List[str], None] = None,
    columns_to_import: Union[List[TDB_columnsFull], None] = None,
    columns_datetime: Union[List[TDB_datetimeColumns], None] = None,
    content_date_range: Union[List[datetime], None] = None,
    decision_date_range: Union[List[datetime], None] = None,
    created_at_date_range: Union[List[datetime], None] = None,
    input_format: str = "csv",
    del_original: bool = True,
    explode_cols: bool = True,
    fillna_str: Union[str, None] = None,
    fillna_bool: Union[bool, None] = False,
    columns_to_fill_str: List[RawAndExplodedColumn] = [
        RawAndExplodedColumn.decision_monetary,  # type: ignore
        RawAndExplodedColumn.decision_provision,  # type: ignore
        RawAndExplodedColumn.decision_account,  # type: ignore
        RawAndExplodedColumn.incompatible_content_illegal,  # type: ignore
        RawAndExplodedColumn.content_language,  # type: ignore
        RawAndExplodedColumn.territorial_scope,  # type: ignore
        RawAndExplodedColumn.automated_decision,  # type: ignore
        RawAndExplodedColumn.automated_detection,
    ],  # type: ignore
    columns_to_fill_bool: list = columns_to_explode["decision_visibility"],
    compute_time_to_action: bool = True,
    compute_restriction_duration: bool = True,
    normalize_platform_name: bool = False,
    normalize_content_type_other: bool = True,
) -> DataFrame:
    """Loads the dataset from the csv dump file.
    Will force datetime columns to be datetime for empty files.

    Parameters
    ----------
    dump_files_pattern : str, List[str]
        The pattern of the dump files to load. Can be a glob pattern.
        If a list is provided, all the files provided are sorted and loaded.
    platforms_to_exclude : list, optional
        The platforms to exclude, by default None.
        Pass the platform names with their simplified name (see
        :attr:`dsa_tdb.fetch.prepare_daily_dumps` for details).
    columns_to_import : list, optional
        The columns to import, by default None
    columns_datetime : list, optional
        The columns to parse as datetime, by default None
    daskClient : Client, optional
        The dask client, by default None
    content_date_range : tuple, optional
        The range of the content date, by default None
    decision_date_range : tuple, optional
        The range of the decision date, by default None
    created_at_date_range : tuple, optional
        The range of the created at date (upload to the DB), by default None
    input_format : str, optional
        The format of the dump file being read, by default 'csv'
        Shall be 'csv' or 'parquet'
    del_original : bool, optional
        Whether to delete the original columns after exploding them, by default False
    explode_cols : bool, optional
        Whether to explode the columns or not, by default True.
        When exploding the columns the
        :attr:dsa_tdb.types.columns_to_explode` are used.
    fillna_str : str, optional
        The value to fill the string columns with, by default 'N/A'
    fillna_bool : bool, optional
        The value to fill the bool columns with, by default False
    columns_to_fill_str : list, optional
        The list of columns to fill with `fillna_str`, by default:
         `['decision_monetary', 'decision_provision',
         'decision_account', 'incompatible_content_illegal',
         'content_language', 'territorial_scope',
         'automated_decision','automated_detection']`
    columns_to_fill_bool : list, optional
        The list of columns to fill with `fillna_bool`, by default:
        :attr:`dsa_tdb.types.columns_to_explode`
    compute_time_to_action : bool, optional
        Whether to compute the time to action and reporting, by default True
    compute_restriction_duration : bool, optional
        Whether to compute the restriction duration time when available, by default True
    normalize_platform_name : bool, optional
        Whether to coalesce platform name when a given platform name changed over time, by default False
        See the :attr:`dsa_tdb.types.coalesce_platforms_names` for details.
    normalize_content_type_other : bool, optional
        Whether to normalize the content type other column, by default False
        See the :attr:`dsa_tdb.types.CONTENT_TYPE_OTHER_NORMALIZATION` for details.

    Returns
    -------
    DataFrame
        The dataframe with the exploded columns.
        This is a spark dataframe.
    """
    _args = LoadFileArguments(
        dump_files_pattern=dump_files_pattern,
        columns_to_import=columns_to_import,
        columns_datetime=columns_datetime,
        content_date_range=content_date_range,
        decision_date_range=decision_date_range,
        created_at_date_range=created_at_date_range,
        input_format=input_format,  # type: ignore
        del_original=del_original,
        explode_cols=explode_cols,
        fillna_str=fillna_str,
        fillna_bool=fillna_bool,
        columns_to_fill_str=columns_to_fill_str,
        columns_to_fill_bool=columns_to_fill_bool,
        compute_time_to_action=compute_time_to_action,
        compute_restriction_duration=compute_restriction_duration,
        normalize_platform_name=normalize_platform_name,
        normalize_content_type_other=normalize_content_type_other,
    )

    columns_to_import = [] if _args.columns_to_import is None else _args.columns_to_import
    if _args.compute_restriction_duration:
        columns_to_import += [
            TDB_columnsFull.end_date_visibility_restriction,
            TDB_columnsFull.end_date_account_restriction,
            TDB_columnsFull.end_date_service_restriction,
            TDB_columnsFull.end_date_monetary_restriction,
        ]
    columns_datetime = [] if _args.columns_datetime is None else _args.columns_datetime

    usecols = list(set(columns_to_import + columns_datetime))
    if len(usecols) == 0:
        raise ValueError("At least one column must be specified in `columns_to_import` or `columns_datetime`.")

    reader = spark.read.__getattribute__(input_format)
    if isinstance(_args.dump_files_pattern, str):
        dump_files_pattern = list(sorted(glob(_args.dump_files_pattern)))
    df = reader(*dump_files_pattern)

    for idx, col in enumerate(usecols.copy()):
        if col not in df.columns:
            logger.warning(
                "Column %s is requested for import but is not in the dataset. Available columns are %s.",
                col,
                df.columns,
            )
            del usecols[idx]
    df.select(*usecols)

    where_queries = []

    if platforms_to_exclude:
        # TODO : try with a single IN query with a list of platforms
        where_queries.append(
            "("
            + " AND ".join([f'{TDB_columnsFull.platform_name} != "{platform}"' for platform in platforms_to_exclude])
            + ")"
        )

    if content_date_range:
        where_queries.append(
            f'({TDB_columnsFull.content_date} BETWEEN "{content_date_range[0]}" AND "{content_date_range[1]}")'
        )
    if decision_date_range:
        where_queries.append(
            f'({TDB_columnsFull.application_date} BETWEEN "{decision_date_range[0]}" AND "{decision_date_range[1]}")'
        )
    if created_at_date_range:
        where_queries.append(
            f'({TDB_columnsFull.created_at} BETWEEN "{created_at_date_range[0]}" AND "{created_at_date_range[1]}")'
        )

    if where_queries:
        where_query = " AND ".join(where_queries)
        df = df.filter(where_query)

    df = loadDataset(
        df,
        del_original=del_original,
        spark=spark,
        explode_cols=explode_cols,
        fillna_str=fillna_str,
        fillna_bool=fillna_bool,
        columns_to_fill_str=columns_to_fill_str,
        columns_to_fill_bool=columns_to_fill_bool,
        compute_time_to_action=_args.compute_time_to_action,
        compute_restriction_duration=_args.compute_restriction_duration,
        normalize_platform_name=_args.normalize_platform_name,
        normalize_content_type_other=_args.normalize_content_type_other,
    )

    return df


def loadDataset(
    df: DataFrame,
    spark: SparkSession,
    del_original: bool = True,
    explode_cols: bool = True,
    fillna_str: Union[str, None] = None,
    fillna_bool: Union[bool, None] = False,
    columns_to_fill_str: list = [
        "decision_monetary",
        "decision_provision",
        "decision_account",
        "incompatible_content_illegal",
        "content_language",
        "territorial_scope",
        "automated_decision",
        "automated_detection",
    ],
    columns_to_fill_bool: list = columns_to_explode["decision_visibility"],
    compute_time_to_action: bool = True,
    compute_restriction_duration: bool = True,
    normalize_platform_name: bool = False,
    normalize_content_type_other: bool = True,
) -> DataFrame:
    """The actual ETL.
    For each col in columns to explode created the columns of the possible values
    and fills them with bool checking for the value.

    Operates on the input df without copy.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input dataframe, could also be a dask dataframe.
        In this case the dask client must be provided in the `client` parameter.
    spark : SparkSession
        The spark session handle.
    del_original : bool, optional
        Whether to delete the original columns after exploding them, by default False
    explode_cols : bool, optional
        whether to explode the columns or not, by default True.
        When exploding the columns the
        :attr:dsa_tdb.types.columns_to_explode` are used.
    fillna_str : str, optional
        The value to fill the string columns with, by default 'N/A'
    fillna_bool : bool, optional
        The value to fill the bool columns with, by default False
    columns_to_fill_str : list, optional
        The list of columns to fill with `fillna_str`, by default:
         ['decision_monetary', 'decision_provision',
         'decision_account', 'incompatible_content_illegal',
         'content_language', 'territorial_scope',
         'automated_decision','automated_detection']
    columns_to_fill_bool : list, optional
        The list of columns to fill with `fillna_bool`, by default:
        :attr:`dsa_tdb.types.columns_to_explode``['decision_visibility']`
    compute_time_to_action : bool, optional
        Whether to compute the time to action and reporting, by default True
    compute_restriction_duration : bool, optional
        Whether to compute the restriction duration time when available, by default True
    normalize_platform_name : bool, optional
        Whether to coalesce platform name when a given platform name changed over time, by default False
        See the :attr:`dsa_tdb.types.coalesce_platforms_names` for details.
    normalize_content_type_other : bool, optional
        Whether to normalize the content type other column, by default False
        See the :attr:`dsa_tdb.types.CONTENT_TYPE_OTHER_NORMALIZATION` for details.

    Returns
    -------
    pd.DataFrame
        The dataframe with the exploded columns.
        This is a dask dataframe if the input was a dask dataframe.
    """
    if explode_cols:
        df = sparkExplodeColumns(
            df, spark=spark, delete_original=del_original, normalize_content_type_other=normalize_content_type_other
        )
        logger.info("Columns after exploding: %s", df.columns)
    else:
        # Array columns are now sorted at chunk time, deprecated
        # df = sparkSortArrayValues(df, spark=spark)
        pass

    if "territorial_scope" in df.columns:
        df = df.withColumn(
            TDB_columnsFull.territorial_scope.value,
            _territories2label_udf(F.col(TDB_columnsFull.territorial_scope.value)),
        )

    if compute_time_to_action:
        # Time to action and reporting
        if TDB_columnsFull.application_date.value in df.columns and TDB_columnsFull.content_date.value in df.columns:
            # The time between content and decision in days
            df = df.withColumn(
                "time_to_action",
                F.datediff(F.col(TDB_columnsFull.application_date.value), F.col(TDB_columnsFull.content_date.value)),
            )
        if TDB_columnsFull.created_at.value in df.columns and TDB_columnsFull.content_date.value in df.columns:
            # The time between content and reporting in days
            df = df.withColumn(
                "time_to_report",
                F.datediff(F.col(TDB_columnsFull.created_at.value), F.col(TDB_columnsFull.content_date.value)),
            )
        if TDB_columnsFull.created_at.value in df.columns and TDB_columnsFull.application_date.value in df.columns:
            # The time between decision and reporting in days
            df = df.withColumn(
                "time_to_upload",
                F.datediff(F.col(TDB_columnsFull.created_at.value), F.col(TDB_columnsFull.application_date.value)),
            )

    if compute_restriction_duration:
        df = df.withColumn(
            "restriction_duration",
            F.datediff(
                F.coalesce(
                    F.col(TDB_columnsFull.end_date_visibility_restriction.value),
                    F.col(TDB_columnsFull.end_date_account_restriction.value),
                    F.col(TDB_columnsFull.end_date_service_restriction.value),
                    F.col(TDB_columnsFull.end_date_monetary_restriction.value),
                ),
                F.col(TDB_columnsFull.application_date.value),
            ),
        )

    if fillna_str is not None and columns_to_fill_str:
        df = df.fillna(fillna_str, subset=[c for c in columns_to_fill_str if c in df.columns])
    if fillna_bool is not None and columns_to_fill_bool:
        df = df.fillna(fillna_bool, subset=[c for c in columns_to_fill_bool if c in df.columns])

    if "platform_id" in df.columns:
        if "platform_name" in df.columns:
            logger.warning("platform id and name present, doing nothing to insert platform name.")
    else:
        if "platform_name" not in df.columns:
            logger.warning("No platform id or name present")

    if TDB_columnsFull.platform_name.value in df.columns and normalize_platform_name:
        df = df.withColumn(
            TDB_columnsFull.platform_name.value,
            _coalesce_platforms_names_udf(F.col(TDB_columnsFull.platform_name.value)),
        )

    return df


def _enforce_columns_to_explode(
    df: DataFrame, spark: SparkSession, delete_original_columns: bool, normalize_content_type_other: bool
) -> DataFrame:
    # Check if the exploded columns are already there otherwise explode them
    tmp_cols = df.columns
    missing_columns = [
        col_explode
        for col, exploded_cols in columns_to_explode.items()
        for col_explode in exploded_cols
        if col in tmp_cols and col_explode not in tmp_cols
    ]
    if missing_columns:
        logger.info("Columns to explode are missing: %s", missing_columns)
        df = sparkExplodeColumns(
            df=df,
            spark=spark,
            delete_original=delete_original_columns,
            normalize_content_type_other=normalize_content_type_other,
        )
    return df


def filter_SoRs(
    spark: SparkSession,
    df: DataFrame,
    columns_to_import: Union[List[TDB_columnsFull], None] = None,
    horizontally_explode_columns: bool = True,
    delete_original_columns: bool = False,
    normalize_platform_name: bool = False,
    platforms_to_exclude: Union[List[str], None] = None,
    platforms_to_include: Union[List[str], None] = None,
    created_at_dt_floor: Union[str, None] = None,
    config_file: Union[str, None] = None,
    **kwargs,
) -> DataFrame:
    """Filters the SoRs from the dataframe.
    The configuration can be passed either using the provided and additional keyword arguments
    or by providing a configuration file in `config_file`.
    Note that if both are provided, the keyword arguments will take precedence.

    Parameters
    ----------
    spark : SparkSession
        The spark session handle.
    df : DataFrame
        The input dataframe.
    columns_to_import : list, optional
        The columns to select from the df **before** exploding them, by default None, in which case all the columns are imported.
    horizontally_explode_columns : bool, optional
        Whether to horizontally explode the columns or not, by default True.
    delete_original_columns : bool, optional
        Whether to delete the original columns after exploding them, by default False.
    normalize_platform_name : bool, optional
        Whether to normalize the platform name, by default False.
    platforms_to_exclude : list, optional
        The platforms to exclude, by default None.
    platforms_to_include : list, optional
        The platforms to include, by default None.
    created_at_dt_floor : str, optional
        The datetime floor for the created_at column, by default None.
    config_file : str, optional
        The configuration file, by default None.
    **kwargs : dict
        The filter arguments. these are all the remaining entries of :attr:`dsa_tdb.types.FilteringConfig`
        that are not directly exposed in the function arguments.

    Returns
    -------
    DataFrame
        The filtered dataframe.
    """
    tmp_config = {}
    if config_file:
        with open(config_file) as f:
            tmp_config.update(yaml.safe_load(f))

    tmp_config.update(
        dict(
            columns_to_import=columns_to_import,
            horizontally_explode_columns=horizontally_explode_columns,
            delete_original_columns=delete_original_columns,
            normalize_platform_name=normalize_platform_name,
            platforms_to_exclude=platforms_to_exclude,
            platforms_to_include=platforms_to_include,
            created_at_dt_floor=created_at_dt_floor,
        )
    )
    tmp_config.update(kwargs)

    config = dsa_tdb.types.FilteringConfig(**tmp_config)

    df = _filter_sors(df, spark, config)

    return df


def _filter_sors(df: DataFrame, spark: SparkSession, config: dsa_tdb.types.FilteringConfig) -> DataFrame:
    """Filters the SoRs from the dataframe.

    Parameters
    ----------
    df : DataFrame
        The input dataframe.
    spark : SparkSession
        The spark session handle.
    config : dsa_tdb.types.FilteringConfig
        The filtering configuration.

    Returns
    -------
    DataFrame
        The filtered dataframe.
    """
    if config.columns_to_import:
        df = df.select(*(config.columns_to_import + config.columns_datetime))

    if config.horizontally_explode_columns:
        df = _enforce_columns_to_explode(
            df=df,
            spark=spark,
            delete_original_columns=config.delete_original_columns,
            normalize_content_type_other=config.normalize_content_type_other,
        )

    if config.created_at_dt_floor is not None:
        df = df.withColumn("created_at", F.date_trunc(config.created_at_dt_floor, "created_at"))

    # Upstream sampling
    if config.upstream_sampling:
        if config.upstream_sampling >= 1:
            n_rows = int(config.upstream_sampling)
            logger.info(f"Upstream sampling: keeping {n_rows} rows before filtering.")
            df = df.limit(n_rows)
        elif config.upstream_sampling > 0:
            logger.info(f"Upstream sampling: keeping {config.upstream_sampling * 100}% of the rows before filtering.")
            df = df.sample(withReplacement=False, fraction=config.upstream_sampling)

    ########################
    # Platforms to include #
    ########################
    if config.platforms_to_include:
        df = df.filter(F.col(TDB_columnsFull.platform_name.value).isin(config.platforms_to_include))

    ####################################################
    # Single point for all the bool (exploded) columns #
    ####################################################
    if config.bool_columns_to_check:
        if config.bool_columns_to_check_operator is None:
            raise ValueError("The operator for the boolean columns to check is not specified.")
        bool_sql = "SELECT * FROM df WHERE "
        bool_sql += config.bool_columns_to_check_operator.join([
            f" {col} = true " for col in config.bool_columns_to_check
        ])
        df.createOrReplaceTempView("df")
        df = spark.sql(bool_sql)

    ####################################
    # Single value columns' filtering: #
    ####################################
    # TODO: the check on the length should also check for duplicates.
    if config.decision_monetary:
        if len(config.decision_monetary) > 0:
            if len(config.decision_monetary) == len(T.DecisionMonetary):
                logger.info("All the decision monetary are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the decision monetary {config.decision_monetary}.")
                df = df.filter(F.col(TDB_columnsFull.decision_monetary.value).isin(config.decision_monetary))
        else:
            logger.info("No decision monetary selected. Skipping the filtering.")

    if config.decision_provision:
        if len(config.decision_provision) > 0:
            if len(config.decision_provision) == len(T.DecisionProvision):
                logger.info("All the decision provisions are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the decision provisions {config.decision_provision}.")
                df = df.filter(F.col(TDB_columnsFull.decision_provision.value).isin(config.decision_provision))
        else:
            logger.info("No decision provision selected. Skipping the filtering.")

    if config.decision_account:
        if len(config.decision_account) > 0:
            if len(config.decision_account) == len(T.DecisionAccount):
                logger.info("All the decision accounts are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the decision accounts {config.decision_account}.")
                df = df.filter(F.col(TDB_columnsFull.decision_account.value).isin(config.decision_account))
        else:
            logger.info("No decision account selected. Skipping the filtering.")

    if config.category:
        if len(config.category) > 0:
            if len(config.category) == len(T.Category):
                logger.info("All the categories are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the categories {config.category}.")
                df = df.filter(F.col(TDB_columnsFull.category.value).isin(config.category))
        else:
            logger.info("No category selected. Skipping the filtering.")

    if config.decision_ground:
        if len(config.decision_ground) > 0:
            if len(config.decision_ground) == len(T.DecisionGround):
                logger.info("All the decision grounds are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the decision grounds {config.decision_ground}.")
                df = df.filter(F.col(TDB_columnsFull.decision_ground.value).isin(config.decision_ground))
        else:
            logger.info("No decision ground selected. Skipping the filtering.")

    if config.automated_detection:
        if len(config.automated_detection) > 0:
            if len(config.automated_detection) == T.AutomatedDetection:
                logger.info("All the automated detections are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the automated detection {config.automated_detection}.")
                df = df.filter(F.col(TDB_columnsFull.automated_detection.value).isin(config.automated_detection))
        else:
            logger.info("No automated detection selected. Skipping the filtering.")

    if config.automated_decision:
        if len(config.automated_decision) > 0:
            if len(config.automated_decision) == T.AutomatedDecision:
                logger.info("All the automated decisions are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the automated decision {config.automated_decision}.")
                df = df.filter(F.col(TDB_columnsFull.automated_decision.value).isin(config.automated_decision))
        else:
            logger.info("No automated decision selected. Skipping the filtering.")

    if config.source_type:
        if len(config.source_type) > 0:
            if len(config.source_type) == len(T.SourceType):
                logger.info("All the source types are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the source types {config.source_type}.")
                df = df.filter(F.col(TDB_columnsFull.source_type.value).isin(config.source_type))
        else:
            logger.info("No source type selected. Skipping the filtering.")

    if config.account_type:
        if len(config.account_type) > 0:
            if len(config.account_type) == len(T.AccountType):
                logger.info("All the account types are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the account types {config.account_type}.")
                df = df.filter(F.col(TDB_columnsFull.account_type.value).isin(config.account_type))
        else:
            logger.info("No account type selected. Skipping the filtering.")

    if config.incompatible_content_illegal:
        if len(config.incompatible_content_illegal) > 0:
            if len(config.incompatible_content_illegal) == T.IncompatibleContentIllegal:
                logger.info("All the incompatible content illegal are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the incompatible content illegal {config.incompatible_content_illegal}.")
                df = df.filter(
                    F.col(TDB_columnsFull.incompatible_content_illegal.value).isin(config.incompatible_content_illegal)
                )
        else:
            logger.info("No incompatible content illegal selected. Skipping the filtering.")

    if config.content_language:
        if len(config.content_language) > 0:
            if len(config.content_language) == T.ContentLanguage:
                logger.info("All the content languages are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the content languages {config.content_language}.")
                df = df.filter(F.col(TDB_columnsFull.content_language.value).isin(config.content_language))
        else:
            logger.info("No content language selected. Skipping the filtering.")

    ##########################
    # Multiple value columns #
    ##########################
    if config.decision_visibility:
        if len(config.decision_visibility) > 0:
            if len(config.decision_visibility) == len(T.DecisionVisibility):
                logger.info("All the decision visibility are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the decision visibility {config.decision_visibility}.")
                df = df.filter(
                    " OR ".join([
                        f' RLIKE({TDB_columnsFull.decision_visibility.value}, "[\\",\']{dv}[\\",\']") '
                        for dv in config.decision_visibility
                    ])
                )
        else:
            logger.info("No decision visibility selected. Skipping the filtering.")

    if config.content_type:
        if len(config.content_type) > 0:
            if len(config.content_type) == len(T.ContentType):
                logger.info("All the content types are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the content types {config.content_type}.")
                df = df.filter(
                    " OR ".join([
                        f' RLIKE({TDB_columnsFull.content_type.value}, "[\\",\']{ct}[\\",\']") '
                        for ct in config.content_type
                    ])
                )
        else:
            logger.info("No content type selected. Skipping the filtering.")

    if config.category_addition:
        if len(config.category_addition) > 0:
            if len(config.category_addition) == len(T.Category):
                logger.info("All the category additions are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the category additions {config.category_addition}.")
                df = df.filter(
                    " OR ".join([
                        f' RLIKE({TDB_columnsFull.category_addition.value}, "[\\",\']{ca}[\\",\']") '
                        for ca in config.category_addition
                    ])
                )
        else:
            logger.info("No category addition selected. Skipping the filtering.")

    if config.category_specification:
        if len(config.category_specification) > 0:
            if len(config.category_specification) == len(T.Keyword):
                logger.info("All the keywords are selected. Skipping the filtering.")
            else:
                logger.info(f"Filtering the category specifications {config.category_specification}.")
                df = df.filter(
                    " OR ".join([
                        f' RLIKE({TDB_columnsFull.category_specification.value}, "[\\",\']{cs}[\\",\']") '
                        for cs in config.category_specification
                    ])
                )
        else:
            logger.info("No category specification selected. Skipping the filtering.")

    if config.territorial_scope:
        if len(config.territorial_scope) > 0:
            if len(config.territorial_scope) == len(T.TerritorialScope):
                logger.info("All the territorial scopes are selected. Skipping the filtering.")
            else:
                # TODO: we have a collision between `EEA` and `EEA_no_IS`. We should fix it.
                logger.info(f"Filtering the territorial scopes {config.territorial_scope}.")
                df = df.filter(
                    " OR ".join([
                        f' RLIKE({TDB_columnsFull.territorial_scope.value}, "[\\",\']{ts}[\\",\']") '
                        for ts in config.territorial_scope
                    ])
                )
        else:
            logger.info("No territorial scope selected. Skipping the filtering.")

    ####################
    # Free text fields #
    ####################
    fields = [
        (
            TDB_columnsFull.decision_visibility_other.value,
            config.decision_visibility_other,
            config.decision_visibility_other_to_lower,
        ),
        (
            TDB_columnsFull.decision_monetary_other.value,
            config.decision_monetary_other,
            config.decision_monetary_other_to_lower,
        ),
        (TDB_columnsFull.decision_facts.value, config.decision_facts, config.decision_facts_to_lower),
        (
            TDB_columnsFull.decision_ground_reference_url.value,
            config.decision_ground_reference_url,
            config.decision_ground_reference_url_to_lower,
        ),
        (
            TDB_columnsFull.illegal_content_legal_ground.value,
            config.illegal_content_legal_ground,
            config.illegal_content_legal_ground_to_lower,
        ),
        (
            TDB_columnsFull.illegal_content_explanation.value,
            config.illegal_content_explanation,
            config.illegal_content_explanation_to_lower,
        ),
        (
            TDB_columnsFull.incompatible_content_ground.value,
            config.incompatible_content_ground,
            config.incompatible_content_ground_to_lower,
        ),
        (
            TDB_columnsFull.incompatible_content_explanation.value,
            config.incompatible_content_explanation,
            config.incompatible_content_explanation_to_lower,
        ),
        (TDB_columnsFull.content_type_other.value, config.content_type_other, config.content_type_other_to_lower),
        (
            TDB_columnsFull.category_specification_other.value,
            config.category_specification_other,
            config.category_specification_other_to_lower,
        ),
        (TDB_columnsFull.source_identity.value, config.source_identity, config.source_identity_to_lower),
    ]
    for field_value, regex, to_lower in fields:
        if regex:
            logger.info(f'Filtering the {"lowered " if to_lower else ""}{field_value} with the regex: "{regex}".')
            tmp_col = F.col(field_value)
            if to_lower:
                tmp_col = F.lower(tmp_col)
            df = df.filter(F.rlike(tmp_col, F.lit(regex)))

    #####################
    # Restriction dates #
    #####################
    if (
        config.end_date_account_restriction
        or config.end_date_monetary_restriction
        or config.end_date_service_restriction
        or config.end_date_visibility_restriction
    ):
        raise NotImplementedError("The end date restriction filtering is not implemented yet.")

    # Downstream sampling
    if config.downstream_sampling:
        if config.downstream_sampling >= 1:
            n_rows = int(config.downstream_sampling)
            logger.info(f"downstream sampling: keeping {n_rows} rows before filtering.")
            df = df.limit(n_rows)
        elif config.downstream_sampling > 0:
            logger.info(
                f"downstream sampling: keeping {config.downstream_sampling * 100}% of the rows after filtering."
            )
            df = df.sample(withReplacement=False, fraction=config.downstream_sampling)

    return df


def aggregate_SoRs(
    spark: SparkSession,
    df: DataFrame,
    columns_to_group: List[T.RawAndExplodedColumns],
    horizontally_exploded_columns: bool = False,
    delete_original_columns: bool = False,
    normalize_platform_name: bool = False,
    platforms_to_exclude: Union[List[str], None] = None,
    created_at_dt_floor: str = "day",
    config_file: Union[str, None] = None,
    **kwargs,
) -> DataFrame:
    """Aggregates the SoRs from the dataframe.

    Parameters
    ----------
    spark : SparkSession
        The spark session handle.
    df : DataFrame
        The input dataframe.
    columns_to_group : List[T.RawAndExplodedColumns]
        The columns to group by.
    horizontally_exploded_columns : bool, optional
        Whether to horizontally explode the columns or not, by default False
    delete_original_columns : bool, optional
        Whether to delete the original columns after horizontally exploding them, by default False
    normalize_platform_name : bool, optional
        Whether to coalesce platform name when a given platform name changed over time, by default False
        See the :attr:`dsa_tdb.types.coalesce_platforms_names` for details.
    platforms_to_exclude : Union[List[str],None], optional
        The platforms to exclude, by default None
    created_at_dt_floor : str, optional
        The date floor for the created at column, by default 'day'
        See the [spark documentation for the `date_trunc` function](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.date_trunc.html)
        for other possible values.
    **kwargs : dict
        The aggregation arguments. These are all the remaining entries of :attr:`dsa_tdb.types.AggregationConfig`
        that are not directly exposed in the function arguments.

    Returns
    -------
    DataFrame
        The aggregated dataframe.
    """
    if config_file:
        with open(config_file) as f:
            tmp_config = yaml.safe_load(f)
    tmp_config.update(
        dict(
            columns_to_group=columns_to_group,
            horizontally_exploded_columns=horizontally_exploded_columns,
            delete_original_columns=delete_original_columns,
            normalize_platform_name=normalize_platform_name,
            platforms_to_exclude=platforms_to_exclude,
            created_at_dt_floor=created_at_dt_floor,
        )
    )
    tmp_config.update(kwargs)

    config = dsa_tdb.types.AggregationConfig(**tmp_config)

    df = _aggregate_sors(df=df, spark=spark, config=config)
    return df


def _aggregate_sors(df: DataFrame, spark: SparkSession, config: dsa_tdb.types.AggregationConfig) -> DataFrame:
    """Aggregates the SoRs from the dataframe.

    Parameters
    ----------
    df : DataFrame
        The input dataframe.
    spark : SparkSession
        The spark session handle.
    config : dsa_tdb.types.AggregationConfig
        The aggregation configuration as in :attr:`dsa_tdb.types.AggregationConfig`.

    Returns
    -------
    DataFrame
        The aggregated dataframe.
    """
    if config.horizontally_explode_columns:
        df = _enforce_columns_to_explode(
            df=df,
            spark=spark,
            delete_original_columns=config.delete_original_columns,
            normalize_content_type_other=config.normalize_content_type_other,
        )

    if config.created_at_dt_floor is not None:
        df = df.withColumn("created_at", F.date_trunc(config.created_at_dt_floor, "created_at"))

    agg_list = [F.count_distinct("uuid").alias("count")]
    if config.compute_time_to_action:
        agg_list.extend([
            F.mean("time_to_action").alias("time_to_action"),
            F.mean("time_to_report").alias("time_to_report"),
            F.mean("time_to_upload").alias("time_to_upload"),
        ])

    # Compute columns to group by
    columns_to_group = config.columns_to_group
    if not columns_to_group:
        # These are the columns to group by when aggregating the data.
        # The values of these columns will be used to group the data
        # and the count of SoR entries will be calculated for each group.
        if config.columns_to_import is None:
            config.columns_to_import = []
        if config.columns_datetime is None:
            config.columns_datetime = []
        imported_columns = config.columns_to_import + config.columns_datetime
        exploded_columns = {k: v for (k, v) in columns_to_explode.items() if k in imported_columns}
        # Update the lists, exploded columns are not directly imported but to be replaced by their exploded counterparts.
        imported_columns = [x for x in imported_columns if x not in exploded_columns]
        exploded_columns = [item for new_columns in exploded_columns.values() for item in new_columns]
        columns_to_group = [column for column in (imported_columns + exploded_columns) if column not in ["uuid"]]

    # Eventually compute the restriction duration time column
    if config.compute_restriction_duration:
        columns_to_group += ["restriction_duration"]

    # Doing the actual aggregation:
    for idx, col in enumerate(columns_to_group.copy()):
        if col not in df.columns:
            logger.warning(
                "Column %s is requested for aggregation but is not in the dataset. Available columns are %s.",
                col,
                df.columns,
            )
            del columns_to_group[idx]
    result_ts = df.groupBy(*columns_to_group).agg(*agg_list)  # type: ignore

    logger.debug(f"Aggregated data columns:\n{result_ts.columns}")
    logger.debug(f"Aggregated data dtypes:\n{result_ts.schema}")
    logger.debug(f"Colums grouped by:\n{columns_to_group}")

    return result_ts


def _enforceAggregatedSchema(df: pd.DataFrame, use_raw: bool = True) -> pd.DataFrame:
    """Enforces the schema for the aggregated data."""
    if not use_raw:
        raise NotImplementedError("Non-raw schema not implemented yet")
    ref_schema = TDB_AGGREGATED_RAW_SCHEMA

    for c in df.columns:
        try:
            out_type = ref_schema[c]
        except KeyError:
            logger.exception(f"Column {c} not in the {ref_schema} schema, check your aggregation schema.")
            raise KeyError(f"Column {c} not in the {ref_schema} schema, check your aggregation schema.")

        df[c] = df[c].astype(out_type)

    return df


def _write_out_chunk(chunk: pd.DataFrame, folder_out: str, out_format: str, chunk_size: int, part: int) -> pd.DataFrame:
    """
    Writes out a chunk of a pandas DataFrame to a specified folder in either parquet or csv format.

    Args:
        chunk (pd.DataFrame): The chunk of data to be written out.
        folder_out (str): The path to the output folder.
        out_format (str): The format of the output file. Must be either 'parquet' or 'csv'.
        chunk_size (int): The number of rows in each chunk.
        part (int): The part number of the output file.

    Returns:
        pd.DataFrame: The remaining chunk of data after writing out the specified chunk.

    Raises:
        ValueError: If the out_format is not one of the accepted formats ('parquet' or 'csv').
    """
    if "content_id_ean" in chunk.columns:
        chunk.drop(columns=["content_id_ean"], inplace=True)
    slice_to_write = chunk.iloc[:chunk_size].copy(deep=True)
    slice_to_return = chunk.iloc[chunk_size:].copy(deep=True)
    if out_format == "parquet":
        slice_to_write = _enforceAggregatedSchema(slice_to_write)
        try:
            schema_out = pa.schema([
                pa.field(c, TDB_PD_TO_PA_LOOKUP[TDB_AGGREGATED_RAW_SCHEMA[c]]) for c in slice_to_write.columns
            ])
        except KeyError as e:
            logger.exception("Column not in the schema, check your aggregation schema.")
            raise KeyError(f"Column {e} not in the schema, check your aggregation schema.")
        slice_to_write.to_parquet(
            os.path.join(folder_out, "part-%04d.parquet" % part), index=False, coerce_timestamps="ms", schema=schema_out
        )
    elif out_format == "csv":
        slice_to_write.to_csv(
            os.path.join(folder_out, "part-%04d.csv.gz" % part), index=False, header=True, compression="gzip"
        )  # type: ignore
    else:
        raise ValueError(f"Format must be one of {accepted_chunk_formats}")
    return slice_to_return


def sparkExplodeColumns(
    df: DataFrame, spark: SparkSession, delete_original: bool = False, normalize_content_type_other: bool = False
) -> DataFrame:
    """This function adds the exploded columns to the spark dataframe.

    .. note::
        Some free-text columns have similar names with the exploded columns (see
        :attr:`dsa_tdb.types.EXPLODED_COLUMNS`) with the exception of the raw columns being lowercase
        and the exploded columns being uppercase. Remember that lowercase **always** refers to the raw,
        free-text columns, whereas uppercase columns names **always** refers to the exploded columns.

    .. note::
        The only array column not exploded is the `territoial_scope` column. This is because the
        column is guaranteed to be sorted and we map it internally in the `territories2label` function.

    Parameters
    ----------
    df : DataFrame
        The spark dataframe.
    spark : SparkSession
        The spark session handle.
    delete_original : bool, optional
        Whether to delete the original columns after exploding them, by default False.
    normalize_content_type_other : bool, optional
        Whether to normalize the content type other column, by default False.
        See the :attr:`dsa_tdb.types.CONTENT_TYPE_OTHER_NORMALIZATION` for details.

    Returns
    -------
    DataFrame
        The spark dataframe with the horizontally exploded columns.
    """

    df_columns = set(df.columns)
    tmp_cols_to_explode = columns_to_explode.keys() & df_columns

    if tmp_cols_to_explode:
        logger.info("Horizontally exploding columns %s", tmp_cols_to_explode)
        # Select all the columns that are not in the columns to explode
        # Also select the raw ones if we do not want to delete them
        first_selection = df_columns - tmp_cols_to_explode if delete_original else df_columns
        SQL_query = f'SELECT {", ".join(first_selection)}'

        for col in tmp_cols_to_explode:
            logger.info("Horizontally exploding column %s", col)
            vals = columns_to_explode[col]
            for v in vals:
                SQL_query += f', coalesce(rlike({col}, "[\\",\']{v}[\\",\']"), false) AS {v}'
        SQL_query += " FROM df_sp"
        df.createOrReplaceTempView("df_sp")
        logger.debug("Corresponding SQL query: \n%s", SQL_query)
        df_exploded = spark.sql(SQL_query)

        if normalize_content_type_other:
            if TDB_columnsFull.content_type_other in df.columns:
                # NOTE: the extra content type columns are already in the exploded dataframe
                # from the previous step. If we do not wan them, we drop them in the else clause
                logger.info("Normalizing content type other")
                columns_to_keep_as_is = set(df_exploded.columns) - set(CONTENT_TYPE_OTHER_NORMALIZATION.keys())
                SQL_query = "SELECT {}, ".format(", ".join(columns_to_keep_as_is))
                for content_type, content_flags in CONTENT_TYPE_OTHER_NORMALIZATION.items():
                    logger.info("Normalizing content type %s with flags %s", content_type, content_flags)
                    tmp_query = "coalesce("
                    src_col = TDB_columnsFull.content_type_other
                    for flag in content_flags:
                        flag = flag.lower()
                        tmp_query += f"like(lower(expl.{src_col}), '%%{flag}%%'), "
                    SQL_query += f"({tmp_query[:-2]}, false)) AS {content_type},"

                SQL_query = SQL_query[:-1] + " FROM df_sp_exploded as expl"
                logger.debug("Corresponding SQL query: \n%s", SQL_query)
                df_exploded.createOrReplaceTempView("df_sp_exploded")
                df_expl_norm = spark.sql(SQL_query)
            else:
                logger.warning(
                    f"Not normalizing content type other because the `{TDB_columnsFull.content_type_other}` column is not present."
                )
                df_expl_norm = df_exploded
        else:
            # I do not want the extra content type columns, so I drop them
            columns_to_keep = set(df_exploded.columns) - set(CONTENT_TYPE_OTHER_NORMALIZATION.keys())
            df_expl_norm = df_exploded.select(*columns_to_keep)

        return df_expl_norm
    else:
        logger.info("No columns to explode, returning the input dataframe")
        return df


def sparkSortArrayValues(df: DataFrame, spark: SparkSession) -> DataFrame:
    """This function sorts the array values in the spark dataframe.
    It expects the columns to be exploded to be filled with string like
    '["val1","val2",...]' or empty string / null.

    .. note::
        The only array column not exploded is the `territoial_scope` column. This is because the
        column is guaranteed to be sorted and we map it internally in the `territories2label` function.

    Parameters
    ----------
    df : DataFrame
        The spark dataframe.
    spark : SparkSession
        The spark session handle.

    Returns
    -------
    DataFrame
        The spark dataframe with the columns to explode sorted.
    """

    df_columns = set(df.columns)
    tmp_cols_to_sort = columns_to_explode.keys() & df_columns

    if tmp_cols_to_sort:
        logger.info("Sorting columns %s", tmp_cols_to_sort)
        SQL_query = f'SELECT {", ".join(df_columns - tmp_cols_to_sort)}'
        for col in tmp_cols_to_sort:
            logger.info("Horizontally sorting column %s", col)
            SQL_query += f", concat('[', array_join(array_sort(split(replace(replace({col},'[', ''), ']', ''), ',')), ','), ']') AS {col}"
        SQL_query += " FROM df_sp"
        df.createOrReplaceTempView("df_sp")
        logger.debug("Corresponding SQL query: \n%s", SQL_query)
        df_sorted = spark.sql(SQL_query)

        return df_sorted
    else:
        logger.info("No columns to sort, returning the input dataframe")
        return df
