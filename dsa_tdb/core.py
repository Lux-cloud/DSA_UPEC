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
from datetime import datetime
from typing import List, Union

import pandas as pd
import psutil
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import dsa_tdb
import dsa_tdb.types as T
from dsa_tdb.utils import get_files_to_load

logger = logging.getLogger(dsa_tdb.__name__)


class TDB_DataFrame:
    # TODO: evaluate if this class should just implement some methods and inherit from a Spark DataFrame
    # TODO: methods in cli should initialize this class and call the methods instead of reimplemting the logic
    #       This is currently doable via the sideload of the data, but it is not the best way to do it.
    # TODO: most of the consistency checks should be done via a decorator
    """The base class for the TDB DataFrame object.
    This class is used to load the TDB data into a DataFrame and perform operations on it.
    The operations are to filter and aggregate the data, exporting it to other formats.

    The class is initialized with a SparkSession object and provides a set of methods to load data from the TDB.

    The inherent DataFrame object is a Spark DataFrame and can be used as such, accessible through the `df` attribute.
    """

    def __init__(self, spark: Union[SparkSession, None] = None) -> None:
        if spark is None:
            logger.warning("No SparkSession provided. Creating a default one.")
            spark = dsa_tdb.utils.spark_session_factory()
        self._spark = spark
        self._df = None
        self._initiated = False
        self._configuration = None
        self._files_loaded = []
        self._load_args = None
        # TODO move these to a separate class inheriting from this one
        self._is_filtered = False
        self._filter_args = None
        self._is_aggregated = False
        self._aggregate_args = None

        super().__init__()

    def loadParquet(self, path: Union[str, List[str]]):
        """Load a parquet file into the DataFrame.

        This method loads a parquet file into the DataFrame.

        Parameters
        ----------
        path : str, List[str]
            The path to the parquet file.
            Can also be a pattern to load multiple files or a list of paths.
        """
        if isinstance(path, list):
            self._df = self._spark.read.parquet(*path)
        elif isinstance(path, str):
            self._df = self._spark.read.parquet(path)
        else:
            raise TypeError("Invalid path type. Must be a string or a list of strings.")
        self._initiated = True

    def loadData(
        self,
        root_folder: str,
        platform: str,
        version: T.TDB_dailyDumpsVersion = T.TDB_dailyDumpsVersion.full,
        platforms_to_exclude: Union[List[str], None] = None,
        start_date: Union[str, None] = None,
        end_date: Union[str, None] = None,
        columns_to_import: Union[List[str], None] = None,
        explode_columns: bool = False,
        delete_original: bool = True,
        fillna_str: Union[str, None] = None,
        fillna_bool: Union[bool, None] = False,
        input_format: T.TDB_chunkFormat = T.TDB_chunkFormat.parquet,
        content_date_range: Union[List[str], List[datetime], None] = None,
        decision_date_range: Union[List[str], List[datetime], None] = None,
        created_at_date_range: Union[List[str], List[datetime], None] = None,
        override_chunked_subfolder: str = T.CHUNKED_FILES_SUBFOLDER_NAME,
        compute_restriction_duration: bool = False,
        normalize_platform_name: bool = True,
        normalize_content_type_other: bool = False,
    ):
        """Load data from the TDB into a Spark DataFrame.

        This method loads the data from the TDB daily dumps into a Spark DataFrame.
        The data is loaded from the files in the specified root folder, for the specified platform and version.
        The data is filtered and transformed according to the specified options.

        Parameters
        ----------
        root_folder : str
            The root folder where the daily dumps for each platform and version are stored.
        platform : str
            The platform to load the data from.
        version : T.TDB_dailyDumpsVersion, optional
            The version of the daily dumps to load, by default :attr:`dsa_tdb.types.TDB_dailyDumpsVersion.full`.
        platforms_to_exclude : Union[List[str],None], optional
            A list of platforms to exclude from the data, by default None.
        start_date : Union[str,None], optional
            The start date to load the data from, by default None.
        end_date : Union[str,None], optional
            The end date to load the data to, by default None.
        columns_to_import : Union[List[str],None], optional
            The list of columns to import from the daily dumps, by default None.
        explode_columns : bool, optional
            Whether to horizontally explode the columns with nested structures, by default False.
        delete_original : bool, optional
            Whether to delete the original files after horizontally exploding them, by default True.
        fillna_str : Union[str,None], optional
            The value to fill the missing string values with, by default None.
        fillna_bool : Union[bool,None], optional
            The value to fill the missing boolean values with, by default False.
        input_format : T.TDB_chunkFormat, optional
            The format of the daily dump files, by default T.TDB_chunkFormat.parquet.
        content_date_range : Union[List[str],List[datetime],None], optional
            The date range to filter the content_date column, by default None.
        decision_date_range : Union[List[str],List[datetime],None], optional
            The date range to filter the decision_date column, by default None.
        created_at_date_range : Union[List[str],List[datetime],None], optional
            The date range to filter the created_at column, by default None.
        override_chunked_subfolder : str, optional
            The subfolder where the chunked files are stored, by default :attr:`dsa_tdb.types.CHUNKED_FILES_SUBFOLDER_NAME`.
            Do not change this unless you know what you are doing.
        compute_restriction_duration : bool, optional
            Whether to compute the restriction duration from the restriction_start and restriction_end columns, by default False.
        normalize_platform_name : bool, optional
            Whether to normalize the platform name to lowercase, by default True.
        normalize_content_type_other : bool, optional
            Whether to normalize the content_type_other column to lowercase, by default False.

        Raises
        ------
        ValueError
            If the content_date_range, decision_date_range or created_at_date_range have more than two elements.
        """

        if self._initiated and self._df:
            logger.warning(
                f"Calling load data on a dataframe already initiated with options {self._load_args}, will reset data."
            )

        start_date_fix = dsa_tdb.utils.compute_start_date(start_date)
        end_date_fix = dsa_tdb.utils.compute_end_date(end_date)
        try:
            files_to_load = get_files_to_load(
                root_folder=root_folder,
                platform=platform,
                version=version,
                start_date=start_date_fix,
                end_date=end_date_fix,  # type: ignore
                input_format=input_format,
                local_chunked_subfolder=override_chunked_subfolder,
            )
        except FileNotFoundError:
            logger.warning(
                f"Daily dumps not found for platform {platform} version {version} in folder {root_folder}, downloading them."
            )
            if platforms_to_exclude:
                logger.warning(f"\t Also excluding platforms {platforms_to_exclude}")
            dsa_tdb.fetch.prepare_daily_dumps(
                dump_files_root_folder=root_folder,
                platform=platform,
                platforms_to_exclude=platforms_to_exclude,
                version=version,
                do_chunking=True,
                chunk_format=T.TDB_chunkFormat.parquet,
                delete_original=True,
                from_date=start_date_fix.strftime("%Y-%m-%d"),  # type: ignore
                to_date=end_date_fix.strftime("%Y-%m-%d"),  # type: ignore
                n_processes=min(12, psutil.cpu_count()),
                override_chunked_subfolder=override_chunked_subfolder,
            )
            logger.info("Download complete. Loading data.")
            files_to_load = get_files_to_load(
                root_folder=root_folder,
                platform=platform,
                version=version,
                start_date=start_date_fix,
                end_date=end_date_fix,  # type: ignore
                input_format=input_format,
                local_chunked_subfolder=override_chunked_subfolder,
            )

        if columns_to_import is None:
            logger.info(f"Loading all columns for {platform} {version} from {start_date} to {end_date}")
            if version == T.TDB_dailyDumpsVersion.full:
                columns_to_import = T.TDB_columnsFull._member_names_
            elif version == T.TDB_dailyDumpsVersion.light:
                columns_to_import = T.TDB_columnsLight._member_names_
            else:
                # Should never happen
                raise ValueError(f"Invalid version {version}")
        if content_date_range is not None:
            if len(content_date_range) != 2:
                raise ValueError("content_date_range must have exactly two elements.")
            elif isinstance(content_date_range[0], str):
                content_date_range = [datetime.strptime(d, "%Y-%m-%d") for d in content_date_range]  # type: ignore
        if decision_date_range is not None:
            if len(decision_date_range) != 2:
                raise ValueError("decision_date_range must have exactly two elements.")
            elif isinstance(decision_date_range[0], str):
                decision_date_range = [datetime.strptime(d, "%Y-%m-%d") for d in decision_date_range]  # type: ignore
        if created_at_date_range is not None:
            if len(created_at_date_range) != 2:
                raise ValueError("created_at_date_range must have exactly two elements.")
            elif isinstance(created_at_date_range[0], str):
                created_at_date_range = [datetime.strptime(d, "%Y-%m-%d") for d in created_at_date_range]  # type: ignore

        dt_col_to_import = [
            T.TDB_datetimeColumns[c] for c in columns_to_import if c in T.TDB_datetimeColumns._member_names_
        ]
        other_cols_to_import = [
            T.TDB_columnsFull[c] for c in columns_to_import if c not in T.TDB_datetimeColumns._member_names_
        ]

        columns_to_fill_bool = [
            T.RawAndExplodedColumn[c] for k, v in T.columns_to_explode.items() for c in v if k in columns_to_import
        ]

        _args = T.LoadFileArguments(
            dump_files_pattern=files_to_load,
            columns_to_import=other_cols_to_import,
            columns_datetime=dt_col_to_import,
            input_format=input_format,
            del_original=delete_original,
            explode_cols=explode_columns,
            created_at_date_range=created_at_date_range,  # type: ignore
            content_date_range=content_date_range,  # type: ignore
            decision_date_range=decision_date_range,  # type: ignore
            columns_to_fill_str=[],
            columns_to_fill_bool=columns_to_fill_bool,
            fillna_str=fillna_str,
            fillna_bool=fillna_bool,
            compute_restriction_duration=compute_restriction_duration,
            compute_time_to_action=False,
            normalize_platform_name=normalize_platform_name,
            normalize_content_type_other=normalize_content_type_other,
        )

        self._df = dsa_tdb.etl.loadFile(
            dump_files_pattern=_args.dump_files_pattern,
            spark=self._spark,
            columns_to_import=_args.columns_to_import,
            columns_datetime=_args.columns_datetime,
            input_format=_args.input_format,
            del_original=_args.del_original,
            explode_cols=_args.explode_cols,
            created_at_date_range=_args.created_at_date_range,
            content_date_range=_args.content_date_range,
            decision_date_range=_args.decision_date_range,
            columns_to_fill_str=_args.columns_to_fill_str,  # type: ignore
            columns_to_fill_bool=_args.columns_to_fill_bool,  # type: ignore
            fillna_str=_args.fillna_str,
            fillna_bool=_args.fillna_bool,
            compute_restriction_duration=_args.compute_restriction_duration,
            compute_time_to_action=_args.compute_time_to_action,
            normalize_platform_name=_args.normalize_platform_name,
            normalize_content_type_other=_args.normalize_content_type_other,
        )

        if platforms_to_exclude:
            self._df = self._df.filter(~self._df.platform.isin(platforms_to_exclude))

        self._files_loaded = files_to_load
        self._load_args = _args
        self._initiated = True

    def filter_SoRs(
        self,
        columns_to_import: Union[List[T.TDB_columnsFull], None] = None,
        horizontally_explode_columns: bool = True,
        delete_original_columns: bool = False,
        normalize_platform_name: bool = False,
        platforms_to_exclude: Union[List[str], None] = None,
        platforms_to_include: Union[List[str], None] = None,
        created_at_dt_floor: Union[str, None] = None,
        config_file: Union[str, None] = None,
        **kwargs,
    ):
        """Filters the SoRs from the dataframe.
        The configuration can be passed either using the provided and additional keyword arguments
        or by providing a configuration file in `config_file`.
        Note that if both are provided, the keyword arguments will take precedence.

        Parameters
        ----------
        columns_to_import : Union[List[T.TDB_columnsFull],None], optional
            The columns to import from the dataframe, by default None.
        horizontally_explode_columns : bool, optional
            Whether to horizontally explode the columns with nested structures, by default True.
        delete_original_columns : bool, optional
            Whether to delete the original columns after horizontally exploding them, by default False.
        normalize_platform_name : bool, optional
            Whether to normalize the platform name to lowercase, by default False.
        platforms_to_exclude : Union[List[str],None], optional
            The platforms to exclude from the data, by default None.
        platforms_to_include : Union[List[str],None], optional
            The platforms to include in the data, by default None.
        created_at_dt_floor : Union[str,None], optional
            The floor to round the created_at datetime to, by default None.
        config_file : Union[str,None], optional
            The path to a configuration file, by default None.
        **kwargs : dict
            The filter arguments. these are all the remaining entries of :attr:`dsa_tdb.types.FilteringConfig`
            that are not directly exposed in the function arguments.
        """
        if not self._initiated:
            raise RuntimeError("No data loaded. Please call loadData() first.")
        elif self._df is None:
            # Should never happen
            raise RuntimeError("No data loaded. Please call loadData() first.")
        if self._is_filtered:
            logger.warning("Data already filtered, will perform the operation again.")
        if self._is_aggregated:
            logger.warning("Dataframe is aggregated, will perform the operation but check the columns!")

        tmp_config = {}
        if config_file:
            with open(config_file) as f:
                tmp_config.update(yaml.safe_load(f))

        if columns_to_import is not None:
            tmp_config["columns_to_import"] = columns_to_import
        if horizontally_explode_columns is not None:
            tmp_config["horizontally_explode_columns"] = horizontally_explode_columns
        if delete_original_columns is not None:
            tmp_config["delete_original_columns"] = delete_original_columns
        if normalize_platform_name is not None:
            tmp_config["normalize_platform_name"] = normalize_platform_name
        if platforms_to_exclude is not None:
            tmp_config["platforms_to_exclude"] = platforms_to_exclude
        if platforms_to_include is not None:
            tmp_config["platforms_to_include"] = platforms_to_include
        if created_at_dt_floor is not None:
            tmp_config["created_at_dt_floor"] = created_at_dt_floor

        tmp_config.update(kwargs)

        config = dsa_tdb.types.FilteringConfig(**tmp_config)

        self._df = dsa_tdb.etl._filter_sors(df=self._df, spark=self._spark, config=config)

    def aggregate_SoRs(
        self,
        columns_to_group: Union[List[T.RawAndExplodedColumn], None] = None,
        horizontally_explode_columns: Union[bool, None] = None,
        delete_original_columns: Union[bool, None] = None,
        normalize_platform_name: Union[bool, None] = None,
        platforms_to_exclude: Union[List[str], None] = None,
        platforms_to_include: Union[List[str], None] = None,
        created_at_dt_floor: Union[str, None] = None,
        config_file: Union[str, None] = None,
        **kwargs,
    ):
        """Aggregates the SoRs from the dataframe.
        The configuration can be passed either using the provided and additional keyword arguments
        or by providing a configuration file in `config_file`.
        Note that if both are provided, the keyword arguments will take precedence.

        Parameters
        ----------
        columns_to_group: Union[List[T.RawAndExplodedColumn],None], optional
            The columns to group the data by, by default None will use all except `uuid` and `platform_uid`.
        horizontally_explode_columns : bool, optional
            Whether to horizontally explode the columns with nested structures, by default True.
        delete_original_columns : bool, optional
            Whether to delete the original columns after horizontally exploding them, by default False.
        normalize_platform_name : bool, optional
            Whether to normalize the platform name to lowercase, by default False.
        platforms_to_exclude : Union[List[str],None], optional
            The platforms to exclude from the data, by default None.
        platforms_to_include : Union[List[str],None], optional
            The platforms to include in the data, by default None.
        created_at_dt_floor : Union[str,None], optional
            The floor to round the created_at datetime to, by default None.
        config_file : Union[str,None], optional
            The path to a configuration file, by default None.
        **kwargs : dict
            The aggregation arguments. these are all the remaining entries of :attr:`dsa_tdb.types.AggregationConfig`
            that are not directly exposed in the function arguments.
        """
        if not self._initiated:
            raise RuntimeError("No data loaded. Please call loadData() first.")
        elif self._df is None:
            # Should never happen
            raise RuntimeError("No data loaded. Please call loadData() first.")
        if self._is_aggregated:
            logger.warning("Data already aggregated, will perform the operation again.")
        if self._is_filtered:
            logger.info("Dataframe is filtered, will perform the aggregation!")

        tmp_config = {}
        if config_file:
            with open(config_file) as f:
                tmp_config.update(yaml.safe_load(f))

        # Overwrite the config with the provided arguments only if they are not None
        if columns_to_group is not None:
            tmp_config["columns_to_group"] = columns_to_group
        if horizontally_explode_columns is not None:
            tmp_config["horizontally_explode_columns"] = horizontally_explode_columns
        if delete_original_columns is not None:
            tmp_config["delete_original_columns"] = delete_original_columns
        if normalize_platform_name is not None:
            tmp_config["normalize_platform_name"] = normalize_platform_name
        if platforms_to_exclude is not None:
            tmp_config["platforms_to_exclude"] = platforms_to_exclude
        if platforms_to_include is not None:
            tmp_config["platforms_to_include"] = platforms_to_include
        if created_at_dt_floor is not None:
            tmp_config["created_at_dt_floor"] = created_at_dt_floor

        tmp_config.update(kwargs)

        config = dsa_tdb.types.AggregationConfig(**tmp_config)

        self._df = dsa_tdb.etl._aggregate_sors(df=self._df, spark=self._spark, config=config)

    def schema(self):
        if self._df is None:
            logger.warning("No data loaded. Please call loadData() first.")
        else:
            return self._df.schema

    def head(self, n: int = 1):
        if self._df is None:
            logger.warning("No data loaded. Please call loadData() first.")
        else:
            return self._df.head(n)

    def show(self, n: int = 20):
        if self._df is None:
            logger.warning("No data loaded. Please call loadData() first.")
        else:
            self._df.show(n)

    def sample(self, n: int = 1):
        if self._df is None:
            logger.warning("No data loaded. Please call loadData() first.")
        else:
            return self._df.sample(n)

    def toPandas(self) -> pd.DataFrame:
        if self._df is None:
            raise ValueError("No data loaded when calling toPandas(). Please call loadData() first.")
        else:
            return self._df.toPandas()

    def text_filter(self, column: str, expr: str):
        """Filter the DataFrame using a text expression.

        Parameters
        ----------
        column : str
            The column to filter on.
        expr : str
            The expression to filter with.
            Can also be a regular expression.
        """
        if self._df is None:
            logger.error("No data loaded. Please call loadData() first.")
            raise ValueError("No data loaded. Please call loadData() first.")
        else:
            self._df = self._df.filter(F.rlike(F.col(column), F.lit(expr)))

    @property
    def columns(self):
        if self._df is None:
            logger.warning("No data loaded. Please call loadData() first.")
        else:
            return self._df.columns

    def _sideload(
        self,
        df: DataFrame,
        is_filtered: bool = False,
        load_args: Union[T.LoadFileArguments, None] = None,
        filter_args: Union[T.FilteringConfig, None] = None,
        is_aggregated: bool = False,
        aggregate_args: Union[T.AggregationConfig, None] = None,
    ):
        self._df = df
        self._initiated = True
        self._load_args = load_args
        self._is_filtered = is_filtered
        self._filter_args = filter_args
        self._is_aggregated = is_aggregated
        self._aggregate_args = aggregate_args
