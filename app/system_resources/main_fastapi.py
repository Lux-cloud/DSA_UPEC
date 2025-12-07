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
from enum import Enum
from typing import Union

try:
    from fastapi import FastAPI, Query
    from fastapi.responses import PlainTextResponse, RedirectResponse
except ImportError:
    raise ImportError(
        "Please install the [webapp] extra by running `pip install dsa-tdb[webapp]` or `poetry add dsa-tdb[webapp] to use the webapp."
    )

import dsa_tdb
from app.tasks import aggregate as aggregate_task
from app.tasks import filtering as filtering_task
from app.tasks import prepare as prepare_task
from dsa_tdb import types as T

logger = logging.getLogger(dsa_tdb.__name__)


# Get the platform names and versions
# PlatformNames = unique(Enum('PlatformsNames',
#                             OrderedDict([(p, p)
#                                          for p in dsa_tdb.fetch.fetch_available_platforms().keys()])))
class PlatformNamesWrapper(str, Enum):
    """The platform names."""

    def __str__(self) -> str:
        return self.value


PlatformNames = PlatformNamesWrapper(
    "PlatformNames", [(p, p) for p in dsa_tdb.fetch.fetch_available_platforms().keys()]
)


description = """
The `dsa_tdb` API helps you interacting with the DSA Transparency Database data. 🚀

## Prepare

You can **prepare** the daily dumps for filtering/aggregation.
This will download them to your local machine and chunk in a parquet or csv format.

## Filter

Use this endpoint to **filter** the aggregated data.
Provide the path to:
- The root folder containing the daily dumps.
- A valid configuration file.
- The output file name.

Specify the start and end date for the filtering, the platform name and version of the daily dumps.

## Aggregate

Use this endpoint to **aggregate** the daily dumps.
Provide the path to:
- The root folder containing the daily dumps (or the filtered data) .
- A valid configuration file.
- The output file name.

Specify the start and end date for the aggregation, the platform name and version of the daily dumps.

## Log

Use this endpoint to **read the log** of a task by providing the task_id.
You can find the `task_id` by looking at the [celery interface](http://localhost:5555) (change the `5555` port to your custom one if different).
You can also follow the status of the aggregation and filtering tasks on the [Spark dashboard](http://localhost:4040) (change the `4040` port to your custom one if different).

Provide the number of lines you want to read from the log file (if `n_lines` is <=0, it will read all the file).

"""

tags_metadata = [
    {
        "name": "Prepare",
        "description": """This is the **Prepare** endpoint.
            Use it to download the daily dumps and chunk them in a parquet or csv format.
            You can specify where to save the daily dumps, the platform to download (use `global` to download all of them),
            the version of the daily dumps, the start and end date for the preparation,
            the platforms to exclude (a comma separated list of platform names), etc. See the
            single value documentation for details.""",
    },
    {
        "name": "Filter",
        "description": """This is the **Filter** endpoint.
            Use it to filter the daily dumps and save the filtered data in a separate folder to speed up the aggregation or later analyses.
            You can specify the root folder containing the daily dumps, the platform name, the version of the daily dumps,
            the configuration file, the output file name, the start and end date for the filtering, etc. See the
            single value documentation for details.
            We offer two endpoints to filter the data:

            - The `filter` endpoint to filter the data using a configuration file.
            - The `filter_manual` endpoint to filter the data by manually specifying all the needed fields in the query parameters.
            """,
    },
    {
        "name": "Aggregate",
        "description": """This is the **Aggregate** endpoint.
            Use it to aggregate the daily dumps and save the aggregated data in a separate file.
            You can specify the root folder containing the daily dumps, the platform name, the version of the daily dumps,
            the configuration file, the output file name, the start and end date for the aggregation, etc. See the
            single value documentation for details.
            We offer two endpoints to aggregate the data:

            - The `aggregate` endpoint to aggregate the data using a configuration file.
            - The `aggregate_manual` endpoint to aggregate the data by manually specifying all the needed fields in the query parameters.
            """,
    },
    {
        "name": "Log",
        "description": "Test doc string for log",
    },
]

# Create a new FastAPI instance
app = FastAPI(
    title="DSA Transparency DB API",
    summary="The API interface to use the `dsa_tdb` util.",
    version=dsa_tdb.__version__,
    terms_of_service="https://transparency.dsa.ec.europa.eu/",
    contact={
        "name": "CNECT DSA Team",
        "url": "https://transparency.dsa.ec.europa.eu/",
        # "email": "TBD",
    },
    description=description,
    license_info={"name": "EUPL", "url": "https://eupl.eu/1.2/en/"},
    openapi_tags=tags_metadata,
)


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url="/docs")


@app.post("/prepare/", tags=["Prepare"])
async def prepare(
    output_dir: str = Query(
        "/data/tdb_data",
        description="The root output directory to store the data. The files will be saved in the `plaform___version` subfolders.",
    ),
    platform_name: PlatformNames = Query(PlatformNames["global"], description="The platform name."),
    version: T.TDB_dailyDumpsVersion = Query(
        T.TDB_dailyDumpsVersion.full, description="The version of the daily dumps (full or light)."
    ),
    start_date: str = Query("", description="The start date for the preparation (format YYYY-MM-DD)."),
    end_date: str = Query("", description="The end date for the preparation (format YYYY-MM-DD)."),
    platforms_to_exclude: list[PlatformNames] = Query(
        [],
        description="The platforms to exclude from the preparation. In the UI, keep CTRL pressed to select multiple platforms.",
    ),
    do_chunking: bool = Query(True, description="Whether to chunk the downloaded raw data."),
    chunk_format: T.TDB_chunkFormat = Query(
        T.TDB_chunkFormat.parquet, description="The format to use for the chunking (parquet or csv)."
    ),
    delete_original: bool = Query(True, description="Whether to delete the original files after their chunking."),
    check_sha1: bool = Query(True, description="Whether to check the SHA1 checksum of the downloaded files."),
    force_sha1: bool = Query(
        False, description="Whether to force the SHA1 checksum for the downloaded files already there."
    ),
    override_chunked_subfolder: str = Query(
        T.CHUNKED_FILES_SUBFOLDER_NAME,
        description="The subfolder name to override the default one. **Do not change it unless you know what you are doing**.",
    ),
    n_processes: int = Query(
        -1,
        description="The number of processes to use for the preparation. If <=0, it will use all the available cores.",
    ),
):
    """Prepare the daily dumps.
    Provide the output directory to store the prepared data.
    Specify the start and end date for the preparation (format YYYY-MM-DD), the platform name and version of the daily dumps.

    The prepared data will be chunked in a parquet or csv format in the output directory following the structure:
    `output_dir/{platform_name}___{version}/daily_dumps_chunked/`

    The checksum files will be stored in the parent daily chunked folder (do not delete them).
    """
    prepare_task.delay(
        platform_name=platform_name,
        version=version,
        start_date=start_date,
        end_date=end_date,
        platforms_to_exclude=platforms_to_exclude,
        output_dir=output_dir,
        delete_original=delete_original,
        do_chunking=do_chunking,
        check_sha1=check_sha1,
        force_sha1=force_sha1,
        chunk_format=chunk_format,
        n_processes=n_processes,
        override_chunked_subfolder=override_chunked_subfolder,
    )  # type: ignore

    return {
        "platform_name": platform_name,
        "version": version,
        "start_date": start_date,
        "end_date": end_date,
        "output_dir": output_dir,
    }


@app.post("/filter/", tags=["Filter"])
async def filter(
    root_folder: str = Query(
        "/data/tdb_data",
        description="The root folder containing the daily dumps folders in the format `platform___version`.",
    ),
    platform_name: PlatformNames = Query(PlatformNames["global"].name, description="The platform name."),
    version: T.TDB_dailyDumpsVersion = Query(
        T.TDB_dailyDumpsVersion.full, description="The version of the daily dumps (full or light)."
    ),
    config_file: str = Query("/data/config_filtering.yaml", description="The path to the configuration file."),
    out_file_name: str = Query("/data/my_filtered_data", description="The output file name."),
    start_date: str = Query("", description="The start date for the filtering (format YYYY-MM-DD)."),
    end_date: str = Query("", description="The end date for the filtering (format YYYY-MM-DD)."),
    override_chunked_subfolder: str = Query(
        T.CHUNKED_FILES_SUBFOLDER_NAME,
        description="The subfolder name to override the default one. **Do not change it unless you know what you are doing**.",
    ),
    n_processes: int = Query(
        -1, description="The number of processes to use for the filtering. If <=0, it will use all the available cores."
    ),
    memory_limit: Union[str, None] = Query(
        None, description="The memory limit to use for the filtering. Use the format '10g' for 10GB."
    ),
    spark_local_dir: Union[str, None] = Query(
        None, description="The local directory to use for the Spark temporary files."
    ),
):
    """Filter the daily dumps.
    Provide the path to:
    - The root folder containing the daily dumps (or the filtered data) .
    - A valid configuration file.
    - The output file name.

    Specify the start and end date for the filtering (format YYYY-MM-DD), the platform name and version of the daily dumps.
    """

    filtering_task.delay(
        root_folder=root_folder,
        platform_name=platform_name,
        version=version,
        config=config_file,
        out_file_name=out_file_name,
        start_date=start_date,
        end_date=end_date,
        override_chunked_subfolder=override_chunked_subfolder,
        n_processes=n_processes,
        memory_limit=memory_limit,
        spark_local_dir=spark_local_dir,
    )  # type: ignore

    return {
        "platform_name": platform_name,
        "version": version,
        "start_date": start_date,
        "end_date": end_date,
        "n_processes": n_processes,
    }


# TODO: See if we can retrieve the description from the pydantic model
@app.post("/filter_manual/", tags=["Filter"])
async def filter_manual(
    root_folder: str = Query(
        "/data/tdb_data",
        description="The root folder containing the daily dumps folders in the format `platform___version`.",
    ),
    input_format: T.InputFileFormat = Query(
        T.InputFileFormat.parquet, description="The input format of the daily dumps."
    ),
    output_format: T.AggregateFileFormat = Query(T.AggregateFileFormat.parquet, description="The output file format."),
    platform_name: PlatformNames = Query(PlatformNames["global"].name, description="The platform name."),
    version: T.TDB_dailyDumpsVersion = Query(
        T.TDB_dailyDumpsVersion.full, description="The version of the daily dumps (full or light)."
    ),
    override_chunked_subfolder: str = Query(
        T.CHUNKED_FILES_SUBFOLDER_NAME,
        description="The subfolder name to override the default one. **Do not change it unless you know what you are doing**.",
    ),
    out_file_name: str = Query("/data/my_aggregation", description="The output file name."),
    # Basic filters
    start_date: Union[str, None] = Query("", description="The start date for the filtering (format YYYY-MM-DD)."),
    end_date: Union[str, None] = Query("", description="The end date for the filtering (format YYYY-MM-DD)."),
    write_mode: T.AggregateWriteMode = Query(
        T.AggregateWriteMode.overwrite, description="The write mode to use for the output file."
    ),
    delete_original_columns: bool = Query(
        False, description="Delete the original columns when horizontally exploding."
    ),
    horizontally_explode_columns: bool = Query(False, description="Horizontally explode the columns."),
    normalize_platform_name: bool = Query(
        False, description="Whether to coalesce platform names when it changed over time."
    ),
    normalize_content_type_other: bool = Query(False, description="Whether to normalize the content type other."),
    fillna_str_value: Union[str, None] = Query(None, description="Value to use for filling NA values."),
    fillna_bool_value: Union[bool, None] = Query(False, description="Value to use for filling NA values."),
    content_date_range: Union[str, None] = Query(
        None,
        description='Content date range to use for filtering the input files, it is a string in the format "YYYY-MM-DD,YYYY-MM-DD".',
    ),
    decision_date_range: Union[str, None] = Query(
        None,
        description='Decision date range to use for filtering the input files, it is a string in the format "YYYY-MM-DD,YYYY-MM-DD".',
    ),
    created_at_date_range: Union[str, None] = Query(
        None,
        description='Created at date range to use for filtering the input files, it is a string in the format "YYYY-MM-DD,YYYY-MM-DD".',
    ),
    platforms_to_exclude: list[PlatformNames] = Query([], description="Platforms to exclude."),
    platforms_to_include: list[PlatformNames] = Query([], description="Platforms to exclude."),
    created_at_dt_floor: str = Query(
        "day",
        description="The argument to pass to pyspark sql date_trunc when flooring the creation date. By default `day`",
    ),
    columns_to_import: Union[list[T.TDB_columnsFull], None] = Query(
        None, description="Columns to import from the input files or df **before** exploding."
    ),
    upstream_sampling: Union[float, None] = Query(
        None,
        description="The fraction (if 0 < x < 1) or the number of rows (if x >= 1, will be rounded) to sample from the input files **BEFORE** filtering. Leave to null or <=0 to keep all the rows.",
    ),
    downstream_sampling: Union[float, None] = Query(
        None,
        description="The fraction (if 0 < x < 1) or the number of rows (if x >= 1, will be rounded) to sample from the input files **AFTER** filtering. Leave to null or <=0 to keep all the rows.",
    ),
    # Boolean columns to check
    bool_columns_to_check: list[T.RawAndExplodedColumn] = Query([], description="Columns to check for boolean values."),
    bool_columns_to_check_operator: T.BooleanOperator = Query(
        T.BooleanOperator.OR, description="The operator to use when checking the boolean columns."
    ),
    # Single value columns to filter
    decision_monetary: list[T.DecisionMonetary] = Query([], description="The decision monetary to filter."),
    decision_provision: list[T.DecisionProvision] = Query([], description="The decision provision to filter."),
    decision_account: list[T.DecisionAccount] = Query([], description="The decision account to filter."),
    decision_visibility: list[T.DecisionVisibility] = Query([], description="The decision visibility to filter."),
    category: list[T.Category] = Query([], description="The list of category(ies) to filter."),
    decision_ground: list[T.DecisionGround] = Query([], description="The decision ground to filter."),
    automated_detection: list[T.AutomatedDetection] = Query([], description="The automated detection to filter."),
    automated_decision: list[T.AutomatedDecision] = Query([], description="The automated decision to filter."),
    source_type: list[T.SourceType] = Query([], description="The source type to filter."),
    account_type: list[T.AccountType] = Query([], description="The account type to filter."),
    incompatible_content_illegal: list[T.IncompatibleContentIllegal] = Query(
        [], description="The incompatible content illegal to filter."
    ),
    content_language: list[T.ContentLanguage] = Query([], description="The content language to filter."),
    content_type: list[T.ContentType] = Query([], description="The content type to filter."),
    category_addition: list[T.Category] = Query([], description="The category addition to filter."),
    category_specification: list[T.Keyword] = Query([], description="The category specification to filter."),
    territorial_scope: list[T.TerritorialScope] = Query([], description="The territorial scope to filter."),
    # Multi value columns to filter
    decision_visibility_other: Union[None, str] = Query(None, description="The decision visibility other to filter."),
    decision_visibility_other_to_lower: bool = Query(
        False, description="Whether the decision visibility other shall be cast to lower text before being analyzed."
    ),
    decision_monetary_other: Union[None, str] = Query(None, description="The decision monetary other to filter."),
    decision_monetary_other_to_lower: bool = Query(
        False, description="Whether the decision monetary other shall be cast to lower text before being analyzed."
    ),
    decision_facts: Union[None, str] = Query(None, description="The decision facts to filter."),
    decision_facts_to_lower: bool = Query(
        False, description="Whether the decision facts shall be cast to lower text before being analyzed.."
    ),
    decision_ground_reference_url: Union[None, str] = Query(
        None, description="The decision ground reference url to filter."
    ),
    decision_ground_reference_url_to_lower: bool = Query(
        False,
        description="Whether the decision ground reference url shall be cast to lower text before being analyzed..",
    ),
    illegal_content_legal_ground: Union[None, str] = Query(
        None, description="The illegal content legal ground to filter."
    ),
    illegal_content_legal_ground_to_lower: bool = Query(
        False,
        description="Whether the illegal content legal ground shall be cast to lower text before being analyzed..",
    ),
    illegal_content_explanation: Union[None, str] = Query(
        None, description="The illegal content explanation to filter."
    ),
    illegal_content_explanation_to_lower: bool = Query(
        False, description="Whether the illegal content explanation shall be cast to lower text before being analyzed.."
    ),
    incompatible_content_ground: Union[None, str] = Query(
        None, description="The incompatible content ground to filter."
    ),
    incompatible_content_ground_to_lower: bool = Query(
        False, description="Whether the incompatible content ground shall be cast to lower text before being analyzed.."
    ),
    incompatible_content_explanation: Union[None, str] = Query(
        None, description="The incompatible content explanation to filter."
    ),
    incompatible_content_explanation_to_lower: bool = Query(
        False,
        description="Whether the incompatible content explanation shall be cast to lower text before being analyzed..",
    ),
    content_type_other: Union[None, str] = Query(None, description="The content type other to filter."),
    content_type_other_to_lower: bool = Query(
        False, description="Whether the content type other shall be cast to lower text before being analyzed.."
    ),
    category_specification_other: Union[None, str] = Query(
        None, description="The category specification other to filter."
    ),
    category_specification_other_to_lower: bool = Query(
        False,
        description="Whether the category specification other shall be cast to lower text before being analyzed..",
    ),
    source_identity: Union[None, str] = Query(None, description="The source identity to filter."),
    source_identity_to_lower: bool = Query(
        False, description="Whether the source identity shall be cast to lower text before being analyzed.."
    ),
    # Restriction en dates: not implemented yet
    end_date_account_restriction: None = Query(
        None, description="The end date account restriction to filter, NOT IMPLEMENTED YET."
    ),
    end_date_monetary_restriction: None = Query(
        None, description="The end date monetary restriction to filter, NOT IMPLEMENTED YET."
    ),
    end_date_service_restriction: None = Query(
        None, description="The end date service restriction to filter, NOT IMPLEMENTED YET."
    ),
    end_date_visibility_restriction: None = Query(
        None, description="The end date visibility restriction to filter, NOT IMPLEMENTED YET."
    ),
    # Misc options
    n_processes: int = Query(
        -1, description="The number of processes to use for the filtering. If <=0, it will use all the available cores."
    ),
    memory_limit: Union[str, None] = Query(
        None, description="The memory limit to use for the filtering. Use the format '10g' for 10GB."
    ),
    spark_local_dir: Union[str, None] = Query(
        None, description="The local directory to use for the Spark temporary files."
    ),
):
    """Filter the daily dumps with a manual configuration.
    Specify all the fields needed for the filtering in the query parameters.
    The `content_date_range` and the other date ranges should be in the format "YYYY-MM-DD,YYYY-MM-DD",
    with the start date on the left and the end date on the right, separated by a comma (both the extremes will be included in the filtering).
    """

    # Parse the date ranges
    if content_date_range:
        content_date_range_fix = content_date_range.split(",")
        content_date_range_fix = (
            datetime.strptime(content_date_range_fix[0], "%Y-%m-%d"),
            datetime.strptime(content_date_range_fix[1], "%Y-%m-%d"),
        )
    else:
        content_date_range_fix = None

    if decision_date_range:
        decision_date_range_fix = decision_date_range.split(",")
        decision_date_range_fix = (
            datetime.strptime(decision_date_range_fix[0], "%Y-%m-%d"),
            datetime.strptime(decision_date_range_fix[1], "%Y-%m-%d"),
        )
    else:
        decision_date_range_fix = None

    if created_at_date_range:
        created_at_date_range_fix = created_at_date_range.split(",")
        created_at_date_range_fix = (
            datetime.strptime(created_at_date_range_fix[0], "%Y-%m-%d"),
            datetime.strptime(created_at_date_range_fix[1], "%Y-%m-%d"),
        )
    else:
        created_at_date_range_fix = None

    # Divide the columns to import in datetime and others
    msg_out = ""
    if columns_to_import is None or len(columns_to_import) == 0:
        tmp_msg = "No columns to import specified, using all the columns."
        logger.warning(tmp_msg)
        msg_out += tmp_msg + "\n"
        if version == T.TDB_dailyDumpsVersion.full:
            columns_to_import_all = list(T.TDB_columnsFull._member_names_)
        elif version == T.TDB_dailyDumpsVersion.light:
            columns_to_import_all = list(T.TDB_columnsLight._member_names_)
        else:
            # Should never happen
            raise ValueError(f"Unknown version {version}")
    else:
        columns_to_import_all = columns_to_import
    columns_to_import_fix = [T.RawAndExplodedColumn[c].name for c in columns_to_import_all]
    dt_cols_to_import = [c for c in columns_to_import_fix if c in T.datetime_columns]
    other_cols_to_import = [c for c in columns_to_import_fix if c not in T.datetime_columns]

    columns_to_fill_bool = [c for k, v in T.columns_to_explode.items() for c in v]
    columns_to_fill_str = []

    # Create the config class
    tmp_config = T.FilteringConfig(
        input_format=input_format,
        output_format=output_format,
        write_mode=write_mode,
        delete_original_columns=delete_original_columns,
        horizontally_explode_columns=horizontally_explode_columns,
        normalize_platform_name=normalize_platform_name,
        normalize_content_type_other=normalize_content_type_other,
        fillna_str_value=fillna_str_value,
        fillna_bool_value=fillna_bool_value,
        content_date_range=content_date_range_fix,  # type: ignore
        decision_date_range=decision_date_range_fix,  # type: ignore
        created_at_date_range=created_at_date_range_fix,  # type: ignore
        platforms_to_exclude=platforms_to_exclude,  # type: ignore
        platforms_to_include=platforms_to_include,  # type: ignore
        created_at_dt_floor=created_at_dt_floor,
        columns_to_import=other_cols_to_import,  # type: ignore
        columns_datetime=dt_cols_to_import,  # type: ignore
        upstream_sampling=upstream_sampling,
        downstream_sampling=downstream_sampling,
        bool_columns_to_check=bool_columns_to_check,
        bool_columns_to_check_operator=bool_columns_to_check_operator,
        decision_monetary=decision_monetary,
        decision_provision=decision_provision,
        decision_account=decision_account,
        decision_visibility=decision_visibility,
        category=category,
        decision_ground=decision_ground,
        automated_detection=automated_detection,
        automated_decision=automated_decision,
        source_type=source_type,
        account_type=account_type,
        incompatible_content_illegal=incompatible_content_illegal,
        content_language=content_language,
        content_type=content_type,
        category_addition=category_addition,
        category_specification=category_specification,
        territorial_scope=territorial_scope,
        decision_visibility_other=decision_visibility_other,
        decision_visibility_other_to_lower=decision_visibility_other_to_lower,
        decision_monetary_other=decision_monetary_other,
        decision_monetary_other_to_lower=decision_monetary_other_to_lower,
        decision_facts=decision_facts,
        decision_facts_to_lower=decision_facts_to_lower,
        decision_ground_reference_url=decision_ground_reference_url,
        decision_ground_reference_url_to_lower=decision_ground_reference_url_to_lower,
        illegal_content_legal_ground=illegal_content_legal_ground,
        illegal_content_legal_ground_to_lower=illegal_content_legal_ground_to_lower,
        illegal_content_explanation=illegal_content_explanation,
        illegal_content_explanation_to_lower=illegal_content_explanation_to_lower,
        incompatible_content_ground=incompatible_content_ground,
        incompatible_content_ground_to_lower=incompatible_content_ground_to_lower,
        incompatible_content_explanation=incompatible_content_explanation,
        incompatible_content_explanation_to_lower=incompatible_content_explanation_to_lower,
        content_type_other=content_type_other,
        content_type_other_to_lower=content_type_other_to_lower,
        category_specification_other=category_specification_other,
        category_specification_other_to_lower=category_specification_other_to_lower,
        source_identity=source_identity,
        source_identity_to_lower=source_identity_to_lower,
        end_date_account_restriction=end_date_account_restriction,
        end_date_monetary_restriction=end_date_monetary_restriction,
        end_date_service_restriction=end_date_service_restriction,
        end_date_visibility_restriction=end_date_visibility_restriction,
        columns_to_fill_str=columns_to_fill_str,
        columns_to_fill_bool=columns_to_fill_bool,  # type: ignore
    )

    filtering_task.delay(
        root_folder=root_folder,
        platform_name=platform_name,
        version=version,
        config=tmp_config.model_dump(),
        out_file_name=out_file_name,
        start_date=start_date,
        end_date=end_date,
        override_chunked_subfolder=override_chunked_subfolder,
        n_processes=n_processes,
        memory_limit=memory_limit,
        spark_local_dir=spark_local_dir,
    )  # type: ignore

    return {
        "platform_name": platform_name,
        "version": version,
        "start_date": start_date,
        "end_date": end_date,
        "n_processes": n_processes,
        "warnings": msg_out,
    }


@app.post("/aggregate/", tags=["Aggregate"])
async def aggregate(
    root_folder: str = Query(..., description="The root folder containing the daily dumps."),
    platform_name: PlatformNames = Query(PlatformNames["global"].name, description="The platform name."),
    version: T.TDB_dailyDumpsVersion = Query(
        T.TDB_dailyDumpsVersion.full, description="The version of the daily dumps (full or light)."
    ),
    config_file: str = Query("/data/config_aggregation.yaml", description="The path to the configuration file."),
    out_file_name: str = Query("/data/my_aggregation", description="The output file name."),
    start_date: str = Query("", description="The start date for the aggregation (format YYYY-MM-DD)."),
    end_date: str = Query("", description="The end date for the aggregation (format YYYY-MM-DD)."),
    override_chunked_subfolder: str = Query(
        T.CHUNKED_FILES_SUBFOLDER_NAME,
        description="The subfolder name to override the default one. **Do not change it unless you know what you are doing**.",
    ),
    n_processes: int = Query(
        -1,
        description="The number of processes to use for the aggregation. If <=0, it will use all the available cores.",
    ),
    memory_limit: Union[str, None] = Query(
        None, description="The memory limit to use for the aggregation. Use the format '10g' for 10GB."
    ),
    spark_local_dir: Union[str, None] = Query(
        None, description="The local directory to use for the Spark temporary files."
    ),
):
    """Aggregate the daily dumps.
    Provide the path to:
    - The root folder containing the daily dumps (or the filtered data) .
    - A valid configuration file.
    - The output file name.

    Specify the start and end date for the aggregation (format YYYY-MM-DD), the platform name and version of the daily dumps.
    """

    aggregate_task.delay(
        root_folder=root_folder,
        platform_name=platform_name,
        version=version,
        config=config_file,
        out_file_name=out_file_name,
        start_date=start_date,
        end_date=end_date,
        override_chunked_subfolder=override_chunked_subfolder,
        n_processes=n_processes,
        memory_limit=memory_limit,
        spark_local_dir=spark_local_dir,
    )  # type: ignore

    return {
        "platform_name": platform_name,
        "version": version,
        "start_date": start_date,
        "end_date": end_date,
        "n_processes": n_processes,
    }


@app.post("/aggregate_manual/", tags=["Aggregate"])
async def aggregate_manual(
    root_folder: str = Query(..., description="The root folder containing the daily dumps."),
    platform_name: PlatformNames = Query(PlatformNames["global"].name, description="The platform name."),
    version: T.TDB_dailyDumpsVersion = Query(
        T.TDB_dailyDumpsVersion.full, description="The version of the daily dumps (full or light)."
    ),
    out_file_name: str = Query(..., description="The output file name."),
    start_date: str = Query("", description="The start date for the aggregation (format YYYY-MM-DD)."),
    end_date: str = Query("", description="The end date for the aggregation (format YYYY-MM-DD)."),
    override_chunked_subfolder: str = Query(
        T.CHUNKED_FILES_SUBFOLDER_NAME,
        description="The subfolder name to override the default one. **Do not change it unless you know what you are doing**.",
    ),
    input_format: T.InputFileFormat = Query(T.InputFileFormat.parquet, description="Input file format."),
    delete_original_columns: bool = Query(
        False, description="Delete the original columns when horizontally exploding."
    ),
    horizontally_explode_columns: bool = Query(False, description="Horizontally explode the columns."),
    normalize_platform_name: bool = Query(
        False, description="Whether to coalesce platform names when it changed over time."
    ),
    normalize_content_type_other: bool = Query(False, description="Whether to normalize the content type other."),
    output_format: T.AggregateFileFormat = Query(T.AggregateFileFormat.parquet, description="Output file format."),
    fillna_str_value: Union[str, None] = Query(None, description="Value to use for filling NA values."),
    fillna_bool_value: Union[bool, None] = Query(False, description="Value to use for filling NA values."),
    content_date_range: Union[None, str] = Query(
        None, description="Content date range to use for filtering the input files."
    ),
    decision_date_range: Union[None, str] = Query(
        None, description="Decision date range to use for filtering the input files."
    ),
    created_at_date_range: Union[None, str] = Query(
        None, description="Created at date range to use for filtering the input files."
    ),
    platforms_to_exclude: list[PlatformNames] = Query([], description="Platforms to exclude."),
    columns_to_import: list[T.TDB_columnsFull] = Query([], description="Columns to import from the input files."),
    columns_to_group: list[T.RawAndExplodedColumn] = Query(
        [T.RawAndExplodedColumn.platform_name, T.RawAndExplodedColumn.category], description="Columns to groupby."
    ),  # type: ignore
    compute_time_to_action: bool = Query(False, description="Compute the average time to action and report."),
    compute_restriction_duration: bool = Query(
        False, description="Compute the restriction duration time when available."
    ),
    write_mode: T.AggregateWriteMode = Query(
        T.AggregateWriteMode.overwrite, description="Write mode for the output file."
    ),
    created_at_dt_floor: str = Query(
        "day",
        description="The argument to pass to pyspark sql date_trunc when flooring the creation date. By default `day`",
    ),
    n_processes: int = Query(
        -1,
        description="The number of processes to use for the aggregation. If <=0, it will use all the available cores.",
    ),
    memory_limit: Union[str, None] = Query(
        None, description="The memory limit to use for the aggregation. Use the format '10g' for 10GB."
    ),
    spark_local_dir: Union[str, None] = Query(
        None, description="The local directory to use for the Spark temporary files."
    ),
):
    """Aggregate the daily dumps with a manual configuration.
    Specify all the fields needed for the aggregation in the query parameters.
    The `content_date_range` and the other date ranges should be in the format "YYYY-MM-DD,YYYY-MM-DD",
    with the start date on the left and the end date on the right, separated by a comma (both the extremes will be included in the aggregation).
    """
    # Parse the date ranges
    if content_date_range:
        content_date_range_fix = content_date_range.split(",")
        content_date_range_fix = (
            datetime.strptime(content_date_range_fix[0], "%Y-%m-%d"),
            datetime.strptime(content_date_range_fix[1], "%Y-%m-%d"),
        )
    else:
        content_date_range_fix = None

    if decision_date_range:
        decision_date_range_fix = decision_date_range.split(",")
        decision_date_range_fix = (
            datetime.strptime(decision_date_range_fix[0], "%Y-%m-%d"),
            datetime.strptime(decision_date_range_fix[1], "%Y-%m-%d"),
        )
    else:
        decision_date_range_fix = None

    if created_at_date_range:
        created_at_date_range_fix = created_at_date_range.split(",")
        created_at_date_range_fix = (
            datetime.strptime(created_at_date_range_fix[0], "%Y-%m-%d"),
            datetime.strptime(created_at_date_range_fix[1], "%Y-%m-%d"),
        )
    else:
        created_at_date_range_fix = None

    # Divide the columns to import in datetime and others
    msg_out = ""
    if columns_to_import is None or len(columns_to_import) == 0:
        tmp_msg = "No columns to import specified, using all the columns."
        logger.warning(tmp_msg)
        msg_out += tmp_msg + "\n"
        if version == T.TDB_dailyDumpsVersion.full:
            columns_to_import_all = list(T.TDB_columnsFull._member_names_)
        elif version == T.TDB_dailyDumpsVersion.light:
            columns_to_import_all = list(T.TDB_columnsLight._member_names_)
        else:
            # Should never happen
            raise ValueError(f"Unknown version {version}")
    else:
        columns_to_import_all = columns_to_import
    columns_to_import_fix = [T.RawAndExplodedColumn[c].name for c in columns_to_import_all]
    dt_cols_to_import = [c for c in columns_to_import_fix if c in T.datetime_columns]
    other_cols_to_import = [c for c in columns_to_import_fix if c not in T.datetime_columns]

    columns_to_fill_bool = [c for k, v in T.columns_to_explode.items() for c in v]
    columns_to_fill_str = []

    # Create the config class
    tmp_config = T.AggregationConfig(
        input_format=input_format,
        delete_original_columns=delete_original_columns,
        horizontally_explode_columns=horizontally_explode_columns,
        normalize_platform_name=normalize_platform_name,
        normalize_content_type_other=normalize_content_type_other,
        output_format=output_format,
        fillna_str_value=fillna_str_value,
        fillna_bool_value=fillna_bool_value,
        content_date_range=content_date_range_fix,  # type: ignore
        decision_date_range=decision_date_range_fix,  # type: ignore
        created_at_date_range=created_at_date_range_fix,  # type: ignore
        platforms_to_exclude=platforms_to_exclude,  # type: ignore
        columns_to_import=other_cols_to_import,  # type: ignore
        columns_datetime=dt_cols_to_import,  # type: ignore
        columns_to_group=columns_to_group,
        columns_to_fill_str=columns_to_fill_str,
        columns_to_fill_bool=columns_to_fill_bool,  # type: ignore
        compute_time_to_action=compute_time_to_action,
        compute_restriction_duration=compute_restriction_duration,
        write_mode=write_mode,
        created_at_dt_floor=created_at_dt_floor,
    )

    aggregate_task.delay(
        root_folder=root_folder,
        platform_name=platform_name,
        version=version,
        config=tmp_config.model_dump(),
        out_file_name=out_file_name,
        start_date=start_date,
        end_date=end_date,
        override_chunked_subfolder=override_chunked_subfolder,
        n_processes=n_processes,
        memory_limit=memory_limit,
        spark_local_dir=spark_local_dir,
    )  # type: ignore

    return {
        "platform_name": platform_name,
        "version": version,
        "start_date": start_date,
        "end_date": end_date,
        "n_processes": n_processes,
    }


@app.get("/log", tags=["Log"], response_class=PlainTextResponse)
async def read_log(task_id: str, n_lines: int = 50):
    """Read the log file of a Celery task by providing the task_id and the number of lines to read.
    If n_lines is <=0, it will read all the file.
    You can find the task_id by looking at the [celery interface](http://localhost:5555)
    (change the `5555` port to your custom one if different).
    You can also follow the status of the aggregation and filtering tasks on the
    [Spark dashboard](http://localhost:4040) (change the `4040` port to your custom one if different).
    """
    # Open the filehandler in /tmp/my_celery_task.log and read the last n_lines (all if n_lines is <=0)
    logfile = f"/tmp/celery_task_{task_id}.log"  # nosec B108
    if not os.path.exists(logfile):
        return f"Log file {logfile} not found"
    with open(logfile) as f:
        if n_lines <= 0:
            txt = f.read()
        else:
            txt = "".join(f.readlines()[-n_lines:])
    return txt


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("FASTAPI_HOST", "0.0.0.0")  # nosec B104
    port = int(os.getenv("FASTAPI_PORT", 8000))
    uvicorn.run(app, host=host, port=port)
