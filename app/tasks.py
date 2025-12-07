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
from typing import List, Union

from celery import shared_task
from celery.utils.log import get_task_logger

import dsa_tdb
from dsa_tdb import types as T

logger = logging.getLogger(__name__)


def setup_loggers(logger, task_id, **kwargs):
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # add filehandler
    fh = logging.FileHandler(f"/tmp/celery_task_{task_id}.log")  # nosec B108
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)


@shared_task(bind=True, queue=T.CELERY_TASK_QUEUE)
def prepare(
    self,
    platform_name: str,
    version: str,
    start_date: str,
    end_date: str,
    platforms_to_exclude: List[str],
    output_dir: str,
    delete_original: bool,
    do_chunking: bool,
    check_sha1: bool,
    force_sha1: bool,
    chunk_format: T.TDB_chunkFormat,
    n_processes: int,
    override_chunked_subfolder: str,
):
    setup_loggers(get_task_logger(__name__), self.request.id)
    from_date = dsa_tdb.utils.compute_start_date(start_date)
    to_date = dsa_tdb.utils.compute_end_date(end_date)
    logger.info(f"Preparing {platform_name} {version} from {start_date} to {end_date} written to {output_dir}")
    version = T.TDB_dailyDumpsVersion(version)
    dsa_tdb.fetch.prepare_daily_dumps(
        dump_files_root_folder=output_dir,
        version=version,
        platform=platform_name,
        from_date=from_date,
        to_date=to_date,
        platforms_to_exclude=platforms_to_exclude,
        delete_original=delete_original,
        do_chunking=do_chunking,
        check_sha1=check_sha1,
        force_sha1=force_sha1,
        chunk_format=chunk_format,
        n_processes=n_processes,
        override_chunked_subfolder=override_chunked_subfolder,
    )

    return f"{platform_name} {version} from {start_date} to {end_date} prepared"


@shared_task(bind=True, queue=T.CELERY_TASK_QUEUE)
def filtering(
    self,
    root_folder: str,
    platform_name: str,
    version: str,
    config: Union[str, T.FilteringConfig],
    out_file_name: str,
    start_date: str,
    end_date: str,
    override_chunked_subfolder: str,
    n_processes: int = -1,
    memory_limit: Union[str, None] = None,
    spark_local_dir: Union[str, None] = None,
):
    setup_loggers(get_task_logger(__name__), self.request.id)
    logger.info(f"Filtering {platform_name} {version} from {start_date} to {end_date} written to {root_folder}")
    version = T.TDB_dailyDumpsVersion(version)

    if isinstance(config, dict):
        # This is the case when the object arrives from the webapp as a dict
        config = T.FilteringConfig(**config)

    dsa_tdb.cli._filterFiles(
        input_root_folder=root_folder,
        out_file_name=out_file_name,
        platform=platform_name,
        version=version,
        config=config,
        start_date=start_date,
        end_date=end_date,
        n_workers=n_processes,
        memory_limit=memory_limit,
        spark_local_dir=spark_local_dir,
        override_chunked_subfolder=override_chunked_subfolder,
    )

    return f"{platform_name} {version} from {start_date} to {end_date} filtered"


@shared_task(bind=True, queue=T.CELERY_TASK_QUEUE)
def aggregate(
    self,
    root_folder: str,
    platform_name: str,
    version: str,
    config: Union[str, T.AggregationConfig],
    out_file_name: str,
    start_date: str,
    end_date: str,
    override_chunked_subfolder: str,
    n_processes: int = -1,
    memory_limit: Union[str, None] = None,
    spark_local_dir: Union[str, None] = None,
):
    setup_loggers(get_task_logger(__name__), self.request.id)
    logger.info(f"Aggregating {platform_name} {version} from {start_date} to {end_date} written to {root_folder}")
    version = T.TDB_dailyDumpsVersion(version)

    if isinstance(config, dict):
        # This is the case when the object arrives from the webapp as a dict
        config = T.AggregationConfig(**config)

    dsa_tdb.cli._aggregateFiles(
        input_root_folder=root_folder,
        out_file_name=out_file_name,
        platform=platform_name,
        version=version,
        config=config,
        start_date=start_date,
        end_date=end_date,
        n_workers=n_processes,
        memory_limit=memory_limit,
        spark_local_dir=spark_local_dir,
        override_chunked_subfolder=override_chunked_subfolder,
    )

    return f"{platform_name} {version} from {start_date} to {end_date} aggregated"
