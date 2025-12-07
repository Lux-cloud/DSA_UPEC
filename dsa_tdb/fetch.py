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
import logging.handlers
import os
import re
import shutil
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import List, Tuple, Union
from zipfile import ZipFile

import bs4
import numpy as np
import pandas as pd
import psutil
import requests
import yaml
from tqdm import tqdm

import dsa_tdb
from dsa_tdb.etl import _write_out_chunk
from dsa_tdb.types import (
    ALL_PLATFORMS_ENTRY_VALUE,
    CHUNKED_FILE_SUCCESS_NAME,
    CHUNKED_FILES_SUBFOLDER_NAME,
    DAILY_CHUNKED_FOLDER_NAME_REGEX,
    DAILY_FILE_CHECKSUM_EXTENSION,
    DAILY_FILE_DATE_FORMAT,
    DAILY_FILE_EXTENSION,
    DAILY_FILE_NAME_REGEX,
    DAILY_FILE_NAME_TEMPLATE,
    DAILY_FILE_URL_TEMPLATE,
    DAILY_FILES_SUBFOLDER_TEMPLATE,
    DAILY_FILES_TABLE_URL,
    TDB_START_DATE,
    PreprocessArguments,
    TDB_chunkFormat,
    TDB_columnsFull,
    TDB_dailyDumpsVersion,
    accepted_chunk_formats,
    columns_to_explode,
    datetime_columns,
    datetime_format,
)
from dsa_tdb.utils import (
    _delete_daily_zipfile,
    _get_sha1_from_str,
    arra2vals,
    check_sha1_sum,
    fifo_pool,
    read_sha1_file,
    sanitize_platform_name,
    vals2arra,
)

logger = logging.getLogger(dsa_tdb.__name__)


def fetch_available_platforms(
    page_url: str = DAILY_FILES_TABLE_URL,
    select_name: str = "platform_id",
    select_id: str = "platform_id",
    select_class: str = "ecl-select",
    return_name: bool = False,
) -> dict:
    """Function to fetch the available platforms from the Transparency DB website.

    Parameters
    ----------
    page_url : str, optional
        The URL of the daily table. The function will append the
        query parameters to the URL '?page=[0-9]'.
        By default :attr:`dsa_tdb.types.DAILY_FILES_TABLE_URL`.
    select_name : str, optional
        The name of the select element to find, by default 'platform_id'.
    select_id : str, optional
        The id of the select element to find, by default 'platform_id'.
    select_class : str, optional
        The class of the select element to find, by default 'ecl-select'.
    return_name : bool, optional
        Whether to return the name of the select element, by default False.
        If False, the value of the selected platform (the platform uid) is returned.
        If True, the full name of the selected element is returned.

    Returns
    -------
    dict
        The dictionary of available platforms.
        The keys are the platform's sanitized names and the values are the platform's uids.
        One special key is 'global' which contains the global dump.
    """
    tmp_page = requests.get(page_url, timeout=10)
    tmp_soup = bs4.BeautifulSoup(tmp_page.text, "html.parser")

    # find the select element with name "uuid" and id "uuid" class "ecl-select"
    tmp_select = tmp_soup.find("select", {"name": select_name, "id": select_id, "class": select_class})
    if tmp_select is None:
        raise RuntimeError("Could not find the select element, edit the parameters")

    # Map the select options to a dictionary of {inner_text: value}
    tmp_options = {}
    for opt in tmp_select.find_all("option"):  # type: ignore
        if opt["value"] != "":
            tmp_text = opt.text
            if tmp_text.startswith(" "):
                tmp_text = tmp_text[1:]
            tmp_text_original = tmp_text

            tmp_text = sanitize_platform_name(tmp_text)
            if tmp_text in tmp_options:
                # Resorted to error, this should not happen in production!
                raise ValueError(f"Duplicate option `{tmp_text}` found, check the select element")
                # logger.warning(f"Duplicate option `{tmp_text}` found, check the select element")
            if return_name:
                tmp_options[tmp_text] = tmp_text_original
            else:
                tmp_options[tmp_text] = opt["value"]

    if len(tmp_options) == 0:
        raise ValueError("No options found in the select element")
    if "global" in tmp_options:
        raise ValueError("Global option found in the select element")

    del tmp_options[sanitize_platform_name(ALL_PLATFORMS_ENTRY_VALUE)]

    tmp_options["global"] = "global"
    return tmp_options


def fetch_daily_table(platform: str = "global", max_pages: int = 10000) -> pd.DataFrame:
    """Function to fetch the daily table from the Transparency DB website.
    It uses pagination and will stop when it finds a page with no data.

    Parameters
    ----------
    platform : str, optional
        The platform to fetch the daily table from, by default 'global'.
    max_pages : int, optional
        The maximum number of pages to fetch, by default 10000.

    Returns
    -------
    pd.DataFrame
        The daily table with the `Date` field converted to a datetime.
    """
    table_url = DAILY_FILES_TABLE_URL + "?"
    table_params = {}
    if platform != "global":
        platform_uid = get_platform_uid(platform)
        table_params["platform_id"] = platform_uid

    records = pd.DataFrame()
    for page in range(max_pages):
        table_params["page"] = page + 1
        tmp_url = table_url + urllib.parse.urlencode(table_params)
        logger.debug("Fetching page %d with URL %s", page + 1, tmp_url)
        tmp_df = pd.read_html(tmp_url, extract_links="body")
        tmp_df = tmp_df[0]
        if tmp_df.shape[0] == 0:
            logger.info("No entries found and breaking at page %d", page + 1)
            break
        else:
            logger.info(f"fetched page {page + 1} with {tmp_df.shape[0]} records...")
            # Extract the links
            tmp_df = tmp_df.apply(lambda col: [c[0] if c[1] is None else c[1] for c in col])
            tmp_df.columns = [
                c.lower() if c.lower() in (TDB_dailyDumpsVersion._member_names_) else c for c in tmp_df.columns
            ]  # Ensure relevant column names are lowercase
            records = pd.concat([tmp_df, records], axis=0, ignore_index=True, sort=True, verify_integrity=True)

    records["Date"] = pd.to_datetime(records["Date"], format="%Y-%m-%d")
    records = records.sort_values("Date")
    logger.info(f"Found {len(records):d} daily dumps on the website")

    return records


def check_local_storage(
    root_folder: str, force_sha1_check: bool = False, chunked_file_subfoder: str = CHUNKED_FILES_SUBFOLDER_NAME
) -> pd.DataFrame:
    """Function to check the local storage for the daily dumps.
    It will check for consistency between the daily dumps files, the sha1 files
    and the chunked files, if present.
    It will also check the sha1 of the files if `force_sha1_check` is `True`.

    Parameters
    ----------
    root_folder : str
        The root folder where to look for the daily dumps.
    force_sha1_check : bool, optional
        Whether to force the sha1 check of the files, by default False.
    chunked_file_subfoder : str, optional
        The name of the subfolder where to look for the chunked files, by default :attr:`dsa_tdb.types.CHUNKED_FILES_SUBFOLDER_NAME`.
        Do not use this parameter unless you know what you are doing.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the local files and their status.
        The dataframe will have the following columns:

        - `platform`: the platform name.
        - `version`: the version of the daily dump.
        - `date`: the date of the daily dump.
        - `dump_file_name`: the original daily dump file name.
        - `dump_file_path`: the original daily dump file path, if present, otherwise `None`.
        - `dump_sha1_path`: the sha1 file name if present, otherwise `None`.
        - `dump_sha1`: the sha1 value of the daily dump file if present, otherwise `None`.
        - `dump_sha1_check`: whether the sha1 check passed for the daily dump file. This will be `True` if:
            - `force_sha1_check` is `False` and no sha1 file or raw dump is found.
            - the daily dump file is or is not present and the sha1 file is consistent with the one found on the web page (if still listed).
            - `force_sha1_check` is `True` and the sha1 check passed.
        - `chunked_folder`: the chunked folder path if present, otherwise `None`.
        - `chuncked_success`: whether the chunking was successful.
        - `chunked_folder_sha1`: the sha1 value of the chunked folder if present, otherwise `None`.
        - `chunked_sha1_check`: whether the sha1 corresponds to the daily dump's one.

    """

    # Create the ouput DataFrame
    # Force columns to deal with empty data
    columns_records = [
        "platform",
        "version",
        "date",
        "dump_file_name",
        "dump_file_path",
        "dump_sha1_path",
        "dump_sha1",
        "dump_sha1_check",
        "chunked_folder",
        "chunked_success",
        "chunked_folder_sha1",
        "chunked_sha1_check",
    ]
    df_out = pd.DataFrame(data=[], columns=columns_records)

    # Loop over the subfolders of the root folder
    root_path = Path(root_folder)
    for subfolder in root_path.iterdir():
        if not subfolder.is_dir():
            continue
        if "___" not in subfolder.name:
            logger.warning("Found unknown subfolder %s", subfolder)
            continue
        platform, version = subfolder.name.split("___")

        # Check for all sor and/or sha1 files
        # Using the date as unique identifier
        tmp_platform_records = {}
        for file in subfolder.glob(
            DAILY_FILE_NAME_TEMPLATE.format(
                platform=platform, version=version, date="*", extension=DAILY_FILE_EXTENSION + "*"
            )
        ):
            tmp_date = re.search(DAILY_FILE_NAME_REGEX, file.name).group("date")  # type: ignore
            try:
                tmp_record = tmp_platform_records[tmp_date]
            except KeyError:
                tmp_platform_records[tmp_date] = {}
                tmp_record = tmp_platform_records[tmp_date]

            if "".join(file.suffixes) == DAILY_FILE_CHECKSUM_EXTENSION:
                tmp_hash = read_sha1_file(file)
                tmp_record["dump_sha1_path"] = file
                tmp_record["dump_sha1"] = tmp_hash
            elif file.suffix == DAILY_FILE_EXTENSION:
                tmp_record["dump_file_path"] = file
                tmp_record["dump_file_name"] = file.name
            else:
                logger.warning("Found unknown file %s with extension %s", file, file.suffix)
                continue

        # Now check for the chunked folders
        chunked_home_folder = subfolder / chunked_file_subfoder
        if chunked_home_folder.is_dir():
            for chunk_folder in chunked_home_folder.iterdir():
                if not subfolder.is_dir():
                    continue
                # Check if the CHUNKED_FILE_SUCCESS_NAME file is present
                # to see if the chunking was successful
                chunked_success_file = chunk_folder / CHUNKED_FILE_SUCCESS_NAME  # type: ignore
                chunked_success = chunked_success_file.is_file()
                # If success is there, read the sha from it
                if chunked_success:
                    chunked_sha1 = read_sha1_file(chunked_success_file)
                else:
                    chunked_sha1 = None

                tmp_date = re.search(DAILY_CHUNKED_FOLDER_NAME_REGEX, chunk_folder.name).group("date")  # type: ignore

                try:
                    tmp_record = tmp_platform_records[tmp_date]
                except KeyError:
                    tmp_platform_records[tmp_date] = {}
                    tmp_record = tmp_platform_records[tmp_date]

                tmp_record["chunked_folder"] = chunk_folder
                tmp_record["chunked_success"] = chunked_success
                tmp_record["chunked_folder_sha1"] = chunked_sha1

        records_out = pd.DataFrame(
            [dict(platform=platform, version=version, date=k, **v) for k, v in tmp_platform_records.items()],
            columns=columns_records,
            dtype="object",
        )
        # Ensure we use none instead of NaN for an easier check
        records_out = records_out.where(pd.notnull(records_out), None)

        logger.info("Checking %s - %s SHA1", platform, version)
        for idx, row in records_out.iterrows():
            # Check the sha1 of the daily dump file
            # Create the reference path and URL for the sha1 that might need to be downloaded
            sha1_url = DAILY_FILE_URL_TEMPLATE.format(
                platform=platform, version=version, extension=DAILY_FILE_CHECKSUM_EXTENSION, date=row["date"]
            )
            tmp_sha1_path = subfolder / DAILY_FILE_NAME_TEMPLATE.format(
                platform=platform, version=version, date=row["date"], extension=DAILY_FILE_CHECKSUM_EXTENSION
            )
            if not row["dump_file_name"]:
                # Fill the name of the dump for reference, the path is the one stating if it is present
                records_out.loc[idx, "dump_file_name"] = DAILY_FILE_NAME_TEMPLATE.format(
                    platform=platform,  # type: ignore
                    version=version,
                    date=row["date"],
                    extension=DAILY_FILE_EXTENSION,
                )
            if row["dump_sha1"]:
                if row["dump_file_path"] and force_sha1_check:
                    try:
                        logger.info("Checking sha1 for %s", row["dump_file_path"])
                        tmp_sha1_check = check_sha1_sum(row["dump_file_path"], row["dump_sha1_path"])
                        logger.debug("SHA1 check result: %s for file %s", tmp_sha1_check, row["dump_file_path"])
                        records_out.loc[idx, "dump_sha1_check"] = tmp_sha1_check  # type: ignore
                    except AssertionError:
                        logger.warning("SHA1 check failed for %s", row["dump_file_path"])
                        records_out.loc[idx, "dump_sha1_check"] = False  # type: ignore
                else:
                    records_out.loc[idx, "dump_sha1_check"] = True  # type: ignore
            else:
                # Since we are here, let's download the sha1 file, if any, to reconstruct the sha1
                # This will make sure that all the sha1  of daily dumps present are re-downloaded, if missing
                download_code = download_file(sha1_url, tmp_sha1_path, check_sha1=False, raise_on_error=False)
                if download_code is None:
                    logger.warning("Could not download sha1 file for %s", row["dump_file_path"])
                    records_out.loc[idx, "dump_sha1_check"] = False  # type: ignore
                    records_out.loc[idx, "dump_sha1_path"] = None  # type: ignore
                else:
                    records_out.loc[idx, "dump_sha1_path"] = tmp_sha1_path  # type: ignore

                if row["dump_file_path"]:
                    # Check that it is consistent with the sha1 file
                    try:
                        logger.info("Checking sha1 for %s", row["dump_file_path"])
                        records_out.loc[idx, "dump_file_path"] = check_sha1_sum(
                            row["dump_file_path"],  # type: ignore
                            tmp_sha1_path,
                        )
                    except AssertionError:
                        logger.warning("SHA1 check failed for %s", row["dump_file_path"])
                        records_out.loc[idx, "dump_sha1_check"] = False  # type: ignore
                else:
                    # Assume the check with local file is ok
                    records_out.loc[idx, "dump_sha1_check"] = True  # type: ignore

            # Check the sha1 of the chunked folder
            if row["dump_sha1"]:
                # The dump sha1 should be there for the existing dumps, if present, by construction
                records_out.loc[idx, "chunked_sha1_check"] = row["chunked_folder_sha1"] == row["dump_sha1"]  # type: ignore
            else:
                # If the dump sha1 is not present, try to fetch the sha1, if still there, otherwise
                download_code = download_file(sha1_url, tmp_sha1_path, check_sha1=False, raise_on_error=False)
                if download_code is None:
                    logger.warning("Could not download sha1 file for %s", row["dump_file_path"])
                    records_out.loc[idx, "chunked_sha1_check"] = True  # type: ignore
                    records_out.loc[idx, "dump_sha1_path"] = None  # type: ignore
                    records_out.loc[idx, "dump_sha1"] = None  # type: ignore
                else:
                    records_out.loc[idx, "dump_sha1_path"] = tmp_sha1_path  # type: ignore
                    records_out.loc[idx, "dump_sha1"] = read_sha1_file(tmp_sha1_path)  # type: ignore
                    # Check that it is consistent with the sha1 file
                    records_out.loc[idx, "chunked_sha1_check"] = row["chunked_folder_sha1"] == row["dump_sha1"]  # type: ignore

        # Append the records to the output DataFrame
        records_out = pd.DataFrame(records_out, columns=columns_records)
        records_out["date"] = pd.to_datetime(records_out["date"], format=DAILY_FILE_DATE_FORMAT)
        if df_out.shape[0] > 0:
            df_out = pd.concat((df_out, records_out), ignore_index=True, sort=True, verify_integrity=True)
        else:
            df_out = records_out

    return df_out


def read_file_from_url(url: str) -> Union[str, None]:
    """Function to read a file from a URL.

    Parameters
    ----------
    url : str
        The URL of the file to read.

    Returns
    -------
    str, None
        The content of the file, if the download was successful.
        None if the download failed.
    """
    try:
        if not url.startswith("http://") and not url.startswith("https://"):
            raise ValueError("Unsupported protocol in URL %s." % url)
        conn = urllib.request.urlopen(url, timeout=10)  # nosec B310 - file protocol is disabled
    except urllib.error.HTTPError as e:
        # Return code error (e.g. 404, 501, ...)
        logger.warning(f"HTTPError: {e.code}")
        return None
    except urllib.error.URLError as e:
        from socket import timeout

        # Not an HTTP-specific error (e.g. connection refused)
        # ...
        if isinstance(e.reason, timeout):
            logger.info("Timeout error for %s, will rety", url)
            return read_file_from_url(url)
        else:
            logger.warning(f"URLError: {e.reason}")
        return None
    else:
        # 200
        return conn.read().decode("utf-8")


def download_file(
    url: str,
    local_filename: Union[str, Path],
    check_sha1: bool = True,
    BUF_SIZE: int = 65536,
    raise_on_error: bool = True,
    n_tries: int = 2,
) -> Union[str, None]:
    """Function to download a file from a URL.
    It will check the sha1 if `check_sha1` is `True`.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    local_filename : str
        The output file name.
    check_sha1 : bool, optional
        Whether to check the sha1 of the file, by default True
    BUF_SIZE : int, optional
        The buffer size in bits to use when computing sha1, by default 65536
    raise_on_error : bool, optional
        Whether to raise an exception if the download fails, by default False
    n_tries : int, optional
        The number of times to try downloading the file before giving up/raising an exception, by default 2

    Returns
    -------
    str, None
        The path of the downloaded file.
        None if the download failed. In this case, a message will be sent to the logger
        process, the file downloaded will be erased and the function will return None.
    """
    message = "Downloading %s to %s" % (url, local_filename)
    logger.log(logging.INFO, message)

    if isinstance(local_filename, str):
        local_filename = Path(local_filename)

    if check_sha1:
        # Regardless of local sha1, always rely on the remote version for sha1
        local_sha1_filename = local_filename.with_suffix(DAILY_FILE_CHECKSUM_EXTENSION)
        url_sha1 = url[: -len(DAILY_FILE_EXTENSION)] + DAILY_FILE_CHECKSUM_EXTENSION
        logger.log(logging.INFO, "Downloading sha1 for %s", url_sha1.split("/")[-1])
        download_file(url_sha1, local_sha1_filename, check_sha1=False, raise_on_error=True)

    while True:
        try:
            with requests.get(url, stream=True, timeout=10) as r:
                if r.status_code != 200:
                    message = f"Could not download {url}, exited with code {r.status_code}"
                    if raise_on_error:
                        raise Exception(message)
                    else:
                        logger.warning(message)
                        return None
                with open(local_filename, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
        except (KeyboardInterrupt, SystemExit):
            # These will leave the sha1 file if it fails when downloading the file. This is
            # expected behavior as the sha1 was correctly downloaded.
            logger.warning("Download interrupted, deleting file %s", local_filename)
            if os.path.exists(local_filename):
                os.remove(local_filename)
            return None
        except Exception as e:
            if n_tries <= 0:
                os.remove(local_filename)
                if raise_on_error:
                    raise Exception(message)
                else:
                    logger.exception(f"Download failed for file {url}: {message}")
                    return None
            else:
                logger.warning(f"Download of file {url} failed with error {e}, retrying...")
                n_tries -= 1
        else:
            break

    if check_sha1:
        check_sha1_sum(local_filename, local_sha1_filename, BUF_SIZE=BUF_SIZE)
    logging.log(logging.INFO, "Downloaded %s", local_filename)
    return str(local_filename)


def chunkFile(
    csv_zip_in: Union[str, Path],
    folder_out: Union[str, Path],
    header_in: bool = True,
    header_out: bool = True,
    chunk_size: int = 100000,
    platforms_to_exclude: Union[List[str], None] = None,
    overwrite: bool = True,
    extension: str = ".zip",
    out_format: str = "csv",
    delete_original: bool = False,
) -> str:
    """Chunks a csv file into multiple files.

    Parameters
    ----------
    csv_zip_in : str
        The input csv zipped file.
    folder_out : str
        The output folder. This will be populated with a folder named as the csv file
        without the `.zip` extension containing the chunks in the format
        `part-0000.csv.gz`.
    header_in : bool, optional
        Whether the input file has a header, by default True
    header_out : bool, optional
        Whether the output files have a header, by default True
    chunk_size : int, optional
        The size of the chunks, by default 100000
    platforms_to_exclude : Union[List[str], None], optional
        The list of the platforms' names to exclude, by default None (keep all).
    overwrite : bool, optional
        Whether to overwrite the output folder, by default True
    extension : str, optional
        The extension of the input file, by default '.zip'
    out_format : str, optional
        The format of the output files, by default 'csv'.
        Must be one of ['csv', 'parquet']. If 'parquet' the output files will be
        named `folder_out/basename_csv/part-0000.parquet`.
    delete_original : bool, optional
        Whether to delete the original file, by default False

    Returns
    -------
    str
        The output folder where the chunks of the file are saved.
    """
    if isinstance(csv_zip_in, str):
        csv_zip_in = Path(csv_zip_in)
    if isinstance(folder_out, str):
        folder_out = Path(folder_out)

    if not (header_in or (not header_in and not header_out)):
        raise ValueError("Header must be present or absent in both input and output")

    if out_format not in accepted_chunk_formats:
        raise ValueError(f"Format must be one of {accepted_chunk_formats}")

    # Check that platforms to drop are valid
    try:
        platforms_to_exclude_fullname = dsa_tdb.fetch.get_platforms_fullnames(platforms_to_exclude)
    except ValueError as e:
        raise ValueError(
            f"platforms_to_exclude must be a list of valid platforms names!\n"
            f"platforms_to_exclude = {platforms_to_exclude} found error: {e}"
        )

    # Save in folder/out/filename/part-0000.csv.gz
    folder_out = os.path.join(folder_out, os.path.basename(csv_zip_in).replace(extension, ""))
    success_file = os.path.join(folder_out, CHUNKED_FILE_SUCCESS_NAME)
    if os.path.exists(folder_out):
        if overwrite:
            message = f"Output folder {folder_out} already exists, overwriting..."
            logger.warning(f"Output folder {folder_out} already exists, overwriting...")
            shutil.rmtree(folder_out)
        elif os.path.exists(success_file):
            message = f"Output folder {folder_out} already exists, skipping..."
            logger.warning(message)
            if delete_original:
                _delete_daily_zipfile(csv_zip_in)
            return folder_out
        else:
            logger.warning(f"Output folder {folder_out} already exists, but not completed, overwriting...")
            shutil.rmtree(folder_out)

    message = "Processing file %s" % csv_zip_in
    logger.info(message)
    os.makedirs(folder_out, exist_ok=False)

    read_header = None
    wroteSomething = False
    tmp_zfile_idx = "__not_initialized__"
    raw_csv_file_idx = "__not_initialized__"
    # Ensures datetime are correctly loaded in pandas>=1.2.2
    # Missing columns are ignored so this works also with the light dumps version
    tmp_dtype = {k: "str" for k in TDB_columnsFull._member_names_ if k not in datetime_columns}
    tmp_out_chunk = pd.DataFrame()
    tmp_out_chunk_index = 0
    try:
        with ZipFile(csv_zip_in, "r") as zip_ref:
            for tmp_zfile_idx, tmp_zfile in enumerate(sorted(zip_ref.namelist())):
                inner_zfile = BytesIO(zip_ref.read(tmp_zfile))
                with ZipFile(inner_zfile) as inner_zip_ref:
                    for raw_csv_file_idx, raw_csv_file in enumerate(sorted(inner_zip_ref.namelist())):
                        with inner_zip_ref.open(raw_csv_file) as tmp_csv_file:
                            with pd.read_csv(
                                tmp_csv_file,
                                dtype=tmp_dtype,
                                chunksize=chunk_size,
                                parse_dates=datetime_columns,
                                date_format=datetime_format,
                                header=0 if header_in else None,
                            ) as reader:
                                for part, chunk in enumerate(reader):
                                    if read_header is None:
                                        read_header = chunk.columns
                                    else:
                                        if not all(read_header == chunk.columns):
                                            raise AssertionError(
                                                "Incorrect header processed, header and chunk columns differ"
                                            )

                                    if platforms_to_exclude is not None:
                                        chunk = chunk[
                                            ~chunk[TDB_columnsFull.platform_name.value].isin(
                                                platforms_to_exclude_fullname
                                            )
                                        ]
                                    if chunk.shape[0] == 0:
                                        message = f"Empty chunk {part:d} in file {csv_zip_in} part {tmp_zfile_idx:d} subpart {raw_csv_file_idx:d}, skipping (this is fine!)..."
                                        logger.info(message)
                                        continue
                                    else:
                                        # Skeleton for the encoding of the exploded columns
                                        # if True: # Unpack and stuff
                                        #     with pd.option_context('future.no_silent_downcasting', True):
                                        #         chunk[TDB_columnsFull.category.value] = chunk[TDB_columnsFull.category.value].replace(CategoryDict)
                                        #         chunk[TDB_columnsFull.category.value] = pd.to_numeric(chunk[TDB_columnsFull.category.value])

                                        #     chunk[TDB_columnsFull.decision_visibility.value] = (chunk[TDB_columnsFull.decision_visibility.value].
                                        #                                                                 apply(lambda s: sorted([DecisionVisibilityDict[e]
                                        #                                                                                         for e in vals2arra(s)]
                                        #                                                                                         if vals2arra(s) else [])))
                                        #     chunk.drop(columns=TDB_freetextColumns._member_names_, inplace=True)

                                        # Enforcing the array cols to have None in the '' and '[]' cases
                                        # Also making sure that these columns are sorted
                                        # Note that the territoial scope column is the only one that is guaranteed to be sorted
                                        # So we skip it.
                                        for col in columns_to_explode.keys():
                                            chunk[col] = chunk[col].apply(
                                                lambda x: None
                                                if x in ["", "[]", None, "None", "<NA>", np.nan, "np.nan"]
                                                else arra2vals(vals2arra(x))
                                            )

                                        tmp_out_chunk = pd.concat([tmp_out_chunk, chunk])

                                    if tmp_out_chunk.shape[0] >= chunk_size:
                                        tmp_out_chunk = _write_out_chunk(
                                            chunk=tmp_out_chunk,
                                            folder_out=folder_out,
                                            out_format=out_format,
                                            chunk_size=chunk_size,
                                            part=tmp_out_chunk_index,
                                        )
                                        wroteSomething = True
                                        tmp_out_chunk_index += 1

            while tmp_out_chunk.shape[0] > 0:
                # While because the last chunk might be larger than chunk_size
                tmp_out_chunk = _write_out_chunk(
                    chunk=tmp_out_chunk,
                    folder_out=folder_out,
                    out_format=out_format,
                    chunk_size=chunk_size,
                    part=tmp_out_chunk_index,
                )
                wroteSomething = True
                tmp_out_chunk_index += 1
    except Exception:
        logger.exception(
            f"Exception for file {csv_zip_in} part {tmp_zfile_idx} raw_csv {raw_csv_file_idx}, removing out folder..."
        )
        shutil.rmtree(folder_out)
        raise

    if not wroteSomething:
        # TODO This choice would cause the empty files to he re-downloaded every time (as the
        # COMPLETE file is not written). To consider if we should still leave the empty folder there with
        # the COMPLETE file
        message = f"No chunks written for file {csv_zip_in}, leaving empty folder..."
        logger.warning(message)
        # shutil.rmtree(folder_out)
    else:
        message = "Finished processing file %s" % csv_zip_in
        logger.info(message)

    if delete_original:
        _delete_daily_zipfile(csv_zip_in)

    # Put in the COMPLETE file inside chunk folder to keep track of the completeness of the chunking process
    with open(success_file, "w") as fout:
        with open(csv_zip_in.with_suffix(DAILY_FILE_CHECKSUM_EXTENSION)) as fin:
            fout.write(fin.read())

    return folder_out


def prepare_daily_dumps(
    dump_files_root_folder: str,
    version: TDB_dailyDumpsVersion = TDB_dailyDumpsVersion.full,
    platform: str = "global",
    platforms_to_exclude: Union[List[str], None] = None,
    check_sha1: bool = True,
    force_sha1: bool = False,
    from_date: Union[date, None] = None,
    to_date: Union[date, None] = None,
    n_processes: int = 1,
    do_chunking: bool = False,
    chunk_size: int = 1000000,
    chunk_format: TDB_chunkFormat = TDB_chunkFormat.parquet,
    delete_original: bool = False,
    loglevel: Union[int, str] = logging.INFO,
    override_chunked_subfolder: str = CHUNKED_FILES_SUBFOLDER_NAME,
    raise_on_error: bool = True,
) -> pd.DataFrame:
    """
    Parameters
    ----------
    dump_files_folder : str
        The root folder where to create a `platform___version` subfolder where to save
        the downloaded files.
    version : str, optional
        The version of the files to download, by default 'full'.
        Can be 'full' or 'light'.
    platform : str, optional
        The platform to download the files from, by default 'global'.
        The other platforms' names can be retrieved visiting the
        [transparency db page](https://transparency.dsa.ec.europa.eu/data-download)
        and getting their name. The name will be sanitized to be used as a folder name
        using the :func:`dsa_tdb.fetch.sanitize_platform_name` function.
    check_sha1 : bool, optional
        Whether to check the sha1 of the downloaded file, by default True.
    force_sha1 : bool, optional
        Whether to force the sha1 check of the already existing files, by default False.
    from_date : str, optional
        The date from which to start downloading the files, by default None.
        It must be in the form YYYY-MM-DD.
    to_date : str, optional
        The date until which to download the files, by default None.
        It must be in the form YYYY-MM-DD.
        Can also be the same as `from_date` to download only one file.
    n_processes : int, optional
        The number of processes to use to download the files, by default 1.
        If > 1, it will use multiprocessing to download the files in parallel.
    do_chunking : bool, optional
        Whether to contextually chunk the downloaded files, by default False.
        This can be useful to speed up the processing of the files and to limit
        the disk space usage.
    platforms_to_exclude : List[str], optional
        The platforms to exclude from the download, by default None.
        Here the names of the platforms should be the original ones, as found on
        the website and without sanitization/manipulations.
    chunk_size : int, optional
        The size of the chunks in number of SoR, by default 1000000.
    chunk_format : str, optional
        The format of the chunks, by default 'parquet'.
        Can be one of :attr:`dsa_tdb.types.TDB_chunkFormat`.
    delete_original : bool, optional
        Whether to delete the original file after chunking (will keep SHA1 files
        for archives purposes), by default False.
    loglevel : int, optional
        The log level to use, by default logging.INFO.
    override_chunked_subfolder : str, optional
        The name of the subfolder where to save the chunked files, by default None.
        If None, the subfolder will be named as :attr:`dsa_tdb.types.CHUNKED_FILES_SUBFOLDER_NAME`.
        Do not use this parameter unless you know what you are doing.
    raise_on_error : bool, optional
        If True (default), failing sha1 checks on dumps and chunks will waise an exception.
        If False, it will try to re-download / re-chunk the failing bits.
        By default True.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the local files and their status.
        See the :attr:`dsa_tdb.fetch.check_local_storage` function for more details.
    """

    platform = sanitize_platform_name(platform, warn_on_change=True)

    dump_files_folder = os.path.join(
        dump_files_root_folder, DAILY_FILES_SUBFOLDER_TEMPLATE.format(platform=platform, version=version)
    )
    if platforms_to_exclude:
        # Check that platforms to drop are valid
        try:
            get_platforms_fullnames(platforms_to_exclude)
        except ValueError as e:
            raise ValueError(
                f"platforms_to_exclude must be a list of valid platforms names!\n"
                f"platforms_to_exclude = {platforms_to_exclude}\nFound error:\n{e}"
            )
    if n_processes < 1:
        n_processes = psutil.cpu_count(logical=True)

    _args = PreprocessArguments(
        dump_files_folder=dump_files_folder,
        version=version,
        platform=platform,
        check_sha1=check_sha1,
        force_sha1=force_sha1,
        from_date=from_date,
        to_date=to_date,
        n_processes=n_processes,
        do_chunking=do_chunking,
        platforms_to_exclude=platforms_to_exclude,
        chunk_size=chunk_size,
        chunk_format=chunk_format,
        delete_original=delete_original,
        loglevel=loglevel,  # type: ignore
        override_chunked_subfolder=override_chunked_subfolder,
        raise_on_error=raise_on_error,
    )

    # Check that platform is valid
    get_platform_uid(_args.platform)

    # Check that platforms to drop are valid
    try:
        get_platforms_fullnames(platforms_to_exclude)
    except ValueError as e:
        raise ValueError(
            f"platforms_to_exclude must be a list of valid platforms names!\n"
            f"Available platforms are: {list(fetch_available_platforms().keys())}\n"
            f"platforms_to_exclude = {platforms_to_exclude}\nFound error:\n{e}"
        )

    # Create folder and prepare filename patterns
    daily_file_name = DAILY_FILE_NAME_TEMPLATE.format(
        platform=_args.platform,
        version=_args.version,
        extension=DAILY_FILE_EXTENSION,
        date=DAILY_FILE_DATE_FORMAT,
    )

    # Set the date range
    from_date = (
        datetime(_args.from_date.year, _args.from_date.month, _args.from_date.day)
        if _args.from_date is not None
        else TDB_START_DATE
    )
    to_date = (
        datetime(_args.to_date.year, _args.to_date.month, _args.to_date.day)
        if _args.to_date is not None
        else datetime.now()
    )

    # Create output folder if it does not exist
    os.makedirs(_args.dump_files_folder, exist_ok=True)

    # Get the snapshot of the local storage for the specified platform and version
    # The forced sha1 check will be done in parallel by the _preprocess_daily_dumps function
    local_files = check_local_storage(
        dump_files_root_folder, force_sha1_check=False, chunked_file_subfoder=_args.override_chunked_subfolder
    )
    local_files = local_files[
        (local_files["platform"] == _args.platform)
        & (local_files["version"] == _args.version)
        & (local_files["date"] >= from_date)
        & (local_files["date"] <= to_date)
    ].copy()

    # Get the daily table and cut it to the specified date range
    web_records = fetch_daily_table(platform=_args.platform)
    web_records = web_records[(web_records["Date"] >= from_date) & (web_records["Date"] <= to_date)][
        ["Date"] + TDB_dailyDumpsVersion._member_names_
    ].rename(columns={"Date": "date"})
    web_records["platform"] = _args.platform
    web_records["version"] = _args.version
    # New in 0.6.5: we fetch the download link directly from the web table.
    logger.debug(f"Got the dataframe out with the columns {web_records.columns}")
    logger.debug(f"Head of the dataframe:\n {web_records.head(2)}")
    web_records["file_dump_url"] = web_records[_args.version]
    web_records["file_sha1_url"] = web_records["file_dump_url"].apply(
        lambda s: s[: -len(DAILY_FILE_EXTENSION)] + DAILY_FILE_CHECKSUM_EXTENSION
    )
    web_records["web_sha1"] = None

    merged_situation = pd.merge(local_files, web_records, on=["platform", "version", "date"], how="outer")
    merged_situation = merged_situation.sort_values("date")
    merged_situation = merged_situation.where(pd.notnull(merged_situation), None)

    # First, let's fill the sha1 that we can to avoid downloading them again
    for idx, row in tqdm(merged_situation.iterrows(), desc="Filling sha1", total=len(merged_situation)):
        if row["file_sha1_url"]:
            tmp_sha1_full = read_file_from_url(row["file_sha1_url"])
            if tmp_sha1_full is None:
                logger.warning("Could not read sha1 file from %s even though listed", row["file_sha1_url"])
                continue
            tmp_sha1 = _get_sha1_from_str(tmp_sha1_full)
            if row["dump_sha1"] is None:
                # This should be redundant with the check already performed by the check_local_storage
                # but it's better to be safe than sorry
                local_sha1_filename = os.path.join(
                    _args.dump_files_folder,
                    DAILY_FILE_NAME_TEMPLATE.format(
                        platform=_args.platform,
                        version=_args.version,
                        date=row["date"].strftime(DAILY_FILE_DATE_FORMAT),
                        extension=DAILY_FILE_CHECKSUM_EXTENSION,
                    ),
                )
                with open(local_sha1_filename, "w") as f:
                    f.write(tmp_sha1_full)
                merged_situation.loc[idx, "dump_sha1"] = tmp_sha1  # type: ignore
                merged_situation.loc[idx, "dump_sha1_path"] = local_sha1_filename  # type: ignore
                if row["dump_file_path"]:
                    # The file was there, let's check its sha1
                    logger.debug("Checking sha1 for %s as the dump was there but the sha1 not:", row["dump_file_path"])
                    try:
                        check_sha1_sum(row["dump_file_path"], local_sha1_filename)
                        merged_situation.loc[idx, "dump_sha1_check"] = True  # type: ignore
                    except AssertionError:
                        # Mark the file for deletion and the sha1 as not checked
                        logger.warning("SHA1 check failed for %s", row["dump_file_path"])
                        merged_situation.loc[idx, "dump_sha1_check"] = False  # type: ignore
            merged_situation.loc[idx, "web_sha1"] = tmp_sha1  # type: ignore
            merged_situation.loc[idx, "web_sha1_dump_check"] = row["dump_sha1"] == tmp_sha1  # type: ignore
            merged_situation.loc[idx, "web_sha1_chunk_check"] = row["chunked_folder_sha1"] == tmp_sha1  # type: ignore
        else:
            # If we don't have a web sha1 then we rely on local info only
            merged_situation.loc[idx, "web_sha1_dump_check"] = True  # type: ignore
            merged_situation.loc[idx, "web_sha1_chunk_check"] = True  # type: ignore

    # At this point we have all the sha1 files that can still be retrieved in place
    # The table is also filled with all the sha1 either from the dumps, the chunks and the web
    # We now have a complete picture of the situation and we have to decide in a decision tree
    # from the presence (and consistency + availability) of the chunks, sha1 and dumps, what to do
    # with the files
    # The dumps to chunk are the ones that are not chunked or for which the chunked sha1 is not consistent with
    # either the web or the dump version
    merged_situation["dump_to_chunk"] = np.logical_or(
        # The ones with a web sha1 and a discrepancy between the chunk and web sha1
        np.logical_and(
            merged_situation["web_sha1"].notnull(),
            np.logical_not(merged_situation["web_sha1_chunk_check"]),
        ),
        # The ones without a web sha1 and a discrepancy between the chunk and dump sha1 (the dump is the sourcce
        # of thruth)
        np.logical_and(
            merged_situation["web_sha1"].isna(),
            np.logical_not(merged_situation["chunked_sha1_check"]),
        ),
    )

    # We have to give up on the ones that are no more available to download and for which we miss a valid local dump
    merged_situation["dump_to_chunk_real"] = np.logical_and(
        merged_situation["dump_to_chunk"],
        np.logical_or(
            # The ones with still something online
            merged_situation["file_dump_url"].notnull(),
            # The ones with a local dump but a failed chunks
            np.logical_and(merged_situation["dump_file_path"].notnull(), merged_situation["dump_sha1_check"]),
        ),
    )
    discrepancy = merged_situation.query("dump_to_chunk_real != dump_to_chunk")
    if discrepancy.shape[0] > 0:
        logger.info("Dropped %d files that are not available for download anymore", discrepancy.shape[0])
        logger.info("The dropped dates are: %s", discrepancy["date"].dt.strftime(DAILY_FILE_DATE_FORMAT).values)
    merged_situation["dump_to_chunk"] = merged_situation["dump_to_chunk_real"]
    merged_situation.drop(columns=["dump_to_chunk_real"], inplace=True)

    # Now we have to download the files that are missing, that is, the ones that are not chunked and for which
    # we have no dump file or an invalid sha1 (either locally or from the web)
    merged_situation["dump_to_download"] = np.logical_and(
        merged_situation["dump_to_chunk"],
        np.logical_or(
            np.logical_not(merged_situation["web_sha1_dump_check"]),
            np.logical_or(
                np.logical_not(merged_situation["dump_sha1_check"]), merged_situation["dump_file_path"].isna()
            ),
        ),
    )
    # Keep only the ones that are available for download
    num_download_before = merged_situation["dump_to_download"].sum()
    merged_situation["dump_to_download"] = np.logical_and(
        merged_situation["dump_to_download"], merged_situation["file_dump_url"].notnull()
    )
    num_download_after = merged_situation["dump_to_download"].sum()
    if num_download_before != num_download_after:
        # This should not happen, but it's better to be safe than sorry
        logger.warning(
            "Dropped %d files that are not available for download anymore - this should not happen!",
            num_download_before - num_download_after,
        )

    # Mark the dumps to delete
    merged_situation["dump_to_delete"] = _args.delete_original

    logger.info(
        "Found {:d} daily dumps to download between {} and {}".format(
            merged_situation["dump_to_download"].sum(), from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        )
    )
    logger.info(
        "Found {:d} daily dumps to chunk between {} and {}".format(
            merged_situation["dump_to_chunk"].sum(), from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")
        )
    )

    # Put the downloads first and then sort by date
    merged_situation["low_priority"] = np.logical_not(merged_situation["dump_to_download"])
    merged_situation.sort_values(["low_priority", "date"], inplace=True)
    del merged_situation["low_priority"]

    args = []
    for i, r in merged_situation.iterrows():
        tmp_args = [
            r["file_dump_url"],  # URL
            os.path.join(_args.dump_files_folder, r["date"].strftime(daily_file_name)),  # dump_file local path
            r["dump_to_download"],  # do_download
            _args.check_sha1,  # check_sha1
            _args.force_sha1,  # force_check_sha1
            65536,  # BUF_SIZE
            _args.raise_on_error,  # raise_on_error
            _args.do_chunking,  # do_chunking from 0.6.13 is passed as the original flag: the inner function will decide whether chunking is needed or not based on sha1 checks.
            _args.chunk_size,  # chunk_size
            _args.platforms_to_exclude,  # platforms_to_exclude
            _args.chunk_format,  # output_format
            r["dump_to_delete"],  # delete_original
            _args.override_chunked_subfolder,  # use_chunked_subfolder
        ]
        args.append(tuple(tmp_args))
    logger.info("Starting file preprocessing...")
    fifo_pool(_args.n_processes, _preprocess_daily_dumps, args, logging_level=loglevel)  # type: ignore

    # Make a final snapshot of the local storage
    local_files = (
        check_local_storage(dump_files_root_folder, force_sha1_check=False)
        .query("platform == @platform and version == @version")
        .copy()
    )

    # Compute the number of file present and the number of files correctly chunked
    num_sha1_present = local_files["dump_sha1_path"].notnull().sum()
    num_sha1_dump_valid = local_files["dump_sha1_check"].sum()
    num_chunks_present = local_files["chunked_folder"].notnull().sum()
    num_files_correctly_chunked = local_files["chunked_success"].sum()

    logger.info("Done with the preprocessing, here is the summary:")
    logger.info("Found %d sha1 files, %d of which are valid", num_sha1_present, num_sha1_dump_valid)
    logger.info(
        "Found %d chunked folders, %d of which are correctly chunked", num_chunks_present, num_files_correctly_chunked
    )

    par_f_out = os.path.join(_args.dump_files_folder, "parameters.yaml")
    logger.info("Saving the parameters to the output folder in %s", par_f_out)
    par_dump = _args.model_dump()
    if isinstance(par_dump["chunk_format"], TDB_chunkFormat):
        par_dump["chunk_format"] = par_dump["chunk_format"].value
    with open(par_f_out, "w") as f:
        f.write(yaml.dump(par_dump))

    return local_files


def _preprocess_daily_dumps(
    url: str,
    dump_file: Union[str, Path],
    do_download: bool = True,
    check_sha1: bool = True,
    force_check_sha1: bool = False,
    BUF_SIZE: int = 65536,
    raise_on_error: bool = True,
    do_chunking: bool = True,
    chunk_size: int = 1000000,
    platforms_to_exclude: Union[List[str], None] = None,
    output_format: str = TDB_chunkFormat.parquet,
    delete_original: bool = False,
    use_chunked_subfolder: str = CHUNKED_FILES_SUBFOLDER_NAME,
) -> Union[Tuple[str, str], Tuple[None, None]]:
    """Function to preprocess the daily dumps.
    It will download the files, check the sha1 and eventually chunk them.

    Parameters
    ----------
    url : str
        The URL of the file to download.
    dump_file : str
        The output file name.
    do_download : bool, optional
        Whether to download the file, by default True.
    check_sha1 : bool, optional
        Whether to check the sha1 of the file, by default True.
    force_check_sha1 : bool, optional
        Whether to force the sha1 check of the files already present, by default False.
    BUF_SIZE : int, optional
        The buffer size in bits to use when computing sha1, by default 65536.
        Do not change this unless you know what you are doing.
    raise_on_error : bool, optional
        Whether to raise an exception if the download fails, by default True.
    do_chunking : bool, optional
        Whether to chunk the file, by default True.
    chunk_size : int, optional
        The size of the chunks in number of SoR, by default 1000000.
        Do not change this unless you know what you are doing.
    platforms_to_exclude : List[str], optional
        The platforms to exclude from the chunking, by default None.
    output_format : str, optional
        The output format of the chunks, by default 'parquet'.
    delete_original : bool, optional
        Whether to delete the original file after chunking, by default False.
    use_chunked_subfolder : str, optional
        The name of the subfolder where to save the chunked files, by default
        :attr:`dsa_tdb.types.CHUNKED_FILES_SUBFOLDER_NAME`.

    Returns
    -------
    Tuple[str,str], Tuple[None,None]
        The path of the downloaded file and the path of the chunked folder.
    """

    if isinstance(dump_file, str):
        dump_file = Path(dump_file)

    # Download daily dump files if needed
    local_filename = dump_file
    if do_download:
        local_filename = download_file(
            url=url, local_filename=dump_file, check_sha1=check_sha1, BUF_SIZE=BUF_SIZE, raise_on_error=raise_on_error
        )
        if local_filename is None:
            message = "WARNING: empty filename when trying to download %s" % url
            logger.warning(message)
            return None, None
        else:
            local_filename = Path(local_filename)
    elif force_check_sha1:
        # This is the enforced sha1 checksum
        # When using the prepare_daily_dumps we will already have the do_download flagged, so we end up here only if
        # the file was not to download (so already validated locally and from web).
        # If dump file exists, check it and raise depending on flag/url, otherwise just warn and go on (this is to
        # allow users to use the delete flag and just store the sha1 for local storage).
        if os.path.isfile(dump_file):
            try:
                check_sha1_sum(dump_file, dump_file.with_suffix(DAILY_FILE_CHECKSUM_EXTENSION))
            except AssertionError:
                if raise_on_error:
                    # Directly raise as per user instructions
                    raise Exception("Local SHA1 check failed for %s" % dump_file)
                else:
                    # User requested to fallback to download:
                    # We try to retrieve the file and mark it as a to-chunk: this is automatically fixed as it will check the chunk's sha1 against the newly downoaded ones.
                    # We raise if either: i) also the download fails, ii) no file is available anymore (no url)
                    if url:
                        logger.warning(
                            "Local SHA1 check failed for %s, but no raise flag provided: will try to re-download from %s",
                            dump_file,
                            url,
                        )
                        local_filename = download_file(
                            url=url, local_filename=dump_file, check_sha1=True, BUF_SIZE=BUF_SIZE, raise_on_error=True
                        )
                        local_filename = Path(local_filename)
                        # Note: we will not override here the do_chunk flag because it is globally set from the `prepare_daily_dumps` args,
                        # We will later test it in the chunking section (if and only if the `do_chunking` flag is set) whether a sha1 mismatch is in place (if file was already downloaded and chunked)
                        # or if the chunk flag is on and the folder is still missing.
                    else:
                        raise RuntimeError(
                            "Local SHA1 check failed for %s and no raise flag provided but no url is available anymore: fix your local storage!"
                            % dump_file
                        )
        else:
            # The file was not there (for example for a delete original policies), just let the user know
            logger.info("Local SHA1 check skipped for %s because file was missing: skipping!", dump_file)

    chunked_folder_base = os.path.join(os.path.dirname(local_filename), use_chunked_subfolder)
    chunked_folder = os.path.join(
        chunked_folder_base, os.path.basename(local_filename).replace(DAILY_FILE_EXTENSION, "")
    )
    complete_file = os.path.join(chunked_folder, CHUNKED_FILE_SUCCESS_NAME)
    dump_sha1 = read_sha1_file(local_filename.with_suffix(DAILY_FILE_CHECKSUM_EXTENSION))
    # Eventually chunk them if needed while forcing the check of the chunked sha1
    # At this point the local dumps are validated and we rely on them to check the sha1
    if do_chunking:
        if os.path.isdir(chunked_folder) and os.path.isfile(complete_file):
            chunk_sha1 = read_sha1_file(complete_file)
            if chunk_sha1 == dump_sha1:
                logger.info(
                    "Chunked folder %s already exists and sha1 is correct, skipping the chunking" % chunked_folder
                )
                do_chunking = False
            else:
                logger.warning("Chunked folder %s already exists but sha1 is wrong, re-chunking!" % chunked_folder)
        else:
            # I just needed to chunk
            logger.debug("No complete file found in %s, doing the chunking" % (chunked_folder))
    else:
        logger.debug("No chunking to do for %s, skipping..." % (local_filename))

    if do_chunking:
        logger.debug("Chunking %s to %s..." % (local_filename, chunked_folder))
        # Make sure to start from a clean sheet
        if os.path.isdir(chunked_folder):
            shutil.rmtree(chunked_folder)
        chunked_folder = chunkFile(
            csv_zip_in=local_filename,
            folder_out=str(Path(chunked_folder).parent),
            platforms_to_exclude=platforms_to_exclude,
            header_in=True,
            header_out=True,
            chunk_size=chunk_size,
            overwrite=True,
            extension=DAILY_FILE_EXTENSION,
            out_format=output_format,
            delete_original=delete_original,
        )

    # Delete irrespective of do_chunking depending on deletion policy
    if delete_original and os.path.isfile(local_filename):
        # Enter only if the file was not chunked and we want to delete it anyway
        logger.info("Deleting original file %s as it was a leftover", local_filename)
        os.remove(local_filename)

    return str(local_filename), str(chunked_folder)


def get_platform_uid(platform: str) -> str:
    # Dynamically fetch the platforms from the webpage
    platforms = fetch_available_platforms()
    if platform not in platforms:
        raise ValueError(f"Platform {platform} not found, check the available platforms:\n{list(platforms.keys())}")
    return platforms[platform]


def get_platform_full_name(platform: str) -> str:
    # Dynamically fetch the platforms from the webpage
    platforms = fetch_available_platforms(return_name=True)
    if platform not in platforms:
        raise ValueError(
            f"Platform {platform} not found, check the available platforms:\n"
            + "\n".join([f"{k}: {v}" for k, v in platforms.items()])
            + "\n"
        )
    return platforms[platform]


def get_platforms_fullnames(platforms: Union[List[str], None]) -> Union[List[str], None]:
    if platforms is None:
        return None
    platforms_fullname = []
    for platform in platforms:
        platforms_fullname.append(get_platform_full_name(platform))
    return platforms_fullname
