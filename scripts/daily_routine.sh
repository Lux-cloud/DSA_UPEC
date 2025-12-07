#!/bin/bash
##
## This file is part of dsa_tdb (see https://code.europa.eu/dsa/transparency-database/dsa-tdb).
##
## SPDX-License-Identifier: EUPLv1.2
## Copyright (C) 2024 European Union
##
## This program is free software: you can redistribute it and/or modify
## it under the terms of the EUROPEAN UNION PUBLIC LICENCE v. 1.2 as
## published by the European Union.
## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## EUROPEAN UNION PUBLIC LICENCE v. 1.2 for further details.
##
## You should have received a copy of the EUROPEAN UNION PUBLIC LICENCE v. 1.2.
## along with this program.
##
## If not, see < https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12 >.##
set -e

# Usage: bash ./scripts/daily_routine.sh --platform global --version full --numprocs 12 --root_dir data/tdb_data --start 2023-09-01 --end 2023-09-30
# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --platform)
            platform="$2"
            shift 2
            ;;
        --version)
            version="$2"
            shift 2
            ;;
        --numprocs)
            numprocs="$2"
            shift 2
            ;;
        --root_dir)
            root_dir="$2"
            shift 2
            ;;
        --start)
            start="$2"
            shift 2
            ;;
        --end)
            end="$2"
            shift 2
            ;;
        --delete_chunks)
            delete_chunks=true
            shift
            ;;
        *)
            echo "Invalid argument: $1"
            exit 1
            ;;
    esac
done

# Set default values if not provided
platform=${platform:-global}
version=${version:-full}
numprocs=${numprocs:-$(($(nproc) < 12 ? $(nproc) : 12))}
root_dir=${root_dir:-data/tdb_data}
start=${start:-2023-09-01}
end=${end:-$(date +%Y-%m-%d)}
# whether or not to delete the files
delete_chunks=${delete_chunks:-false}
# ===

platform=$(python -c "import dsa_tdb as t; print(t.utils.sanitize_platform_name('${platform}'))")

basedir="${root_dir}/${platform}___${version}"
# derived from data directory structure
logFile="${basedir}/log"

# Ensure log folder exists
mkdir -p $(dirname $logFile)

# Concat the string to make the output file name
outFileAgg="$basedir/aggregations/aggregated-$platform-$version.{}"

# Create a log file to append everything to it
echo "[$(date +'%Y-%m-%dT%H:%M:%S%:z')] STARTING IMPORT PROCESS" | tee -a "${logFile}"

while (( $(date -d "${start}" +%s) <= $(date -d "${end}" +%s) )); do
    current_end=$(date -d "${start} +1month -1day" +%Y-%m-%d)
    if [[ "$current_end" > "$end" ]]; then
	    current_end=$end
    fi
    echo
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%:z')] current date range: ${start} - ${current_end}" | tee -a "${logFile}"

    # Download the latest dumps
    dsa-tdb-cli preprocess \
                -o $root_dir \
                -p $platform -v $version \
                -i "${start}" \
                -f "${current_end}" \
                --loglevel INFO \
                -n $numprocs --chunk_size 1000000 -d --format parquet 2>&1 | tee -a "${logFile}"

    # Do the aggregation keeping the time needed to run it
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%:z')] download and chunking done, starting aggregation" | tee -a "${logFile}"
    tic=$(date +%s)
    dsa-tdb-cli aggregate \
            -d $root_dir \
            -p $platform -v $version \
            -o $outFileAgg \
            -i "${start}" \
            -f "${current_end}" \
            -c config_aggregation_simple.yaml \
            -n 4 2>&1 | tee -a "${logFile}"
    toc=$(date +%s)
    echo "[$(date +'%Y-%m-%dT%H:%M:%S%:z')] Aggregation took $((toc - tic)) seconds" | tee -a "${logFile}"

    if $delete_chunks; then
        # Delete chunk files (keep folder and COMPLETE file to avoid downloading the files once again at next routine run)
        rm -r ${basedir}/daily_dumps_chunked/*/*.parquet
    fi

    start=$(date -d "${current_end} +1day" +%Y-%m-%d)
done
echo "[$(date +'%Y-%m-%dT%H:%M:%S%:z')] DONE WITH IMPORT PROCESS" | tee -a "${logFile}"
