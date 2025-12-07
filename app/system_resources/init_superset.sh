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
set -ex

sup_cmd=${HOME}/.local/share/pipx/venvs/apache-superset/bin/superset

check_file=${HOME}/.superset/SUPERSET_INIT_DONE

# Check if SUPERSET_INIT_DONE file in the home directory does not exists
if [ ! -f $check_file ]; then
    echo "SUPERSET_INIT_DONE file not found, initializing Superset..."
    # TODO move to netclt
    sleep 10; # wait for the database to be ready
    # Initialize Superset database and dashboards
    $sup_cmd db upgrade;
    $sup_cmd fab create-admin --username admin --firstname admin --lastname user --email admin@example.com --password admin;
    $sup_cmd init;
    $sup_cmd import-directory ${HOME}/app/system_resources/superset_exports/
    touch $check_file
fi

# Serve superset with gunicorn instead of built-in development server
# $sup_cmd run -h 0.0.0.0 -p $SUPERSET_PORT --with-threads
${HOME}/.local/share/pipx/venvs/apache-superset/bin/gunicorn -w 10       -k gevent       --worker-connections 1000       --timeout 600       -b  0.0.0.0:${SUPERSET_PORT}       --limit-request-line 0       --limit-request-field_size 0       "superset.app:create_app()"
