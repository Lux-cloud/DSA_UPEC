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
from flask_caching.backends.rediscache import RedisCache

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_filter_cache",
    "CACHE_REDIS_URL": "redis://redis-docker-service:6379/0",
}
EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_explore_cache",
    "CACHE_REDIS_URL": "redis://redis-docker-service:6379/0",
}
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_metadata_cache",
    "CACHE_REDIS_URL": "redis://redis-docker-service:6379/0",
}
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_data_cache",
    "CACHE_REDIS_URL": "redis://redis-docker-service:6379/0",
}
# Following line is not production-ready! Use your own secret key
# https://superset.apache.org/docs/configuration/configuring-superset/#specifying-a-secret_key
SECRET_KEY = "uBRE0GnkLTS5c9PfDxRRcLIi6II2UuN/gCedUjbx/yhWSd9s37qRvzAk"  # nosec B105
RATELIMIT_STORAGE_URI = "redis://redis-docker-service:6379"
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ENABLE_SUPERSET_META_DB": True,
}


class CeleryConfig:
    broker_url = "redis://redis-docker-service:6379/0"
    imports = (
        "superset.sql_lab",
        "superset.tasks.scheduler",
    )
    result_backend = "redis://redis-docker-service:6379/0"
    worker_prefetch_multiplier = 10
    task_acks_late = True
    task_annotations = {
        "sql_lab.get_sql_results": {
            "rate_limit": "100/s",
        },
    }


CELERY_CONFIG = CeleryConfig

RESULTS_BACKEND = RedisCache(host="redis-docker-service", port=6379, key_prefix="superset_results")
PREVENT_UNSAFE_DB_CONNECTIONS = False


# Let users run queries over large subsets
SUPERSET_WEBSERVER_TIMEOUT = 600
SQL_MAX_ROW = 1000000000
DISPLAY_MAX_ROW = SQL_MAX_ROW
