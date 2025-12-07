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
try:
    from celery import Celery
except ImportError:
    raise ImportError(
        "Please install the [webapp] extra by running `pip install dsa-tdb[webapp]` or `poetry add dsa-tdb[webapp] to use the celery."
    )

from dsa_tdb.types import CELERY_TASK_QUEUE

app = Celery("dsa_tdb", broker="redis://redis-docker-service:6379/0", include=["app.tasks"])

# Optional configuration, e.g., result backend
app.conf.update(
    result_expires=3600,
    worker_pool_restarts=True,
    task_routes={
        "app.tasks.*": {"queue": CELERY_TASK_QUEUE},
    },
)

if __name__ == "__main__":
    app.start()
