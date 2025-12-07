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
__license__ = "EUPLv1.2"
__version__ = "0.6.13"
__maintainer__ = "Enrico Ubaldi"
__email__ = "enrico.ubaldi@ec.europa.eu"
__homepage__ = "https://code.europa.eu/dsa/transparency-database/dsa-tdb"
__status__ = "Development"

__doc__ = """
The `dsa_tdb` module documentation.

The `dsa_tdb` module provides a set of tools to interact with the DSA Transparency Database (TDB) data.
It provides a set of classes and functions to fetch, extract, transform, filter and load data from the TDB.

It intrnally uses `pyspark` to handle the data at scale even on regular computers and can be easily introduced
in pipelines using `pandas` or other data manipulation libraries.
"""

import logging

logging.basicConfig(format="%(asctime)s:%(levelname)s:%(message)s", datefmt="%Y-%m-%d,%H:%M:%S")
logging.getLogger(__name__).addHandler(logging.NullHandler())

import dsa_tdb.cli as cli  # noqa: E402
import dsa_tdb.etl as etl  # noqa: E402
import dsa_tdb.fetch as fetch  # noqa: E402
import dsa_tdb.types as types  # noqa: E402
import dsa_tdb.utils as utils  # noqa: E402
from dsa_tdb.core import TDB_DataFrame as TDB_DataFrame  # noqa: E402

__all__ = ["cli", "etl", "fetch", "types", "utils", "TDB_DataFrame"]
