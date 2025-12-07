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
import pandas as pd

from dsa_tdb.types import TDB_columnsFull, TDB_datetimeColumns, datetime_format


def test_loadFile():
    # Test case 1: Loading a file with corrupted dates raises an error
    # TODO: restore the tests to use the spark backend
    # with pytest.raises(ValueError):
    #     loadFile(['tests/test_data/test_sor_wrong_date.csv'],
    #              columns_datetime=[TDB_datetimeColumns.created_at,
    #                                TDB_datetimeColumns.application_date,
    #                                TDB_datetimeColumns.content_date])

    # Test case 2: Loading a file should load correct dates
    tmp_dtype = {k: "str" for k in TDB_columnsFull._member_names_ if k not in TDB_datetimeColumns._member_names_}
    assert (
        pd.read_csv(
            "tests/test_data/test_sor_date_parse.csv",
            parse_dates=TDB_datetimeColumns._member_names_,
            dtype=tmp_dtype,
            date_format=datetime_format,
        )[TDB_datetimeColumns.content_date].dt.day.iloc[0]
        > 0
    )
