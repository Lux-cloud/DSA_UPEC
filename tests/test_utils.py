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
import pytest

from dsa_tdb.utils import territories2label, vals2arra


def test_vals2arra():
    # Test case 1: Empty string
    assert vals2arra("") == []

    # Test case 2: Single value
    assert vals2arra("AAA") == ["AAA"]

    # Test case 3: Multiple values
    assert vals2arra("[AAA,BBB]") == ["AAA", "BBB"]

    # Test case 4: Multiple values with spaces and double quotes
    assert vals2arra('["AAA","BBB"]') == ["AAA", "BBB"]

    # Test case 5: Input is not a string
    assert vals2arra(None) == []

    # Test case 6: Input is '<NA>'
    assert vals2arra("<NA>") == []

    # Test case 7: Input has missing closing bracket
    with pytest.raises(ValueError):
        vals2arra("[AAA,BBB")

    # Test case 8: as_set=True
    assert vals2arra("[AAA,BBB]", as_set=True) == {"AAA", "BBB"}


def test_territories2label():
    # Test case 1: Single value
    assert territories2label("[IT,ES]") == "ES_IT"

    # Test case 2: Multiple values
    assert territories2label("[IT,ES,FR]") == "ES_FR_IT"

    # Test case 3: Input is not a string
    assert territories2label("") == ""
