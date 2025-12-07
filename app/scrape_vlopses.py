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
import re

import bs4
import pandas as pd
import requests

r = requests.get("https://digital-strategy.ec.europa.eu/en/policies/list-designated-vlops-and-vloses", timeout=10)

soup = bs4.BeautifulSoup(r.text, "html5lib")
vlopses = []
for table in soup.select("main div.content_main_wrapper div.ecl .ecl-container"):
    vlopse_dict = {}
    for row in table.select(".ecl-row"):
        try:
            header = row.select_one(".ecl-col-4").text.strip().strip("*")
            value = (
                row.select_one(".ecl-col-8")
                .text.strip()
                .replace("[notranslate]", "")
                .replace("[/notranslate]", "")
                .strip("*")
            )
            if header and value:
                vlopse_dict[header] = value
        except AttributeError:
            pass
    if vlopse_dict:
        vlopses.append(vlopse_dict)

# Sanitizing number of users
for idx, vlopse in enumerate(vlopses):
    average_nb_users = vlopse["Average monthly active users in millions"]
    try:
        average_nb_users = float(average_nb_users)
    except ValueError:
        average_nb_users = max([float(x[0]) for x in re.findall(r"([0-9]+(.[0-9]+)?)", average_nb_users)])
    vlopses[idx]["Average monthly active users in millions (sanitized)"] = average_nb_users

df = pd.DataFrame(vlopses)
df.to_parquet("vlopses.parquet")
