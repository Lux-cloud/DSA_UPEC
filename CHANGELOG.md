<!--
This file is part of dsa_tdb (see https://code.europa.eu/dsa/transparency-database/dsa-tdb).

SPDX-License-Identifier: EUPLv1.2
Copyright (C) 2024 European Union

This program is free software: you can redistribute it and/or modify
it under the terms of the EUROPEAN UNION PUBLIC LICENCE v. 1.2 as
published by the European Union.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
EUROPEAN UNION PUBLIC LICENCE v. 1.2 for further details.

You should have received a copy of the EUROPEAN UNION PUBLIC LICENCE v. 1.2.
along with this program.

If not, see < https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12 >.-->
# Changelog

## Breaking Changes

* [0.6.13] The `preprocess` cli tool will properly raise on sha1-validation/general download error (previously the exception was wrapped and program would have exited with a 0 return state).
* [0.6.5] Switched to new names for the aggregates: `aggregated-complete` and `aggregated-simple`.
* [0.6.5] Fixed an error in the default aggregation provided and switched to a truly full one.

## [0.6.13] - 2025-11-18
* Exposed the raise on sha1-check error for the preprocessing cli. Closes #10
* Implemented a no-raise on sha1-check flag to try to rebuild dumps and/or chunked folders. Closes #10
* Fixed the web sha1 check against local storage for changing files. Closes #9
* The `preprocess` step will now properly chunk all the missing / inconsistent chunked folders.
* Porting the MR !12 

## [0.6.8] - 2025-07-10
* Update of download pipeline following migration to API v2.

## [0.6.6] - 2025-02-28
* Using the same spark master configuration of the package in the Docker image. Closes #3

## [0.6.5] - 2025-02-17

* [Breaking Change] Changed the names of the default aggregates to be `aggregated-{simple,complete}.parquet` and to be in the `aggregations` subfolder of the platform/version directory: `/data/tdb_data/global___full/aggregations/aggregated-complete.parquet'
* [Breaking Change] Fixed the default aggregation not to account for the `content_type_other` column. To reflect this in the old aggregates do:

```python
df_agg_old = spark.read.parquet('/path/to/old/aggregated.parquet')
df_agg_complete_new = (
     df_agg_old
     .groupby(*[c for c in df_agg_old.columns if c not in ['count', 'content_type_other']])
     .agg(F.sum('count').alias('count'))
)
df_agg_complete_new.write.parquet('/path/to/new/aggregate.parquet')
```
* Improved default dashboard.
* Switched to a single spark master in the docker-compose template.
* Using relative path in the dates file for filtering/aggregation to make the installation portable.
