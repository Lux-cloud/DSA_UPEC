Data download
=============

.. toctree::
   :maxdepth: 4


The `dsa_tdb` package relies on the raw data sources made available on the `DSA Transparency Database <https://transparency.dsa.ec.europa.eu/>`_
webpage as well as some pre-processed data.

To run customized queries to filter the data and  aggregate them, the package currently downloads the dumps containing the daily CSV dumps
of the database and process them in parquet files.

We also offer a pre-processed version of the raw CSV data in parquet format as well as pre-made aggregations.
While the former are useful to skip the (long and resource-intensive) pre-processing workload to have parquets on your machine,
the latter are useful to quickly access pre-made aggregations and to quickly start woring with the Superset-dashboard (see :ref:`superset-dashboard`).
We are working to have the package to automatically download these new versions of the data and use them by default.
In the meanwhile, please refer to the information below to understand the data sources and how to download them.

.. note::
   **TLDR;**
   For automated (but long pre-process running) data download and preprocessing use the :py:mod:`dsa_tdb.cli` CLI.
   If you want to manually download the pre-processed parquets, see the :ref:`raw-data` section below.
   If you want to manually download the pre-made aggregations, see the :ref:`aggregations` section below.

.. _raw-data:

Raw data
--------

There are two versions of the raw data:
1. **Daily CSV dumps**: These are the raw data dumps made available on the DSA Transparency Database
`data download page <https://transparency.dsa.ec.europa.eu/explore-data/download>`_. They are updated daily and contain the latest data.
The package automatically downloads these dumps and processes them into valid CSVs or parquet files with the :py:mod:`dsa_tdb.cli`
or :py:mod:`dsa_tdb.etl` modules.
2. **Pre-processed parquet**: These are the pre-processed data made available on a separate S3 bucket.
They are updated daily and contain all the data of the CSV daily dumps in a data-science friendly format.
We are working to make the package able to automatically download these data. In the meanwhile, the preferred
way to have them on the local machine is to download them manually from the S3 bucket using the following command:

.. code-block::
   :caption: Download September 2023 data

   for day in `seq 1 30`; do
      tmp_fname=sor-global-2023-09-`printf "%02d" $day`-full.parquet.zip;
      wget -O data/$tmp_fname https://d3vax7phxnku8l.cloudfront.net/raw/pqt/data/tdb_data/global___full/daily_dumps_chunked/$tmp_fname;
      # Optional: check sha1
      # wget -O data/"$tmp_fname".sha1 https://d3vax7phxnku8l.cloudfront.net/raw/pqt/data/tdb_data/global___full/daily_dumps_chunked/"$tmp_fname".sha1;
      # cd data
      # sha1sum -c "$tmp_fname".sha1;
      # cd ..
      unzip -d /data/tdb_data/ data/"tmp_fname"
   done

.. note::
   The zipfile already contains the full relative path from the root folder of the data.
   For instance, the latter is `/data/tdb_data` in the docker image provided.
   So you can directly unzip the file passing `-d root/folder/path` to the unzip command and you can start using the files with the package.

.. warning::
   The pipeline creating the pre-processed is still in experimental mode and it's not guaranteed to be up-to-date with the raw data.
   We aim at providing this on a daily basis, but hiccups are possible.


.. _aggregations:

Aggregated data
---------------

We provide aggregated data in both CSV and parquet format.
The data are provided in two versions: a limited and complete one.
These aggregated views report the overall count of Statements of Reasons (SoRs) that share the same value in a specific subset of the categorical features.
See `here <https://transparency.dsa.ec.europa.eu/page/api-documentation>`_ for more information regarding the SoRs' schema.

In aggregating the data:

- The `created_at` datetime field is truncated (floored) at the day level.
- All the array fields like `content_type` or `decision_visibility` are pivoted, that is, for each one of their possible values a new column is created and filled with a boolean, indicating whether the SoR has that value or not.
- The `territorial_scope` field is not pivoted but left as a string-representation of the array.
- An average restriction duration in days is computed for each group of Statements of Reasons, coalescing on the `end_date_visibility_restriction`, `end_date_account_restriction`, `end_date_service_restriction`, and, `end_date_monetary_restriction` as the end date and using the `application_date` as the starting one.
- The count of SoRs is reported in the `count` column.

The data is then aggregated on the features of interest, and the counts of SoRs are computed for each values combination.
We provide the zip, dates and configuration files in the bucket as follows:

.. code-block::
   :linenos:

   ./aggregated-{simple,complete}-YYYY-MM.{parquet,csv}.zip
   ./aggregated-version.configuration.yaml
   ./aggregated-version.dates_files.csv

To download and unzip them you can use the following:

.. code-block::
   :caption: Download 2024 simple aggregated data in parquet format

   for month in `seq 1 12`; do
      tmp_fname=aggregated-simple-2024-`printf "%02d" $month`-01-parquet.zip;
      wget -O data/$tmp_fname https://d3vax7phxnku8l.cloudfront.net/agg/pqt/data/tdb_data/global___full/aggregations/$tmp_fname;

      # Optional: download configuration and files dates files as well
      # wget -O data/"$tmp_fname".configuration.yaml https://d3vax7phxnku8l.cloudfront.net/agg/pqt/data/tdb_data/global___full/aggregations/"$tmp_fname".configuration.yaml;
      # wget -O data/"$tmp_fname".dates_files.csv https://d3vax7phxnku8l.cloudfront.net/agg/pqt/data/tdb_data/global___full/aggregations/"$tmp_fname".dates_files.csv;

      # Optional: check sha1
      # wget -O data/"$tmp_fname".sha1 https://d3vax7phxnku8l.cloudfront.net/agg/pqt/data/tdb_data/global___full/aggregations/"$tmp_fname".sha1;
      # cd data/;
      # sha1sum -c "$tmp_fname".sha1;

      unzip -d /data/tdb_data/global___full/aggregations/ data/"$tmp_fname"
   done

Once, unzipped, the output folder structure is as follows:

.. code-block::
   :linenos:

   ./aggregated-{simple,complete}.parquet
   -- /created_at_month=YYY-MM-DD
       -- /part-0000.{parquet,csv.gz}
   ./aggregated-version.configuration.yaml
   ./aggregated-version.dates_files.csv

.. note::
   The aggregated data are updated on a daily basis using an "append" fashion.
   You should then synchronize the data to have updated figures (especially for the most recent days).

.. note::
   The configuration and dates files files refer to all the data, irrespectively of the month.

.. warning::
   The aggregated zip files **do not contain** the relative path from the root data folder.
   Therefore, to use them in the Superset dashboard, they have to be extracted to the default aggregations
   folder (see :ref:`superset-dashboard` for more details).
   The latter is `/data/tdb_data/global___full/aggregations/` in the docker image provided.

.. warning::
   The aggregated data provisioning is still in an experimentatl phase and are not guaranteed to be up-to-date with the raw data.
    We aim at providing this on a daily basis, but hiccups are possible.

Versions
^^^^^^^^

**Limited version**
   This version contains the SoRs data aggregated in a way to reproduce the `website version of the dashboard <https://transparency.dsa.ec.europa.eu/dashboard>`_.
   That is, SoRs are grouped by:

   - automated_decision
   - automated_detection
   - category
   - content_type (pivoted, see below)
   - decision_account
   - decision_ground
   - decision_monetary
   - decision_provision
   - decision_visibility (pivoted)
   - platform_name
   - created_at (truncated to day)
   - source_type

.. note::
   The `content_type` field is pivoted and we also add the :py:attr:`dsa_tdb.types.CONTENT_TYPE_OTHER_NORMALIZATION` additional values.
   These values are meant to map recurring values found in the `content_type_other` field to standardized values.
   If you find other recurring values in the `content_type_other` field, please let us know opening a PR or an issue on the `gitlab code page <https://code.europa.eu/dsa/transparency-database/dsa-tdb>`_.

**Complete version**
   This version contains the SoRs data aggregated over all the categorical fields available,
   that is, all of the above plus:

   - territorial_scope (not pivoted, see below)
   - incompatible_content_illegal
   - content_language
   - application_date (truncated to day)
   - content_date (truncated to day)
   - restriction_duration (the median restriction duration, see below)

.. note::
   The `territorial_scope` field is not pivoted, it contains the values of the `territorial_scope` field concatenated with a `_` (e.g., `AT_FR_ES`).
   We have three special values for `territorial_scope`: `EU`, `EEA`, and `EEA_no_IS`:
   
   - `EU`` is reported when all the EU member states are reported.
   - `EEA` is reported when all the EEA countries, that is `EU`  countries plus Iceland (`IS`) and Norway (`NO`) are reported.
   - `EEA_no_IS` is reported when all the EEA countries except Iceland are reported (which is common).

   These values are meant to map recurring values found in the `territorial_scope` field to standardized values.
   If you find other recurring values in the `territorial_scope` field, please let us know opening a PR or an issue on the `gitlab code page <https://code.europa.eu/dsa/transparency-database/dsa-tdb>`_.

.. note::
   The `restriction_duration` field is computed as the number of days between the `application_date` (the day on which the platform reports having started the penalty)
   and the first, non-null value of either `end_date_visibility_restriction`, `end_date_account_restriction`, `end_date_service_restriction`, `end_date_monetary_restriction`.
   If no end date is specified, the `restriction_duration` is set to `null` (`None` in Python).
   See :py:attr:`dsa_tdb.etl.loadDataset` for more details.

Syncing
^^^^^^^
The automatic handling of the parquet raw file and the aggregated ones will be implemented in the next releases.
For the time being you can use the commands above to daily download the new files being published.

Note that there are statements created in one day that might end in the following or previous day's dump.
So at the beginning of one month you should check for the sha1 of the previous month for a couples of days more to check that
the aggregate did not change.


.. _superset-dashboard:

Superset-dashboard
^^^^^^^^^^^^^^^^^^
The aggregated data **in the complete parquet format** can be used to feed the Superset dashboard shipped with the official docker image.
To launch it, mount the root data folder of your host system (we assume `/home/user/my_tdb_data` in the following) to the `/data` folder in the container.
To do so:

- Download the complete, parquet aggregates of the months you are interested in.
- Create the `/home/user/my_tdb_data/tdb_data/global___full/aggregations/` folder, if not existing.
- Extract the content of the aggregated files in this new folder (for example by adding `-d /home/user/my_tdb_data/tdb_data/global___full/aggregations/` to the unzip command).
- Copy the `docker-compose.yml` file from the latest release and launch the `compose up` command with:

.. code-block::
   :caption: Launch the docker-compose with local directory mounted to serve dashboard data.
   :linenos:

   DOCKER_DATA_DIR=/home/user/my_tdb_data PODMAN_USERNS=keep-id:uid=1000 podman-compose up

- On the first startup the image will build the dashboard and import the data. After a couple of minutes you can connect to the default superset dashboard at `http://localhost:8088` (default user and password `admin`).



