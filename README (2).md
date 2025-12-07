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
DSA Transparency database tools
===============================

`dsa-tdb` provides a set of tools to work with daily or total dumps coming from the [DSA Transparency Database](https://transparency.dsa.ec.europa.eu/).


## Requirements

The Transparency database is a large dataset. As of October 2024, you will require a minimum of:

* 4.1TB disk space to store the daily dump files as downloaded from the DSA Transparency Database website.
* 500GB to store the daily dumps in a "chunked" form (see documentation below).
* 1GB to store the aggregated dataset with the default aggregation configuration.

Overall, the data throughput is in the range of 5 to 10GB per day (meaning you should have as a bare minimum 5GB of free disk space per daily dump you want to process).

The `dsa-tdb` Python package aims at making working with such a large dataset easier by providing convenience functions to convert from the raw dumps to more efficient data storage as well as scripts to handle the conversion over a sliding time window to reduce the disk space requirements (see documentation below).


## Installation

### With Docker/podman (recommended)


1. [Install `Docker`](https://docs.docker.com/engine/install/) or [`podman`](https://podman.io/docs/installation) (recommended) on your machine (if using `podman`, replace `docker` with `podman` in the following commands or install the [`podman-docker`](https://github.com/containers/podman) extension to have compatible CLI). You can also download the Desktop versions for the two ([here for Docker](https://www.docker.com/products/docker-desktop/) and [here for podman](https://podman-desktop.io/)).

2. [optional] If you need to edit some of the build-time variables, you can clone/download the zip of the repository, cd into it and use the provided `docker-compose.yml` file.
The build allows to customize:
    - The user name, id and group id of the container (defaults are `user`, `uid=1000` and `gid=1000`)
    - The ports exposed (see the **Ports** section below).

    You can customize them:
    - Directly in the [`docker-compose`](docker-compose.yml) file, building with `podman-compose build`.
    - Or at buildtime with `--build-arg`. For instance, this sets the user name, id and group id to the ones of the host user:
    ```bash
    docker build --build-arg DOCKER_USER=$(id -un) --build-arg DOCKER_USER_ID=$(id -u) --build-arg DOCKER_GROUP_ID=$(id -g) -t localhost/dsa-tdb-nb .
    ```

3. Build the container using `docker-compose`. After customizing the values in the `docker-compose.yaml`, simply do a `docker-compose build` (required once to build the image). If using `podman-compose`, please upgrade it at least to version 1.2.0 with `pip install --user --upgrade podman-compose`.

> **_NOTE_** If using podman, be sure to have version podman >=4.3.0. Also, always add the `PODMAN_USERNS=keep-id:uid=1000` in front of all the `podman run` or `podman-compose up -d` commands, where `1000` is the container's user id (the default one, change it if you edited the build).
> In alternative, you can add the `--userns=keep-id:uid:1000` to the podman or set an environment variable (`PODMAN_USERNS=keep-id:uid:1000`). In the desktop application, you can set it in the *Security->Specify user namespace to use* tab in the *Create container* interface. This is needed to have the mounted folders being writeable by the container's user.

4. A `/cache` directory can be mounted and it will be the `spark.local.dir`, that is, where Spark writes its cache. To do so, uncomment the matching line in `docker-compose.yml` file. You should mount there a folder located on a fast volume with a lot of free space (~300 GB per month analyzed if you want to analyze the full database).
5. To start the container interactively, just use a
```bash
docker-compose up
```

> **_NOTE__** The default user in the Docker container is `user` with same user and group ids `1000`. The latter can be changed to match your user name and id specifying `DOCKER_USER=your_user` and `DOCKER_USER_ID=1234` when building, as outlined above.

**Ports** The docker container will expose these ports (edit the `docker-compose.yml` file to change the mapping):
- `8765` the [Jupyter lab](https://jupyter.org/) home
- `4040` the [Spark](https://spark.apache.org/) status page for the user's application
- `5555` the [Celery's flower](https://flower.readthedocs.io/en/latest/) dashboard to check the status of your  tasks
- `8000` the [Fastapi webapp](https://fastapi.tiangolo.com/) (visit the [docs](http://localhost:8000/docs) to see the API usage)
- `8088` the [Superset](https://superset.apache.org/) instance. Default credentials: `admin`/`admin`.
- `8080` the Spark master and thrift server web uis for advanced users.

To stop the containers, just use `docker-compose down`.

> **__NOTE__** You can change the mount point of the `data` folder when running `docker-compose up`  with the `DOCKER_DATA_DIR` environment variable, to point to a data folder outside of the Git repository. Example: `DOCKER_DATA_DIR=/path/to/data/ podman-compose up`

### With pip / poetry

We ship a python package providing the command line interface. You can install it with:
- **pip**: `pip install dsa-tdb --index-url https://code.europa.eu/api/v4/projects/943/packages/pypi/simple`
- **poetry**:
    - Add the source: `poetry source add --priority=supplemental code_europa https://code.europa.eu/api/v4/projects/943/packages/pypi/simple`
    - Install the package: `poetry add --source code_europa dsa-tdb`

### From source (with poetry)

1. Install `poetry^1.8` on your system with either `pip install --user poetry>=1.8` or other methods.
2. Download and extract the code folder and cd into it.
3. Create the venv and install the dependencies using `poetry install` (with `--with dev` if you also want the jupyter notebook kernel and the developer tools)

## Usage

### CLI

The package will install a command line interface (cli) installing the command `dsa-tdb-cli` on your path.

The command has three subcommands:

- `preprocess` will download the specified daily dumps (eventually filtered by platforms or time window), verify their `SHA1` checksum and check for new files, later chunking them in smaller csv or parquet files. Optionally, it will delete the original dumps as they are processed (to save disk space), leaving the sha1 files as a proof of work. This allows to repeately run the `preprocess` step on a daily basis to always have the files in place. The resulting "chunked" files are stored as regular flat csv or parquet files which can be conveniently and efficiently loaded into the data processing pipeline of your choice (Spark, Dask, etc.) without having to go through the complex data structure of the daily dumps (zipped csv files).

- `aggregate` will use a separate configuration file (a template of which is provided in the repo under the [Aggregation Configuration Template](config_aggregation_simple.yaml) for the simple version that reproduces the data used in the [online dashboard](https://transparency.dsa.ec.europa.eu/dashboard) and in [a complete version](config_aggregation_complete.yaml), which is the default used in the Superset dashboard) to perform aggregation, that is, counting the number of Statements of reasons (SoRs) corresponding to a given combination of the fields in the database (such as `content_date`, `platform_name`, `category`, etc.):
    * This command will considerably reduce the size of the database by aggregating together similar rows (each statement of reason is a new row in the chunked data files, when they share the same values as defined in the aggregation configuration, they are represented as a single row with an incremented count in the aggregated files).
    * This command will also write an auxiliary csv file (with the same name of the `out_file_name`) containing the files and dates of the daily dumps used for the aggregation.
    * It will also make a copy of the configuration file used in the same folder of the output file with the same name and the `configuration.yaml` file for later reference.
    * If the aggregation mode is set to `append` in the configuration, it will load only the files that are not already in the (possibly existing) dates auxiliary file and will append the aggregated data to the (possibly already existing) file. Note that the `append` mode only works if:
        - the schema of the aggregated data is the same as the one from the existing file
        - **and** the input files are in the same relative or global path as
        found in the dates auxiliary file.
        - **and** the `parquet` output format is used.

> **_NOTE:_** Also note that, if using the `created_at` column to group, all the files
> produced with the `append` mode will have to be aggregated again on the
> desired keys as **there is no guarantee** that all the SoR from one day
> are in the corresponding daily dump file.

- `filter` will use a separate configuration file (a template of which is provided  in the repo under the [Filtering Configuration Template](config_filtering.yaml)) to filter the raw SoRs, that is, keeping only the ones respecting all the filters set (in an "AND" fashion).
    * This command will also write an auxiliary csv file (with the same name of the `out_file_name`) containing the files and dates of the daily dumps used for the filtering. It will also make a copy of the configuration file used in the same folder of the output file with the same name and the `configuration.yaml` file for later reference.
    * If the filtering mode is set to `append` in the configuration, it will load only the files that are not already in the (possibly existing) dates auxiliary file and will append the filtered data to the (possibly already existing) file. Note that the `append` mode only works if:
        - the schema of the filtered data is the same of the existing file.
        - **and** the input files are in the same relative or global path as found in the dates auxiliary file.
        - **and** the `parquet` output format is used.


You can see the help and documentation of the cli command by running `dsa-tdb-cli --help` or `dsa-tdb-cli subcommand --help`.


### Scripts

The [`scripts`](scripts) folder contains some examples on how to use the library. They can also be readily used in an automated manner to ingest and process on a daily basis the data dumps (e.g. with a crontask).

There are two examples:
- [`scripts/daily_routine.py`](scripts/daily_routine.py) is a script that can be called with the platform name and dump version (`full` or `light`). Without any further argument, it will:
    - preprocess (download and chunk) all the missing/newest daily dumps from the full version of the daily dumps for all available platforms.
    - aggregate them using the default configuration.
    - (optionally) delete the chunked files to save disk space.

- [`scripts/download_platform.py`](scripts/download_platform.py) is a subset of the previous script, it just preprocesses (download and chunk) the file for a specific platform and version (`full` or `light`).

> **_NOTE:_** The daily routine script can be called on a daily basis and it will update the files and dumps with the newest ones (leaving the latest as a checkpoint for next run).

### Dashboards with Apache Superset

Starting from version 0.5.1, the `dsa-tdb` package comes with a pre-built dashboard based on [Apache Superset](https://superset.apache.org/). The dashboard allows to visualize the aggregated data for the global, full version of the dumps. These dashboards and the corresponding dataset definitions are located in the [superset_exports](app/system_resources/superset_exports/) folder.


The default dashboard expects the aggregated data to be under the `/data/tdb_data/global___full/aggregations/aggregated-global-full.parquet` directory inside the container.

To view the dashboard:
- Launch the docker container with the `docker-compose up` using the `docker-compose.yml` file provided in the repo, as shown in the section [Docker](#with-docker-podman-recommended) above.
- Create an aggregated view of the global full dataset, using either the cli or the `daily_routine` in the [scripts](scripts) folder. Using the API:
    - Do a prepare with root data folder in `/data/tdb_data` and `global` platform and `full` version.
    - Do an aggregate with the same root folder, `global` platform and `full` version, and the `output file` set to `/data/tdb_data/global___full/aggregations/aggregated-global-full`
    - Please note that this might take a lot of time, so please test the procedure with a short time period first.
- Visit the Superset UI at `http://localhost:8088` (default username and password are `admin`).

### Notebooks

An example usage notebook is available in [`notebooks/Example.ipynb`](notebooks/Example.ipynb).


## Contributing

If you'd like to report issues, suggest code modifications or contribute in any other form, please head to the [CONTRIBUTING.md](CONTRIBUTING.md) documentation file.


## License

`dsa-tdb` is licensed under European Union Public Licence (EUPL) version 1.2.
See the [LICENSE](LICENSE) for details.

The data contained in the daily dumps are licensed under the CC BY 4.0 license.
See the [data release](https://data.europa.eu/data/datasets/dsa-transparency-database) for details.

If you use the data from the DSA Transparency Database for your research work, please cite it using the following information:

European Commission-DG CONNECT, Digital Services Act Transparency Database,
Directorate-General for Communications Networks, Content and Technology, 2023.

[doi.org/10.2906/134353607485211](https://doi.org/10.2906/134353607485211)


## Related community projects

A few community-driven codebases exist out there and might address particular use cases which are not covered by this
`dsa-tdb` package. Here is the list of projects we are aware of:

* [Shantay](https://github.com/apparebit/shantay), as an alternative to `dsa-tdb` targeting consumer hardware only. It treats storage as a scarce resource and separates long-term storage from the current working data. Instead of Spark, it builds on Pola.rs for data wrangling. While it doesn't support distributed runs, it does support multiprocessing runs for a small number of workers.


## Documentation

Documentation about the fields and values can be found in the official [API documentation](https://transparency.dsa.ec.europa.eu/page/api-documentation).

Interactive online documentation for the package is available on the [dsa-tdb page](https://dsa.pages.code.europa.eu/transparency-database/dsa-tdb/).
