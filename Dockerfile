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

# Docker file to use the spark-docker/3.5.2/scala2.12-java11-python3-ubuntu
# as a base to install our dependencies and serve a jupyter notebook
# with our venv's kernel by default.

FROM docker.io/apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

# Make a user to run the notebook and owning the /home/spark directory
# We can also override user name at build. If build-arg is not passed, will create user named `user`
ARG DOCKER_USER=${DOCKER_USER:-user}
ARG DOCKER_USER_ID=${DOCKER_USER_ID:-1000}
ARG DOCKER_GROUP_ID=${DOCKER_GROUP_ID:-1000}

# Ports
ENV JUPYTER_PORT=${JUPYTER_PORT:-8765}
ENV SPARK_THRIFT_PORT=${SPARK_THRIFT_PORT:-4050}
ENV SPARK_APPLICATIONS_UI_PORT=${SPARK_APPLICATIONS_UI_PORT:-4040}
ENV SPARK_MASTER_UI_PORT=${SPARK_MASTER_UI_PORT:-8080}
ENV CELERY_FLOWER_PORT=${CELERY_FLOWER_PORT:-5555}
ENV FASTAPI_PORT=${FASTAPI_PORT:-8000}
ENV SUPERSET_PORT=${SUPERSET_PORT:-8088}

# Misc env variables
ENV DOCKER_USER="$DOCKER_USER"
ENV VIRTUAL_ENV="/home/$DOCKER_USER/.venv"
ENV PATH="$VIRTUAL_ENV/bin:/home/$DOCKER_USER/.local/bin:$PATH"
ENV PIPX_HOME="/home/$DOCKER_USER/.local/share/pipx"
ENV SPARK_LOCAL_DIRS=/cache
ENV CELERY_BROKER_URL=${CELERY_BROKER_URL:-redis://redis-docker-service:6379/0}
ENV FLOWER_UNAUTHENTICATED_API=true
ENV FLASK_APP=superset
ENV SUPERSET_CONFIG_PATH=/home/$DOCKER_USER/app/system_resources/superset_config.py
# Drop Superset telemetry, see https://superset.apache.org/docs/faq/#does-superset-collect-any-telemetry-data
ENV SCARF_ANALYTICS=false

USER root

# TODO check if group already exists
RUN addgroup --firstuid $DOCKER_GROUP_ID --lastuid $DOCKER_GROUP_ID --uid $DOCKER_GROUP_ID $DOCKER_USER && useradd --no-log-init -m --shell /bin/bash -u $DOCKER_USER_ID --gid $DOCKER_GROUP_ID $DOCKER_USER

RUN set -ex; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        supervisor \
        python3-wheel python3-wheel-whl \
        python3-dev \
        python3-venv \
        python3-pip \
	    build-essential libssl-dev libffi-dev \
        python3-dev python3-pip \
        libsasl2-dev libldap2-dev default-libmysqlclient-dev vim; \
    apt-get clean && rm -rf /var/lib/apt/lists/*;

# Make a /notebooks and /data directories to store the notebooks & data
# and change the owner to spark
# Also Set Spark config
RUN mkdir /notebooks && chown -R $DOCKER_USER:$DOCKER_GROUP_ID /notebooks; \
    mkdir /data      && chown -R $DOCKER_USER:$DOCKER_GROUP_ID /data; \
    mkdir /cache     && chown -R $DOCKER_USER:$DOCKER_GROUP_ID /cache; \
    mkdir /src     && chown -R $DOCKER_USER:$DOCKER_GROUP_ID /src; \
    mkdir -p /home/$DOCKER_USER/.jupyter/lab/; \
    mkdir -p /home/$DOCKER_USER/.local/share/pipx; \
    chown -R $DOCKER_USER:$DOCKER_GROUP_ID /home/$DOCKER_USER; \
    mkdir -p $SPARK_HOME/conf && chown -R $DOCKER_USER $SPARK_HOME/conf; \
    mkdir -p $SPARK_HOME/logs && chown -R $DOCKER_USER $SPARK_HOME/logs; \
    echo 'spark.sql.caseSensitive: True' > $SPARK_HOME/conf/spark-defaults.conf;

# Switch to the regular user
USER $DOCKER_USER

RUN set -ex; \
    pip --no-cache-dir install --user pipx; \
    echo $PIPX_HOME; \
    echo `which pipx`; \
    pipx install poetry==1.8.3; \
    pipx inject poetry poetry-plugin-export; \
    pipx install apache-superset==4.1.1; \
    pipx inject apache-superset pillow==11.0.0 pyhive==0.7.0 thrift==0.21.0 thrift-sasl gevent; \
    pipx inject --force apache-superset marshmallow==3.26.1; \
    python3 -m venv $VIRTUAL_ENV;

WORKDIR /src
COPY . .
RUN poetry export --with dev --with webapp -f requirements.txt | pip --no-cache-dir install -r /dev/stdin; \
    poetry build && pip install dist/*.whl;

# Copy the docker_wrapper.sh script to the home directory
COPY --chown=$DOCKER_USER:$DOCKER_GROUP_ID app /home/$DOCKER_USER/app
COPY --chown=$DOCKER_USER:$DOCKER_GROUP_ID app/system_resources/main_celery.py /home/$DOCKER_USER/
COPY --chown=$DOCKER_USER:$DOCKER_GROUP_ID app/system_resources/main_fastapi.py /home/$DOCKER_USER/
COPY --chown=$DOCKER_USER:$DOCKER_GROUP_ID app/system_resources/user-settings /home/$DOCKER_USER/.jupyter/lab/user-settings
COPY --chown=$DOCKER_USER:$DOCKER_GROUP_ID app/system_resources/init_superset.sh /home/$DOCKER_USER/

# Set the working directory
WORKDIR /home/$DOCKER_USER

# # Init superset # Moved to runtime
# RUN /opt/spark/sbin/start-master.sh -h 0.0.0.0 -p 7077 --webui-port $SPARK_MASTER_UI_PORT && /opt/spark/sbin/start-thriftserver.sh --master spark://127.0.0.1:7077 --conf spark.ui.port=$SPARK_THRIFT_PORT; \
#     superset db upgrade && superset fab create-admin --username admin --firstname admin --lastname user --email admin@example.com --password admin && superset init; \
#     superset import-directory /home/$DOCKER_USER/app/system_resources/superset_exports/; \
#     /opt/spark/sbin/stop-master.sh -h 0.0.0.0 -p 7077 --webui-port $SPARK_MASTER_UI_PORT && /opt/spark/sbin/stop-thriftserver.sh --conf spark.ui.port=$SPARK_THRIFT_PORT;

# Expose the port for the notebook
EXPOSE $JUPYTER_PORT
# Expose the port for the spark thrift server app
EXPOSE $SPARK_THRIFT_PORT
# Expose the port for the spark applications UI
EXPOSE $SPARK_APPLICATIONS_UI_PORT
# Expose the port for the spark master UI
EXPOSE $SPARK_MASTER_UI_PORT
# Expose the port for the flower interface
EXPOSE $CELERY_FLOWER_PORT
# Expose the fastapi port
EXPOSE $FASTAPI_PORT
# Expose the superset port
EXPOSE $SUPERSET_PORT

# Give precedence to the user's PATH over pipx
ENV PATH="$VIRTUAL_ENV/bin:/home/$DOCKER_USER/.local/bin:$PATH"
# Set the entrypoint
CMD ["/bin/bash", "-c", "supervisord -c /home/$DOCKER_USER/app/system_resources/supervisord.conf"]
