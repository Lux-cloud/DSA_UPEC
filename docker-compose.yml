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
services:
  redis-docker-service:
    image: docker.io/redis:7.4.6
    container_name: dsa-tdb-redis-cache
    restart: unless-stopped
    command: redis-server /conf/redis.conf
    user: 999:999
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      retries: 5
      start_period: 30s
      timeout: 3s
    volumes:
       - "redis-data:/data"
       - "./app/system_resources/redis_config:/conf"


  superset-docker-service:
    build:
      context: .
      dockerfile: Dockerfile
      args:
        DOCKER_USER: ${DOCKER_USER:-user}
        DOCKER_USER_ID: ${DOCKER_USER_ID:-1000}
        DOCKER_GROUP_ID: ${DOCKER_GROUP_ID:-1000}
    image: code.europa.eu:4567/dsa/transparency-database/dsa-tdb:${DOCKER_IMG_TAG:-latest}
    container_name: dsa-tdb-superset
    entrypoint: "/bin/bash -c \"supervisord -c /home/user/app/system_resources/superset-supervisord.conf\""
    ports:
      # These ports are used to access the services
      # The port on the left side of the colon is the host port (the one to edit)
      # The port on the right side of the colon is the container port
      - "8088:8088" # Superset
    depends_on:
      redis-docker-service:
        condition: service_healthy
      dsa-tdb-docker-service:
        condition: service_healthy
    volumes:
      # These volumes are used to persist data and notebooks
      # The path on the left side of the colon is the host path (the one to edit)
      # The path on the right side of the colon is the container path
      - type: bind
        source: ${DOCKER_DATA_DIR:-./data}
        target: /data
      - type: bind
        source: ./superset_home
        target: /home/user/.superset

  dsa-tdb-docker-service:
    build:
      context: .
      dockerfile: Dockerfile
      args:
        DOCKER_USER: ${DOCKER_USER:-user}
        DOCKER_USER_ID: ${DOCKER_USER_ID:-1000}
        DOCKER_GROUP_ID: ${DOCKER_GROUP_ID:-1000}
    healthcheck:
      test: ["CMD-SHELL", "curl http://127.0.0.1:8080"]
      interval: 10s
      retries: 5
      start_period: 30s
      timeout: 10s
    image: code.europa.eu:4567/dsa/transparency-database/dsa-tdb:${DOCKER_IMG_TAG:-latest}
    container_name: dsa-tdb-api
    ports:
      # These ports are used to access the services
      # The port on the left side of the colon is the host port (the one to edit)
      # The port on the right side of the colon is the container port
      - "8765:8765" # Notebook
      - "4040:4040" # Spark user applications UI
      - "5555:5555" # Celery Flower
      - "8000:8000" # FastAPI
      - "8080:8080" # Spark master UI
      - "8081:8081" # Spark worker UI
    depends_on:
      redis-docker-service:
        condition: service_healthy
    volumes:
      # These volumes are used to persist data and notebooks
      # The path on the left side of the colon is the host path (the one to edit)
      # The path on the right side of the colon is the container path
      - type: bind
        source: ${DOCKER_DATA_DIR:-./data}
        target: /data
      - ./notebooks:/notebooks
      # - /path/to/cache:/cache


volumes:
  redis-data:
    name: "dsa-tdb-redis-cache-volume"
