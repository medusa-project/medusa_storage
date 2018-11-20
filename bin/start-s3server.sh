#!/bin/bash

echo "Starting server"
docker pull scality/s3server:mem-latest
docker restart $S3_SERVER_DOCKER_NAME
