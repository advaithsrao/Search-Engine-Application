#!/bin/bash
## /scripts/setup_scripts/relationalDB.sh
## Description: This shell script helps setup a docker image for postgres, and push our data into respective tables into a postgres server instance

# Pull the latest docker image of postgres
docker pull postgres:latest

# Export the image_id of our postgres image
export imageID=$(docker images|grep postgres|xargs|awk '{print $3}')

# Docker run our postgres image
docker run -d --name postgresServer \
    -p 127.0.0.1:5544:5432 \
    -e POSTGRES_USER=postgresUser \
    -e POSTGRES_PASSWORD=postgresPW \
    $imageID

# Sleep for 30 seconds as we want the earlier command to run
sleep 30

# Run script to create our postgres tables and push our data onto postgres tables
python3 scripts/relationalDB.py
