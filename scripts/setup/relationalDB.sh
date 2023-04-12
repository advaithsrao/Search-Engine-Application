#!/bin/bash
## /scripts/setup_scripts/relational_db.sh
## Description: This shell script helps setup a docker image for postgres, and push our data into respective tables into a postgres server instance

# Pull the latest docker image of postgres
docker pull postgres:latest

# Export the image_id of our postgres image
export imageID=$(docker images|grep postgres|xargs|awk '{print $3}')

# Docker run our postgres image
docker run -p 127.0.0.1:5544:5432 -e POSTGRES_USER=postgresUser -e POSTGRES_PASSWORD=postgresPW $imageID

# Run script to push our data onto postgres tables
python3 ../pushRelationalDB.py