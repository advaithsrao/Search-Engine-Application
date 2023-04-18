#!/bin/bash
## /scripts/setup_scripts/nonrelationalDB.sh
## Description: This shell script helps setup a docker image for elasticsearch, and push our data into respective tables into a elasticsearch server instance

# Pull the latest docker image of elasticsearch
docker pull elasticsearch:7.14.0

# Export the image_id of our elasticsearch image
export imageID=$(docker images|grep elasticsearch|xargs|awk '{print $3}')

# Create common network for Elasticsearch and Kibana
docker network create noSQL_network

# Run the elasticsearch image
docker run -d --name elasticSearchServer \
    --net noSQL_network \
    --restart=always \
    -p 9200:9200  \
    -p 9300:9300 \
    -e "discovery.seed_hosts=localhost" \
    -e "ELASTIC_USERNAME=elasticsearchUser" \
    -e "ELASTIC_PASSWORD=elasticsearchPW" \
    -e "discovery.type=single-node" \
    -e "node.data=true" \
    $imageID

# Run kibana for monitoring ELK
docker run -d --name kib-01 \
    --net noSQL_network \
    -p 5601:5601 \
    docker.elastic.co/kibana/kibana:8.7.0

# docker run -d --name ElasticSearchServer \
#     -p 9200:9200 \
#     -p 9300-9400:9300-9400 \
#     -e "discovery.seed_hosts=esnode1,esnode2,esnode3,esnode4,esnode5,esnode6,esnode7,esnode8,esnode9,esnode10,esnode11,esnode12,esnode13,esnode14,esnode15" \
#     -e "ELASTIC_USERNAME=elasticsearchUser" \
#     -e "ELASTIC_PASSWORD=elasticsearchPW" \
#     -e "discovery.type=zen" \
#     -e "cluster.name=my_cluster" \
#     -e "node.name=node1" \
#     -e "node.master=true" \
#     -e "node.data=true" \
#     -e "node.max_local_storage_nodes=15" \
#     $imageID

# Sleep for 30 seconds as we want the earlier command to run
sleep 30

# Run script to create our postgres tables and push our data onto postgres tables
python3 scripts/nonrelationalDB.py