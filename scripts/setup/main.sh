#!/bin/bash
## /scripts/setup_scripts/main.sh
## Description: This shell script helps setup our modules 

# Setup relational database
sh scripts/setup/relationalDB.sh

# Setup non-relational database
sh scripts/setup/nonrelationalDB.sh  