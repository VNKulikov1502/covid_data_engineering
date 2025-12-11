#!/bin/bash

# Initialize the database
superset db upgrade

# Create admin user
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin

# Initialize Superset
superset init

# Start Superset
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger