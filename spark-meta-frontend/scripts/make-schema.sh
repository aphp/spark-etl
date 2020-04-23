#!/bin/bash -e

/etc/init.d/postgresql start 

if [ ! -f /mnt/schema.sql ]; then
    exit 0
fi

POSTGRES_USER="docker"
POSTGRES_DB="docker"
PASSWORD="docker"
psql --command "CREATE USER ${POSTGRES_USER} WITH SUPERUSER PASSWORD '${PASSWORD}';" && createdb -O ${POSTGRES_DB} ${POSTGRES_USER}
psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "$POSTGRES_DB" < /mnt/schema.sql