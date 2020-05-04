#!/bin/bash
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -d "$POSTGRES_DB"  <<-EOSQL
     create schema if not exists $SCHEMA;
     create table $SCHEMA.urls (
        id serial primary key,
        chars text not null,
        urls json
     );
     create role $ROLE nologin;
     create role $AUTHENTICATOR noinherit login password '$POSTGRES_PASSWORD';
     grant $ROLE to $AUTHENTICATOR;
EOSQL