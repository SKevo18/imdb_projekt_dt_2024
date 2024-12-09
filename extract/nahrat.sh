#!/bin/bash
cd "$(dirname "$0")"

# --account parameter sa dá zistiť cez tento Snowflake query: `SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();`
snow sql -x --account="SFEDU02-IDB20327" --user HEDGEHOG --database HEDGEHOG_IMDB --warehouse HEDGEHOG_IMDB_WH --schema STAGING --password="totoniejemojeheslo"  --filename nahrat.sql
