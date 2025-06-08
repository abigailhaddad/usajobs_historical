#!/bin/bash
# Upload schema to Neon database
# Requires DATABASE_URL environment variable to be set

if [ -z "$DATABASE_URL" ]; then
    echo "Error: DATABASE_URL environment variable not set"
    echo "Please set it in your .env file"
    exit 1
fi

echo "Uploading schema to database..."
psql "$DATABASE_URL" -f sql/create_historical_jobs.sql
echo "Schema upload complete!"