#!/bin/bash

echo "Building DuckdB script..."

python ~/movi-duckdb/duck_setup.py >> logs.txt 2>&1
return_code=$?

if [ $return_code -eq 0 ]; then
    echo "DuckDB builder executed successfully."
else
    echo "DuckDB builder encountered an error with return code $return_code."
fi

echo "DuckDB builder execution complete."

echo "Starting Metabase..."

java -jar /home/metabase.jar >> logs.txt 2>&1
return_code=$?

if [ $return_code -eq 0 ]; then
    echo "Metabase executed successfully."
else
    echo "Metabaseapplication encountered an error with return code $return_code."
fi
