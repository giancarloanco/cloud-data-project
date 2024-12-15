#!/bin/bash

RETAIL_LOCAL_FILE="/files/retail.csv"
RETAIL_HDFS_PATH="/datalake/raw/retail/"

hdfs dfs -put $RETAIL_LOCAL_FILE $RETAIL_HDFS_PATH
if [ $? -eq 0 ]; then
    echo "$(date): Successful Load
else
    echo "$(date): Error
fi
