#!/bin/bash

MISC_LOCAL_FILES="/files/misc/"
MISC_HDFS_PATH="/datalake/raw/misc/"

hdfs dfs -put $MISC_LOCAL_FILES $MISC_HDFS_PATH
if [ $? -eq 0 ]; then
    echo "$(date): Successful Load
else
    echo "$(date): Error
fi
