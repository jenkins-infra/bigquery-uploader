#!/usr/bin/env bash

#
# Converts extension-points.json in to big query friendly extension records
#
function usage(){
    echo "Usage: bq-prepare-data.sh source_path destination_path"
    exit 1
}

if [ -z "$1" ]; then
    echo "Path of json file to be prapared for BigQuery needed"
    usage
fi

if [ -z $2 ]; then
    echo "Path of destination BigQuery file needed"
    usage
fi


result=( )
while IFS= read -r ; do
    echo "$REPLY" >> $2
done < <(jq '.artifacts | map(.)' < "$1" | jq -c '.[] | select(.)')

# Dash '-' is not supported by big query, replace them by underscore '_'
sed -i -e 's/-/_/g' $2