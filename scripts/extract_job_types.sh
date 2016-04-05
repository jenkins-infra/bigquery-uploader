#!/usr/bin/env bash

#
# Extracts extensionPoints/hudson.model.Job/implementations/className from source extension-points.json
# in to a destination files
#
function usage(){
    echo "Usage: extract_job_types.sh extension_points_path destination_file_path"
    exit 1
}

if [ -z "$1" ]; then
    echo "Path of extension-points.json, if local use file:// scheme with absolute path"
    usage
fi

if [ -z $2 ]; then
    echo "Path to destination file"
    usage
fi

curl $1 |jq '.extensionPoints | ."hudson.model.Job" | .implementations[].className' | sed  -e 's/\./_/g' > $2
