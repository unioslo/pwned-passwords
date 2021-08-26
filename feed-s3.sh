#!/bin/bash

hashfile="$1"
s3path="$2"
s3config="$3"

if [[ ! -f "$hashfile" ]]; then
    echo "Error: $0 <hashfile> <s3path> [<s3config>]" >&2
    exit 1
fi
if [[ ! "$s3path" =~ ^s3://.*/$ ]]; then
    echo "Error: Not a S3 path: '$s3path'. Use a trailing slash." >&2
    exit 1
fi
if [[ "$s3config" == "" ]] || [[ ! -f "$s3config" ]]; then
    s3config="$HOME/.s3cfg"
fi

shorthash=""
output=""
tmpfile=$(mktemp)

# This is going to take a while. It uploads the hash files one by one.

while read line; do
    previoushash=$smallhash
    smallhash=${line::5}
    if [[ "$previoushash" != "" ]] && [[ "$previoushash" != "$smallhash" ]]; then
        echo -en "$output" > "$tmpfile"
        s3cmd --config="$s3config" put "$tmpfile" "${s3path}${previoushash}" || exit 1
        output=""
    fi
    output+="$line\n"
done < "$hashfile"

echo -en "$output" > "$tmpfile"
s3cmd --config="$s3config" put "$tmpfile" "${s3path}${smallhash}" || exit 1
