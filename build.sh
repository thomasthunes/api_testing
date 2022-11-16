#!/bin/bash

docker build -t tsd:fileapi .
export id=$(docker create tsd:fileapi)
docker cp $id:/file-api/dist/ .
