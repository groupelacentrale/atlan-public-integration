#!/bin/sh -l

echo "github_repository=$GITHUB_REPOSITORY"
echo "branch=$GITHUB_REF"
echo "working dir"
pwd
echo "list of files in working dir"
ls -l
echo "list of files in the racine"
ls -l /
echo "list of files in /opt/app"
ls -l /opt/app/

python /opt/app/main.py