#!/bin/sh -l

echo "github_repository=$GITHUB_REPOSITORY"
echo "branch=$GITHUB_REF"

python /opt/app/main.py