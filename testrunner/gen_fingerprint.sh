#!/bin/bash

set -ex

if [ -z "$(git status --untracked-files=no --porcelain)" ]; then
  echo "repo_clean=true"
else
  echo "repo_clean=false"
  # Uncommitted changes in tracked files
fi

echo "commit=$(git rev-parse --verify HEAD)"

echo "desc=$(git describe --tags)"
