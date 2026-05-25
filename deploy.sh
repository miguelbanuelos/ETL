#!/bin/bash
cd /docker/etl
git config --global --add safe.directory /docker/etl
git fetch --all
git reset --hard origin/main
docker compose up -d --build --force-recreate --remove-orphans