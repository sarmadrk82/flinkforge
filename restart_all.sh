#!/bin/bash
# restart_all.sh — ONE COMMAND TO BRING THE WHOLE UNIVERSE BACK TO LIFE

set -e  # stop on first error

echo "Stopping & removing old containers..."
docker stop flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 postgres-flink 2>/dev/null || true
docker rm   flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 postgres-flink 2>/dev/null || true
docker network rm flink-net 2>/dev/null || true

echo "Creating network..."
docker network create flink-net

echo "Starting Postgres (crm_db + user postgres, password secret)..."
docker run -d \
  --name postgres-flink \
  --network flink-net \
  -e POSTGRES_DB=crm_db \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  postgres:15-alpine

echo "Waiting 8 seconds for Postgres to be ready..."
sleep 8

echo "Starting Flink JobManager..."
docker run -d --name flink-jobmanager --network flink-net \
  -p 8081:8081 \
  -v $(pwd):/workspace \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager" \
  myflink-pyflink jobmanager

echo "Starting TaskManagers..."
docker run -d --name flink-taskmanager-1 --network flink-net \
  -v $(pwd):/workspace \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager" \
  myflink-pyflink taskmanager

docker run -d --name flink-taskmanager-2 --network flink-net \
  -v $(pwd):/workspace \
  -e FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager" \
  myflink-pyflink taskmanager

echo ""
echo "ALL SYSTEMS UP"
echo "Flink UI    → http://localhost:8081"
echo "Postgres    → host.docker.internal:5432  (db: crm_db, user: postgres, pass: secret)"
echo "Workspace   → $(pwd) mounted into all containers"
echo ""
echo "IT'S ALIVE (again)."


