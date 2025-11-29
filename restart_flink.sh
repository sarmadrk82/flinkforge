# Clean
docker stop flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 2>/dev/null || true
docker rm flink-jobmanager flink-taskmanager-1 flink-taskmanager-2 2>/dev/null || true
docker network rm flink-net 2>/dev/null || true

docker network create flink-net

# JobManager
docker run -d --name flink-jobmanager --network flink-net -p 8081:8081 -v $(pwd):/workspace -e FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager" myflink-pyflink jobmanager

# TaskManagers
docker run -d --name flink-taskmanager-1 --network flink-net -v $(pwd):/workspace -e FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager" myflink-pyflink taskmanager
docker run -d --name flink-taskmanager-2 --network flink-net -v $(pwd):/workspace -e FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager" myflink-pyflink taskmanager


