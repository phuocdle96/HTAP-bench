### Important to save and restore the original database before the test

Dump database:
neo4j-admin database dump HealthCareDB --to-path="E:\Dataset\Backups"

Load database:
neo4j-admin database load HealthCareDB --from-path="E:\Dataset\Backups" --overwrite-destination=true

### Original database meta:
Nodes: 272929005
Relationships: 774560425
Labels: 23
Relationship Types: 5
Property Keys: 35

### Example Benchmark Run Command

After compiling with `mvn clean install`, you can run the benchmark like this from your project's root directory:

```bash
java -jar target\neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar ^
  --uri "neo4j://localhost:7687" ^
  --user neo4j ^
  --password your_neo4j_password ^
  --database HealthCareDB ^
  --oltp-clients 14 ^
  --olap-clients 1 ^
  --graph-clients 1 ^
  --duration 60
```

#closed mode: fires queries with no limit
java -jar target\neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --uri "neo4j://localhost:7687" --user neo4j --password Nhung1998 --database HealthCareDB --oltp-clients 14 --olap-clients 1 --graph-clients 1 --duration 180

MAC & linux:
java -jar target/neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --uri "neo4j://192.168.1.24:7687" --user neo4j --password Nhung1998 --database HealthCareDB --oltp-clients 14 --olap-clients 1 --graph-clients 1 --duration 180

#open mode: fires queries based on posion distribution
java -jar target/neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --uri "neo4j://localhost:7687" --user neo4j --password Nhung1998 --database HealthCareDB --arrival-mode OPEN --arrival-rate OLTP=5000 --arrival-rate GRAPH=50 --arrival-rate OLAP=3 --duration 180

MAC & Linux:
java -jar target/neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --uri "neo4j://192.168.1.24:7687" --user neo4j --password Nhung1998 --database HealthCareDB --arrival-mode OPEN --arrival-rate OLTP=5000 --arrival-rate GRAPH=50 --arrival-rate OLAP=0.1 --duration 180


JanusGraph:
java -jar target/neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --engine JANUSGRAPH --uri "gremlin://172.31.11.94:8182" --arrival-mode CLOSED --oltp-clients 8 --graph-clients 1 --olap-clients 1 --warmup 10 --duration 180


java -jar target/neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --engine JANUSGRAPH --uri "gremlin://172.31.11.94:8182" --arrival-mode OPEN --arrival-rate OLTP=1500 --arrival-rate GRAPH=50 --arrival-rate OLAP=0 --warmup 10 --duration 180

Memgraph:
java -jar target/neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --engine MEMGRAPH --uri "bolt://172.31.1.68:7687" --user "" --password "" --database memgraph --arrival-mode CLOSED --oltp-clients 8 --graph-clients 3 --olap-clients 10 --warmup 10 --duration 180

java -jar target/neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --engine MEMGRAPH --uri "bolt://172.31.1.68:7687" --user "" --password "" --database memgraph --arrival-mode OPEN --arrival-rate OLTP=10000 --arrival-rate GRAPH=0 --arrival-rate OLAP=0 --duration 300 --ramp-interval 30 --ramp-rate-step OLTP=10000 --max-arrival-rate OLTP=100000


E:
cd AWS
