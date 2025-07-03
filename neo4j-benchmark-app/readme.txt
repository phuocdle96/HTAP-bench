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
java -jar target\neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --uri "neo4j://localhost:7687" --user neo4j --password Nhung1998 --database HealthCareDB --oltp-clients 14 --olap-clients 1 --graph-clients 1 --duration 60

#open mode: fires queries based on posion distribution
java -jar target/neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --uri "neo4j://localhost:7687" --user neo4j --password Nhung1998 --database HealthCareDB --arrival-mode open --arrival-rate OLTP=5000 --arrival-rate GRAPH=50 --arrival-rate OLAP=3 --oltp-clients 14 --graph-clients 1 --olap-clients 1 --duration 60
