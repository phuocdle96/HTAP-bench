### Important to save and restore the original database before the test

Dump database:
neo4j-admin database dump HealthCareDB --to-path="E:\Dataset\Backups"

Load database:
neo4j-admin database load HealthCareDB --from="E:\Dataset\Backups\HealthCareDB.dump" --overwrite-destination=true

### Example Benchmark Run Command

After compiling with `mvn clean install`, you can run the benchmark like this from your project's root directory:

```bash
java -jar target\neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar ^
  --uri "neo4j://localhost:7687" ^
  --user neo4j ^
  --password your_neo4j_password ^
  --database HealthCareDB ^
  --clients 16 ^
  --duration 60
```

java -jar target\neo4j-benchmark-app-1.0-SNAPSHOT-jar-with-dependencies.jar --uri "neo4j://localhost:7687" --user neo4j --password Nhung1998 --database HealthCareDB --clients 16 --duration 60