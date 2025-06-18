package com.benchmark.client;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jClient implements DatabaseClient {
    private final Driver driver;
    private final String databaseName;

    public Neo4jClient(String uri, String user, String password, String databaseName) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        this.databaseName = databaseName;
        connect();
    }

    @Override
    public void connect() {
        driver.verifyConnectivity();
        System.out.println("Successfully connected to Neo4j.");
    }

    @Override
    public List<Map<String, Object>> executeQuery(String query, Map<String, Object> params) {
       try (Session session = driver.session(SessionConfig.forDatabase(this.databaseName))) {
           Result result = session.run(query, params);
           return result.list(r -> r.asMap());
       }
    }
    
    @Override
    public List<String> fetchSampleIds(String query, String idColumn) {
        List<String> ids = new ArrayList<>();
        try (Session session = driver.session(SessionConfig.forDatabase(this.databaseName))) {
            Result result = session.run(query);
            while (result.hasNext()) {
                ids.add(result.next().get(idColumn).asString());
            }
        }
        return ids;
    }

    @Override
    public void close() {
        driver.close();
    }
}