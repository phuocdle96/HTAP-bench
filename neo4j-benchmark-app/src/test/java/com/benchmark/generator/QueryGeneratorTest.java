package com.benchmark.generator;

import com.benchmark.client.DatabaseClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueryGeneratorTest {

    @Mock
    private DatabaseClient mockDbClient;

    private QueryGenerator queryGenerator;

    @BeforeEach
    void setUp() {
        when(mockDbClient.fetchSampleIds("MATCH (p:Patient) RETURN p.patientId as id LIMIT 10000", "id"))
                .thenReturn(List.of("patient1"));
        when(mockDbClient.fetchSampleIds("MATCH (h:HealthcareUnit) RETURN h.unitId as id LIMIT 10000", "id"))
                .thenReturn(List.of("unit1"));
        when(mockDbClient.fetchSampleIds("MATCH (d:Diagnosis) RETURN d.code as id LIMIT 10000", "id"))
                .thenReturn(List.of("diag1"));
        when(mockDbClient.fetchSampleIds("MATCH (a:Admission) RETURN a.eventId as id LIMIT 10000", "id"))
                .thenReturn(List.of("adm1"));
        
        queryGenerator = new QueryGenerator(mockDbClient);
    }

    @Test
    void testTemplatesAreLoaded() {
        assertNotNull(queryGenerator);
    }

    @Test
    void testPrepareAllQueriesGeneratesData() {
        Map<String, List<QueryTemplate.PreparedQuery>> preparedQueries = queryGenerator.prepareAllQueries();

        assertNotNull(preparedQueries);
        assertFalse(preparedQueries.isEmpty());
        assertTrue(preparedQueries.containsKey("OLTP"));
        
        List<QueryTemplate.PreparedQuery> oltpQueries = preparedQueries.get("OLTP");
        assertFalse(oltpQueries.isEmpty());
        
        QueryTemplate.PreparedQuery firstQuery = oltpQueries.get(0);
        assertNotNull(firstQuery.cypher());
        assertNotNull(firstQuery.params());
    }
}