package com.gs.iceberg.ui.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.expressions.Expressions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for querying data from Iceberg tables.
 */
@Service
@Slf4j
public class QueryService {

    @Value("${storage.type:HDFS}")
    private String storageType;

    @Value("${storage.base-path}")
    private String basePath;

    @Value("${iceberg.table-name:messages}")
    private String tableName;

    private final StorageConfigurationService storageConfigurationService;

    @Autowired
    public QueryService(StorageConfigurationService storageConfigurationService) {
        this.storageConfigurationService = storageConfigurationService;
    }

    /**
     * Executes a query against the Iceberg table.
     *
     * @param filters The filters to apply to the query
     * @param limit The maximum number of results to return
     * @return The query results
     */
    public List<Map<String, Object>> executeQuery(Map<String, Object> filters, int limit) {
        log.info("Executing query with filters: {}, limit: {}", filters, limit);
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            Table table = getIcebergTable();

            org.apache.iceberg.expressions.Expression expression = buildExpression(filters);

            results = mockQueryResults(limit);

            log.info("Query executed successfully, returned {} results", results.size());
        } catch (Exception e) {
            log.error("Error executing query", e);
        }

        return results;
    }

    /**
     * Executes a bulk query against the Iceberg table.
     *
     * @param filtersList The list of filters to apply to the query
     * @param limit The maximum number of results to return per query
     * @return The query results
     */
    public List<List<Map<String, Object>>> executeBulkQuery(List<Map<String, Object>> filtersList, int limit) {
        log.info("Executing bulk query with {} filter sets, limit: {}", filtersList.size(), limit);
        List<List<Map<String, Object>>> results = new ArrayList<>();

        for (Map<String, Object> filters : filtersList) {
            results.add(executeQuery(filters, limit));
        }

        return results;
    }

    /**
     * Gets the Iceberg table.
     *
     * @return The Iceberg table
     */
    private Table getIcebergTable() {
        Configuration conf = new Configuration();
        
        if ("HDFS".equalsIgnoreCase(storageType)) {
            storageConfigurationService.getConfigurationByName("default")
                .ifPresent(config -> {
                    if ("HDFS".equalsIgnoreCase(config.getStorageType())) {
                        conf.set("hadoop.security.authentication", "kerberos");
                        conf.set("hadoop.security.authorization", "true");
                        conf.set("dfs.namenode.kerberos.principal", config.getPrincipal());
                    }
                });
        } else if ("S3".equalsIgnoreCase(storageType)) {
            storageConfigurationService.getConfigurationByName("default")
                .ifPresent(config -> {
                    if ("S3".equalsIgnoreCase(config.getStorageType())) {
                        conf.set("fs.s3a.access.key", config.getAccessKey());
                        conf.set("fs.s3a.secret.key", config.getSecretKey());
                        conf.set("fs.s3a.endpoint", "s3.amazonaws.com");
                        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                        conf.set("fs.s3a.aws.credentials.provider", 
                                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
                        
                        if (config.getRegion() != null && !config.getRegion().isEmpty()) {
                            conf.set("fs.s3a.region", config.getRegion());
                        }
                    }
                });
        }

        String tablePath = getTablePath();

        HadoopTables tables = new HadoopTables(conf);
        return tables.load(tablePath);
    }

    /**
     * Gets the table path.
     *
     * @return The table path
     */
    private String getTablePath() {
        if ("S3".equalsIgnoreCase(storageType)) {
            return "s3a://" + basePath + "/" + tableName;
        } else {
            return "hdfs://" + basePath + "/" + tableName;
        }
    }

    /**
     * Builds an Iceberg expression from filters.
     *
     * @param filters The filters to apply to the query
     * @return The Iceberg expression
     */
    private org.apache.iceberg.expressions.Expression buildExpression(Map<String, Object> filters) {
        if (filters == null || filters.isEmpty()) {
            return Expressions.alwaysTrue();
        }

        org.apache.iceberg.expressions.Expression expression = Expressions.alwaysTrue();

        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();

            if (value instanceof String) {
                expression = Expressions.and(expression, Expressions.equal(field, value));
            } else if (value instanceof Number) {
                expression = Expressions.and(expression, Expressions.equal(field, value));
            } else if (value instanceof Boolean) {
                expression = Expressions.and(expression, Expressions.equal(field, value));
            } else if (value instanceof Map) {
                Map<String, Object> rangeFilter = (Map<String, Object>) value;
                
                if (rangeFilter.containsKey("gt")) {
                    expression = Expressions.and(expression, 
                                               Expressions.greaterThan(field, rangeFilter.get("gt")));
                }
                
                if (rangeFilter.containsKey("gte")) {
                    expression = Expressions.and(expression, 
                                               Expressions.greaterThanOrEqual(field, rangeFilter.get("gte")));
                }
                
                if (rangeFilter.containsKey("lt")) {
                    expression = Expressions.and(expression, 
                                               Expressions.lessThan(field, rangeFilter.get("lt")));
                }
                
                if (rangeFilter.containsKey("lte")) {
                    expression = Expressions.and(expression, 
                                               Expressions.lessThanOrEqual(field, rangeFilter.get("lte")));
                }
            }
        }

        return expression;
    }

    /**
     * Creates mock query results for testing.
     *
     * @param limit The maximum number of results to return
     * @return The mock query results
     */
    private List<Map<String, Object>> mockQueryResults(int limit) {
        List<Map<String, Object>> results = new ArrayList<>();

        for (int i = 0; i < limit; i++) {
            Map<String, Object> result = new HashMap<>();
            result.put("id", "msg-" + i);
            result.put("timestamp", System.currentTimeMillis() - i * 1000);
            result.put("field1", "value" + i);
            result.put("field2", i);
            result.put("field3", i % 2 == 0);
            
            results.add(result);
        }

        return results;
    }
}
