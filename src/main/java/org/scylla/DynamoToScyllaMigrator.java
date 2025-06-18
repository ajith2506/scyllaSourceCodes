/**
 * DynamoToScyllaMigrator is a utility class for migrating data from DynamoDB tables
 * to ScyllaDB tables using the DynamoDB-compatible API. This class reads
 * configuration from a YAML file and performs data migration from DynamoDB endpoint to
 * ScyllaDB endpoints for the list of Source-Target table mappings
 *
 * <p>Configuration is loaded from 'dynamo-scylla.yml' file in the classpath which
 * contains the DynamoDB endpoint with Source tables and ScyllaDB endpoint with Target tables
 * </p>
 *
 * @author Ajith Babu Kalaiselvan
 */

package org.scylla;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class DynamoToScyllaMigrator {
    private static final Logger logger = LoggerFactory.getLogger(DynamoToScyllaMigrator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final HttpClient httpClient = HttpClient.newHttpClient();

    public static void main(String[] args) {

        Map<String, Map<String, String>> config = loadConfiguration();
        if (config == null) return;

        Map<String, String> dynamoConfig = config.get("dynamo");
        Map<String, String> scyllaConfig = config.get("scylla");
        List<Map<String, String>> tableMappings = loadTableMappings(config);

        if (dynamoConfig == null || scyllaConfig == null || tableMappings.isEmpty()) {
            logger.error("Missing required configuration sections or table mappings. Please check your dynamo-scylla.yml file.");
            return;
        }

        AmazonDynamoDB dynamoDBClient = initializeDynamoDBClient(
                dynamoConfig.get("accessKey"),
                dynamoConfig.get("secretKey"),
                dynamoConfig.get("region"),
                dynamoConfig.get("endpoint")
        );
        if (dynamoDBClient == null) return;

        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);

        int totalSuccess = 0;
        int totalFailure = 0;

        for (Map<String, String> mapping : tableMappings) {
            String sourceTableName = mapping.get("sourceTableName");
            String targetTableName = mapping.get("targetTableName");

            logger.info("Starting migration for Source Table: {} -> Target Table: {}", sourceTableName, targetTableName);

            try {
                Map<String, String> attributeTypes = getAttributeTypes(dynamoDBClient, sourceTableName);
                boolean isTtlEnabled = isTimeToLiveEnabled(dynamoDBClient, sourceTableName);
                MigrationResult result = migrateTable(dynamoDB, sourceTableName, targetTableName, scyllaConfig.get("endpoint"), attributeTypes, isTtlEnabled, dynamoDBClient);

                totalSuccess += result.successCount;
                totalFailure += result.failureCount;

                logger.info("Migration completed for Source Table: {} -> Target Table: {} | Success: {} | Failure: {}",
                        sourceTableName, targetTableName, result.successCount, result.failureCount);
            } catch (Exception e) {
                logger.error("Error occurred while migrating table {}: {}", sourceTableName, e.getMessage(), e);
            }
        }

        logger.info("All migrations completed. Total Success: {} | Total Failure: {}", totalSuccess, totalFailure);

        dynamoDBClient.shutdown();
    }

    private static boolean isTimeToLiveEnabled(AmazonDynamoDB dynamoDBClient, String tableName) {
        try {
            DescribeTimeToLiveRequest ttlRequest = new DescribeTimeToLiveRequest().withTableName(tableName);
            DescribeTimeToLiveResult ttlResult = dynamoDBClient.describeTimeToLive(ttlRequest);
            String ttlStatus = ttlResult.getTimeToLiveDescription().getTimeToLiveStatus();

            return "ENABLED".equalsIgnoreCase(ttlStatus);
        } catch (Exception e) {
            logger.error("Failed to check TTL status for table {}: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    private static List<Map<String, String>> loadTableMappings(Map<String, Map<String, String>> config) {
        Object tablesObj = config.get("tables");
        if (tablesObj instanceof List) {
            @SuppressWarnings("unchecked")
            List<Map<String, String>> tableMappings = (List<Map<String, String>>) tablesObj;
            return tableMappings;
        } else {
            logger.error("Invalid format for 'tables' section in dynamo-scylla.yml. Expected a list of mappings.");
            return Collections.emptyList();
        }
    }

    private static MigrationResult migrateTable(DynamoDB dynamoDB, String sourceTableName, String targetTableName,
                                                String scyllaEndpoint, Map<String, String> attributeTypes, boolean isTtlEnabled, AmazonDynamoDB amazonDynamoDB) {
        int successCount = 0;
        int failureCount = 0;

        try {
            Table sourceTable = dynamoDB.getTable(sourceTableName);
            Iterable<Item> items = sourceTable.scan();

            if (!isTableExists(scyllaEndpoint, targetTableName)) {
                logger.error("Target table does not exist: {}", targetTableName);
                return new MigrationResult(0, 0);
            }

            if (isTtlEnabled) {
                enableTtlInScylla(scyllaEndpoint, targetTableName, sourceTableName, amazonDynamoDB);
                logger.info("TTL enabled for target table: {}", targetTableName);
            }

            for (Item item : items) {
                logger.info("Processing item: {}", item.toJSON());

                HttpRequest request = buildPutItemRequest(scyllaEndpoint, targetTableName, item, attributeTypes);
                HttpResponse<String> response = sendHttpRequest(request);

                if (response.statusCode() == 200) {
                    successCount++;
                } else {
                    failureCount++;
                    logger.warn("Failed to migrate item. Error: {}", response.body());
                }
            }

        } catch (Exception e) {
            logger.error("An error occurred while migrating table {}: {}", sourceTableName, e.getMessage(), e);
        }

        return new MigrationResult(successCount, failureCount);
    }

    private static String getTtlAttributeName(AmazonDynamoDB dynamoDBClient, String tableName) {
        try {
            DescribeTimeToLiveRequest ttlRequest = new DescribeTimeToLiveRequest().withTableName(tableName);
            DescribeTimeToLiveResult ttlResult = dynamoDBClient.describeTimeToLive(ttlRequest);

            if ("ENABLED".equalsIgnoreCase(ttlResult.getTimeToLiveDescription().getTimeToLiveStatus())) {
                return ttlResult.getTimeToLiveDescription().getAttributeName();
            } else {
                logger.warn("TTL is not enabled for table: {}", tableName);
            }
        } catch (Exception e) {
            logger.error("Failed to fetch TTL attribute name for table {}: {}", tableName, e.getMessage(), e);
        }
        return null;
    }

    private static void enableTtlInScylla(String scyllaEndpoint, String tableName, String sourceTableName, AmazonDynamoDB dynamoDBClient) {
        try {
            String ttlAttributeName = getTtlAttributeName(dynamoDBClient, sourceTableName);
            if (ttlAttributeName == null) {
                logger.warn("Source table {} does not have TTL enabled. Skipping TTL configuration for target table {}.", sourceTableName, tableName);
                return;
            }

            final long defaultTtlInSeconds = Instant.now().getEpochSecond() + 900L;

            String requestBody = String.format(
                    "{\"TableName\": \"%s\", \"TimeToLiveSpecification\": {\"Enabled\": true, \"AttributeName\": \"%s\", \"TimeToLiveSeconds\": \"%d\"}}",
                    tableName, ttlAttributeName, defaultTtlInSeconds);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(scyllaEndpoint))
                    .header("Content-Type", "application/x-amz-json-1.0")
                    .header("x-amz-target", "DynamoDB_20120810.UpdateTimeToLive")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                logger.info("TTL successfully enabled for table: {}", tableName);
            } else {
                logger.warn("Failed to enable TTL for table: {}. Response: {}", tableName, response.body());
            }
        } catch (Exception e) {
            logger.error("Failed to enable TTL for table {}: {}", tableName, e.getMessage(), e);
        }
    }


    private static Map<String, Map<String, String>> loadConfiguration() {
        try (InputStream inputStream = DynamoToScyllaMigrator.class.getClassLoader().getResourceAsStream("dynamo-scylla.yml")) {
            if (inputStream == null) {
                logger.error("Could not find dynamo-scylla.yml in resources.");
                return null;
            }
            Yaml yaml = new Yaml();
            Map<String, Map<String, String>> config = yaml.load(inputStream);
            if (config == null) {
                logger.error("YAML file is empty or could not be parsed.");
            }
            return config;
        } catch (Exception e) {
            logger.error("Failed to load configuration file: {}", e.getMessage(), e);
            return null;
        }
    }

    private static AmazonDynamoDB initializeDynamoDBClient(String accessKey, String secretKey, String region, String endpoint) {
        try {
            BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            return AmazonDynamoDBClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                    .build();
        } catch (Exception e) {
            logger.error("Failed to initialize DynamoDB client: {}", e.getMessage(), e);
            return null;
        }
    }

    private static Map<String, String> getAttributeTypes(AmazonDynamoDB dynamoDBClient, String tableName) {
        DescribeTableRequest request = new DescribeTableRequest().withTableName(tableName);
        DescribeTableResult result = dynamoDBClient.describeTable(request);
        List<AttributeDefinition> attributeDefinitions = result.getTable().getAttributeDefinitions();

        Map<String, String> attributeTypes = new HashMap<>();
        for (AttributeDefinition attributeDefinition : attributeDefinitions) {
            attributeTypes.put(attributeDefinition.getAttributeName(), attributeDefinition.getAttributeType());
        }
        return attributeTypes;
    }

    private static boolean isTableExists(String scyllaEndpoint, String tableName) {
        try {
            String requestBody = "{\"TableName\": \"" + tableName + "\"}";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(scyllaEndpoint))
                    .header("Content-Type", "application/x-amz-json-1.0")
                    .header("x-amz-target", "DynamoDB_20120810.DescribeTable")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            return response.statusCode() == 200 && response.body().contains("\"Table\"");
        } catch (Exception e) {
            logger.error("Error checking for table existence in ScyllaDB: {}", e.getMessage(), e);
            return false;
        }
    }

    private static HttpRequest buildPutItemRequest(String scyllaEndpoint, String targetTableName, Item item, Map<String, String> attributeTypes) throws Exception {
        String jsonRequestBody = buildPutItemRequestBody(targetTableName, item, attributeTypes);
        return HttpRequest.newBuilder()
                .uri(URI.create(scyllaEndpoint))
                .header("Content-Type", "application/x-amz-json-1.0")
                .header("x-amz-target", "DynamoDB_20120810.PutItem")
                .POST(HttpRequest.BodyPublishers.ofString(jsonRequestBody))
                .build();
    }

    private static HttpResponse<String> sendHttpRequest(HttpRequest request) throws Exception {
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void logMigrationSummary(int successCount, int failureCount) {
        logger.info("Migration completed.");
        logger.info("Total items processed: {}", successCount + failureCount);
        logger.info("Successfully migrated items: {}", successCount);
        logger.info("Failed to migrate items: {}", failureCount);
    }

    public static String buildPutItemRequestBody(String tableName, Item item, Map<String, String> attributeTypes) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("TableName cannot be null or empty");
        }
        if (item == null) {
            throw new IllegalArgumentException("Item cannot be null or empty");
        }

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("TableName", tableName);
        Map<String, Map<String, Object>> itemAttributes = new HashMap<>();

        for (Map.Entry<String, Object> entry : item.attributes()) {
            String attributeName = entry.getKey();
            Object attributeValue = entry.getValue();
            String attributeType = inferAttributeType(attributeValue, attributeTypes.get(attributeName));

            Map<String, Object> attributeMap = new HashMap<>();
            if (attributeValue == null) {
                attributeMap.put("NULL", true); // Ensure nulls are mapped correctly
            } else if (attributeValue instanceof Collection) {
                attributeMap.put("L", convertList((Collection<?>) attributeValue));
            } else if (attributeValue instanceof Map) {
                attributeMap.put("M", convertMap((Map<?, ?>) attributeValue));
            } else {
                convertSimpleType(attributeMap, attributeValue, attributeType);
            }
            itemAttributes.put(attributeName, attributeMap);
        }

        requestBody.put("Item", itemAttributes);
        return objectMapper.writeValueAsString(requestBody);
    }

    private static String inferAttributeType(Object value, String definedType) {
        if (value == null) return "NULL"; // Explicitly handle null values
        if (definedType != null) return definedType;
        if (value instanceof String) return "S";
        if (value instanceof Number) return "N";
        if (value instanceof Boolean) return "BOOL";
        if (value instanceof Collection) return "L";
        if (value instanceof Map) return "M";
        if (value instanceof byte[]) return "B";
        throw new IllegalArgumentException("Unsupported type: " + value.getClass());
    }


    private static void convertSimpleType(Map<String, Object> attributeMap, Object value, String type) {
        if ("NULL".equals(type)) {
            attributeMap.put("NULL", true); // Handle null type explicitly
            return;
        }
        switch (type) {
            case "N":
                attributeMap.put("N", String.valueOf(value));
                break;
            case "S":
                attributeMap.put("S", String.valueOf(value));
                break;
            case "BOOL":
                attributeMap.put("BOOL", value);
                break;
            case "B":
                attributeMap.put("B", Base64.getEncoder().encodeToString((byte[]) value));
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }


    private static List<Map<String, Object>> convertList(Collection<?> list) {
        return list.stream()
                .map(item -> {
                    Map<String, Object> elementMap = new HashMap<>();
                    String type = inferAttributeType(item, null);
                    if (item instanceof Collection) {
                        elementMap.put("L", convertList((Collection<?>) item));
                    } else if (item instanceof Map) {
                        elementMap.put("M", convertMap((Map<?, ?>) item));
                    } else {
                        convertSimpleType(elementMap, item, type);
                    }
                    return elementMap;
                })
                .collect(Collectors.toList());
    }

    private static Map<String, Map<String, Object>> convertMap(Map<?, ?> map) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        map.forEach((key, value) -> {
            Map<String, Object> elementMap = new HashMap<>();
            String type = inferAttributeType(value, null);
            if (value instanceof Collection) {
                elementMap.put("L", convertList((Collection<?>) value));
            } else {
                convertSimpleType(elementMap, value, type);
            }
            result.put(key.toString(), elementMap);
        });
        return result;
    }
}