package org.scylla;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

public class ScyllaDeleteTool {

    private static final Logger log = LoggerFactory.getLogger(ScyllaDeleteTool.class);

    public static void main(String[] args) {
        Properties config = loadProperties();

        String tableName = config.getProperty("table.name");
        String filterAttribute = config.getProperty("filter.attribute");
        String endpoint = config.getProperty("endpoint.url");
        String region = config.getProperty("region");
        String accessKey = config.getProperty("aws.accessKey");
        String secretKey = config.getProperty("aws.secretKey");

        List<String> filterValues = Arrays.stream(
                        config.getProperty("filter.values", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();

        DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .endpointOverride(URI.create(endpoint))
                .build();

        deleteMatchingItems(dynamoDbClient, tableName, filterAttribute, filterValues);

        dynamoDbClient.close();
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = ScyllaDeleteTool.class.getClassLoader().getResourceAsStream("delete_config.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find configuration file: delete_config.properties");
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config file", e);
        }
        return props;
    }

    private static void deleteMatchingItems(DynamoDbClient client, String tableName, String filterAttribute, List<String> filterValues) {
        if (filterValues.isEmpty()) {
            System.err.println("No values provided for filtering.");
            return;
        }

        Map<String, String> expressionNames = Map.of("#attr", filterAttribute);
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        StringBuilder inClause = new StringBuilder();

        for (int i = 0; i < filterValues.size(); i++) {
            String placeholder = ":val" + i;
            inClause.append(placeholder).append(", ");
            expressionValues.put(placeholder, AttributeValue.builder().s(filterValues.get(i)).build());
        }
        inClause.setLength(inClause.length() - 2); // remove trailing comma and space

        String filterExpression = "#attr IN (" + inClause + ")";

        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(tableName)
                .filterExpression(filterExpression)
                .expressionAttributeNames(expressionNames)
                .expressionAttributeValues(expressionValues)
                .build();

        ScanResponse scanResponse = client.scan(scanRequest);
        List<Map<String, AttributeValue>> itemsToDelete = scanResponse.items();

        int totalItems = itemsToDelete.size();
        log.info("Deleting {} items from table: {}", totalItems, tableName);
        int deletedCount = 0;

        List<KeySchemaElement> keySchema = client.describeTable(DescribeTableRequest.builder()
                .tableName(tableName)
                .build()).table().keySchema();

        List<String> keyAttributes = keySchema.stream()
                .map(KeySchemaElement::attributeName)
                .toList();

        for (Map<String, AttributeValue> item : itemsToDelete) {
            Map<String, AttributeValue> key = new HashMap<>();
            for (String keyAttr : keyAttributes) {
                key.put(keyAttr, item.get(keyAttr));
            }

            try {
                client.deleteItem(DeleteItemRequest.builder()
                        .tableName(tableName)
                        .key(key)
                        .build());
                deletedCount++;
            } catch (DynamoDbException e) {
                log.error("Failed to delete item with key {}: {}", key, e.getMessage(), e);
            }
        }

        // ANSI escape codes for colored output
        String GREEN = "\u001B[32m";
        String CYAN = "\u001B[36m";
        String RESET = "\u001B[0m";

        System.out.println("======================================");
        System.out.println(CYAN + "🔍 Found " + totalItems + " item(s) to delete." + RESET);
        System.out.println(GREEN + "✅ Successfully deleted " + deletedCount + " item(s)." + RESET);
        System.out.println("======================================");
    }
}
