/**
 * ScyllaTableMigrator is a utility class for migrating data between two ScyllaDB tables
 * within the same cluster using the DynamoDB-compatible API. This class reads configuration
 * from a YAML file and performs bulk data migration from a source table to a destination
 * table on the same ScyllaDB endpoint.
 *
 * <p>The migrator performs a full table scan on the source table and copies all items
 * to the destination table within the same ScyllaDB cluster. It provides logging for
 * successful and failed migrations, making it suitable for production data migration
 * scenarios such as table restructuring, data archiving, or table consolidation.</p>
 *
 * <p>Configuration is loaded from 'scylla-config.yml' file in the classpath which should
 * contain the ScyllaDB cluster endpoint and source/destination table names.</p>
 *
 * @author Ajith Babu Kalaiselvan
 */

package org.scylla;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class ScyllaTableMigrator {
    private static final Logger logger = LoggerFactory.getLogger(ScyllaTableMigrator.class);

    public static void main(String[] args) {
        Map<String, Map<String, String>> config = loadConfiguration();
        if (config == null) {
            return;
        }

        Map<String, String> scyllaConfig = config.get("scylla");
        Map<String, String> tablesConfig = config.get("tables");

        if (scyllaConfig == null || tablesConfig == null) {
            logger.error("Missing required configuration sections: 'scylla' or 'tables'");
            return;
        }

        String endpoint = scyllaConfig.get("endpoint");
        String region = scyllaConfig.getOrDefault("region", "ap-south-1");
        String sourceTableName = tablesConfig.get("source");
        String targetTableName = tablesConfig.get("destination");

        if (endpoint == null || sourceTableName == null || targetTableName == null) {
            logger.error("Missing required parameters: endpoint, source table, or destination table.");
            return;
        }

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .build();

        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
        Table sourceTable = dynamoDB.getTable(sourceTableName);
        Table targetTable = dynamoDB.getTable(targetTableName);
        logger.info("Migrating data to {}", targetTableName);

        ScanSpec scanSpec = new ScanSpec();
        Iterator<Item> items = sourceTable.scan(scanSpec).iterator();
        int successCount = 0, failureCount = 0;

        while (items.hasNext()) {
            Item item = items.next();
            try {
                targetTable.putItem(item);
                successCount++;
            } catch (Exception e) {
                logger.error("Failed to migrate item {}: {}", item.toJSON(), e.getMessage());
                failureCount++;
            }
        }

        logger.info("Migration completed. Success: {}, Failures: {}", successCount, failureCount);
        dynamoDBClient.shutdown();
    }

    private static Map<String, Map<String, String>> loadConfiguration() {
        try (InputStream inputStream = ScyllaTableMigrator.class.getClassLoader().getResourceAsStream("scylla-config.yml")) {
            if (inputStream == null) {
                logger.error("Could not find scylla-config.yml in resources.");
                return null;
            }
            Yaml yaml = new Yaml();
            return yaml.load(inputStream);
        } catch (Exception e) {
            logger.error("Failed to load configuration file: {}", e.getMessage(), e);
            return null;
        }
    }
}
