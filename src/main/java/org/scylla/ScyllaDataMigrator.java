/**
 * ScyllaDataMigrator is a utility class for migrating data between ScyllaDB tables
 * across different clusters using the DynamoDB-compatible API. This class reads
 * configuration from a YAML file and performs cross-cluster data migration from a
 * source table in one ScyllaDB cluster to a destination table in another ScyllaDB cluster.
 *
 * <p>Configuration is loaded from 'scylla_mig.yml' file in the classpath which should
 * contain both source and target ScyllaDB cluster endpoints along with table names.</p>
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

public class ScyllaDataMigrator {
    private static final Logger logger = LoggerFactory.getLogger(ScyllaDataMigrator.class);

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

        String sourceEndpoint = scyllaConfig.get("sourceEndpoint");
        String targetEndpoint = scyllaConfig.get("targetEndpoint");
        String region = scyllaConfig.getOrDefault("region", "ap-south-1");
        String sourceTableName = tablesConfig.get("source");
        String targetTableName = tablesConfig.get("destination");

        if (sourceEndpoint == null || targetEndpoint == null || sourceTableName == null || targetTableName == null) {
            logger.error("Missing required parameters: sourceEndpoint, targetEndpoint, source table, or destination table.");
            return;
        }

        AmazonDynamoDB sourceDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(sourceEndpoint, region))
                .build();

        AmazonDynamoDB targetDynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(targetEndpoint, region))
                .build();

        DynamoDB sourceDynamoDB = new DynamoDB(sourceDynamoDBClient);
        DynamoDB targetDynamoDB = new DynamoDB(targetDynamoDBClient);

        Table sourceTable = sourceDynamoDB.getTable(sourceTableName);
        Table targetTable = targetDynamoDB.getTable(targetTableName);
        logger.info("Migrating data to table: {}", targetTableName);

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
        sourceDynamoDBClient.shutdown();
        targetDynamoDBClient.shutdown();
    }

    private static Map<String, Map<String, String>> loadConfiguration() {
        try (InputStream inputStream = ScyllaDataMigrator.class.getClassLoader().getResourceAsStream("scylla_mig.yml")) {
            if (inputStream == null) {
                logger.error("Could not find scylla_mig.yml in resources.");
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
