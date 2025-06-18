package org.scylla;

public class MigrationResult {
    int successCount;
    int failureCount;

    public MigrationResult(int successCount, int failureCount) {
        this.successCount = successCount;
        this.failureCount = failureCount;
    }
}
