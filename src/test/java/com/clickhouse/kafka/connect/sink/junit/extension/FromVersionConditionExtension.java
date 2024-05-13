package com.clickhouse.kafka.connect.sink.junit.extension;

import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.AnnotationUtils;

import java.util.List;
import java.util.Optional;

public class FromVersionConditionExtension implements BeforeTestExecutionCallback {

    @Override
    public void beforeTestExecution(ExtensionContext context) {
        Optional<SinceClickHouseVersion> optionalFromVersion = AnnotationUtils.findAnnotation(context.getElement(), SinceClickHouseVersion.class);
        if (optionalFromVersion.isPresent()) {
            String requiredVersion = optionalFromVersion.get().value();
            String currentVersion = System.getenv("CLICKHOUSE_VERSION");
            if (currentVersion == null) {
                // We assume latest if the version env is not set
                return;
            }
            if (compareVersions(currentVersion, requiredVersion) < 0) {
                throw new org.junit.AssumptionViolatedException("Test skipped because CLICKHOUSE_VERSION is lower than required");
            }
        }
    }

    private int compareVersions(String currentVersion, String requiredVersion) {
        if (List.of("latest", "cloud").contains(currentVersion))
            return 0;

        String[] currentParts = currentVersion.split("\\.");
        String[] requiredParts = requiredVersion.split("\\.");

        try {
            int length = Math.max(currentParts.length, requiredParts.length);
            for (int i = 0; i < length; i++) {
                int currentPart = i < currentParts.length ? Integer.parseInt(currentParts[i]) : 0;
                int requiredPart = i < requiredParts.length ? Integer.parseInt(requiredParts[i]) : 0;
                if (currentPart != requiredPart) {
                    return Integer.compare(currentPart, requiredPart);
                }
            }
        } catch (NumberFormatException e) {
            return 0;
        }
        return 0;
    }
}
