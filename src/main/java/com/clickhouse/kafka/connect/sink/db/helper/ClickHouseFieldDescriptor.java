package com.clickhouse.kafka.connect.sink.db.helper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

/**
 * Java object representation of one DESCRIBE TABLE result row.
 * <p>
 * We use Jackson to instantiate it from JSON.
 */
@Data
@Builder
@Jacksonized
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class ClickHouseFieldDescriptor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private String name;
    private String type;
    private String defaultType;
    private String defaultExpression;
    private String comment;
    private String codecExpression;
    private String ttlExpression;
    private boolean isSubcolumn;

    public boolean isAlias() {
        return "ALIAS".equals(defaultType);
    }

    public boolean isMaterialized() {
        return "MATERIALIZED".equals(defaultType);
    }

    public boolean isEphemeral() {
        return "EPHEMERAL".equals(defaultType);
    }

    public boolean hasDefault() {
        return "DEFAULT".equals(defaultType);
    }

    public static ClickHouseFieldDescriptor fromJsonRow(String json) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json.replace("\\", "\\\\").replace("\n", "\\n"), ClickHouseFieldDescriptor.class);
    }
}
