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

    private ClickHouseFieldDescriptor(String name, String type, String defaultType, String defaultExpression, String comment, String codecExpression, String ttlExpression, boolean isSubcolumn) {
        this.name = name;
        this.type = type;
        this.defaultType = defaultType;
        this.defaultExpression = defaultExpression;
        this.comment = comment;
        this.codecExpression = codecExpression;
        this.ttlExpression = ttlExpression;
        this.isSubcolumn = isSubcolumn;
    }
    public boolean isAlias() {
        return "ALIAS".equals(defaultType);
    }

    public boolean isMaterialized() {
        return "MATERIALIZED".equals(defaultType);
    }

    public boolean hasDefault() {
        return "DEFAULT".equals(defaultType);
    }

    public static ClickHouseFieldDescriptor fromJsonRow(String json) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json.replace("\n", "\\n"), ClickHouseFieldDescriptor.class);
    }

    public static ClickHouseFieldDescriptor fromValues(String name, String type, String defaultType, String defaultExpression, String comment, String codecExpression, String ttlExpression, boolean isSubcolumn) {
        return new ClickHouseFieldDescriptor(name, type, defaultType, defaultExpression, comment, codecExpression, ttlExpression, isSubcolumn);
    }
}
