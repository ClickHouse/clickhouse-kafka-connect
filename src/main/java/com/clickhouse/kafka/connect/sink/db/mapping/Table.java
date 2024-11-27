package com.clickhouse.kafka.connect.sink.db.mapping;

import com.clickhouse.kafka.connect.util.Utils;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
public class Table {
    private static final Logger LOGGER = LoggerFactory.getLogger(Table.class);
    private static final Predicate<String> SIZE_FIELD_MATCHER = Pattern.compile(".+\\.size[0-9]+$").asMatchPredicate();
    private static final Pattern MULTIPLE_MAP_VALUES_PATTERN = Pattern.compile("(\\.values)(?=((\\.values)+$))");

    private final String name;
    private final String database;

    private final List<Column> rootColumnsList;
    private final Map<String, Column> rootColumnsMap;
    private final List<Column> allColumnsList;
    private final Map<String, Column> allColumnsMap;

    @Setter
    @Accessors(fluent = true)
    private boolean hasDefaults;

    @Setter
    @Getter
    private int numColumns = 0;

    public Table(String database, String name) {
        this.database = database;
        this.name = name;
        this.rootColumnsList = new ArrayList<>();
        this.rootColumnsMap = new HashMap<>();

        this.allColumnsList = new ArrayList<>();
        this.allColumnsMap = new HashMap<>();
    }

    public Table(String database, String name, int numColumns) {
        this(database, name);
        this.numColumns = numColumns;
    }

    public String getCleanName() {
        return name;
    }
    public String getName() {
        return Utils.escapeName(name);
    }

    public String getFullName() {
        return Utils.escapeTableName(database, name);
    }

    private void registerValidColumn(Column column) {
        allColumnsMap.put(column.getName(), column);
        allColumnsList.add(column);
    }

    public void addColumn(Column column) {
        registerValidColumn(column);

        if (column.isSubColumn()) handleNonRoot(column);
        else {
            rootColumnsList.add(column);
            rootColumnsMap.put(column.getName(), column);
        }
    }

    private void handleNonRoot(Column column) {
        String parentName = column.getName().substring(0, column.getName().lastIndexOf("."));
        Column parent = allColumnsMap.getOrDefault(parentName, null);
        if (parent == null) {
            LOGGER.error("Got non-root column, but its parent was not found to be updated. {}", column);
            return;
        }

        updateParent(parent, column);
    }

    private void updateParent(Column parent, Column child) {
        switch (parent.getType()) {
            case VARIANT:
                // Variants are handled fully in the Column class because its types are always primitive. Let's ignore them here.
                return;
            case ARRAY:
                if (SIZE_FIELD_MATCHER.test(child.getName()))
                    return;

                Column parentArrayType = parent.getArrayType();
                switch (parentArrayType.getType()) {
                    case MAP:
                    case TUPLE:
                        updateParent(parent.getArrayType(), child.getArrayType());
                        return;
                    case ARRAY:
                        do {
                            child = child.getArrayType();
                            parent = parent.getArrayType();
                        } while (child.getType() == Type.ARRAY && parent.getType() == Type.ARRAY);
                        updateParent(parent, child);
                        return;
                    case VARIANT:
                        return;
                    default:
                        LOGGER.error("Unhandled complex type '{}' as a child of an array", parentArrayType.getType());
                        return;
                }
            case MAP:
                // Keys are parsed fully in the Column class as its type is always primitive.
                if (child.getName().endsWith(".keys") || SIZE_FIELD_MATCHER.test(child.getName()))
                    return;

                if (child.getType() == Type.ARRAY && child.getName().endsWith(".values")) {
                    int depth = 1;

                    Matcher matcher = MULTIPLE_MAP_VALUES_PATTERN.matcher(child.getName());
                    while (matcher.find()) depth += 1;

                    int remainingDepth = depth;

                    // ClickHouse outputs nested maps values as nested array types
                    while (remainingDepth-- > 0) {
                        child = child.getArrayType();
                    }

                    child.setParent(parent);

                    parent.setMapDepth(depth);
                    parent.setMapValueType(child);
                    registerValidColumn(child);
                }
                return;
            case TUPLE:
                Column parentOfParent = parent.getParent();

                if (parentOfParent != null) {
                    boolean anyTransitionalParentIsMap = parentOfParent.getType() == Type.MAP;

                    if (!anyTransitionalParentIsMap && parentOfParent.getType() == Type.ARRAY) {
                        Column currentParent = parentOfParent.getParent();

                        while (currentParent != null) {
                            anyTransitionalParentIsMap = currentParent.getType() == Type.MAP;

                            if (anyTransitionalParentIsMap)
                                break;

                            currentParent = currentParent.getParent();
                        }
                    }

                    if (anyTransitionalParentIsMap) {
                        int remainingDepth = getRemainingDepth(parent, parentOfParent);

                        while (remainingDepth-- > 0) {
                            child = child.getArrayType();
                        }
                    }
                }
                parent.getTupleFields().add(child);
                return;
            default:
                if (child.getName().endsWith(".null")) {
                    LOGGER.debug("Ignoring complex column: {}", child);
                } else {
                    LOGGER.warn("Unsupported complex parent type: {}", parent.getType());
                }
        }
    }

    private static int getRemainingDepth(Column parent, Column parentOfParent) {
        int compensationDepth = 0;

        // I don't really know why the ClickHouse describe table result wraps the type in an additional
        // array only when the parent is a map which is under array. But we have to deal with it.
        Matcher matcher = MULTIPLE_MAP_VALUES_PATTERN.matcher(parent.getName());
        while (matcher.find()) compensationDepth += 1;

        return parentOfParent.getMapDepth() + parentOfParent.getArrayDepth() - compensationDepth;
    }
}
