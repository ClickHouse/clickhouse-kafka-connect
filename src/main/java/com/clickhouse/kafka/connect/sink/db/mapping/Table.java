package com.clickhouse.kafka.connect.sink.db.mapping;

import com.clickhouse.kafka.connect.util.Utils;
import lombok.AccessLevel;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A table whose schema has been read from ClickHouse with DESCRIBE TABLE. */
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

    @Getter(AccessLevel.NONE)
    private final boolean hasDefaults;

    private final int numColumns;

    /**
     * @param columns the columns worth keeping, in the order ClickHouse described them
     * @param numColumns how many columns ClickHouse reported for this table, counting the aliases,
     *     materialized and ephemeral ones dropped from {@code columns}, so that it stays comparable to
     *     {@link TableDesc#getNumColumns()}. It has to be tallied from the same DESCRIBE response as
     *     {@code columns}: a count borrowed from any other query could describe a different schema
     *     version, and this description would then be cached as if it were already up to date.
     */
    public Table(String database, String name, boolean hasDefaults, List<Column> columns, int numColumns) {
        this.database = database;
        this.name = name;
        this.hasDefaults = hasDefaults;
        this.numColumns = numColumns;

        this.rootColumnsList = new ArrayList<>();
        this.rootColumnsMap = new HashMap<>();
        this.allColumnsList = new ArrayList<>();
        this.allColumnsMap = new HashMap<>();

        columns.forEach(this::addColumn);
    }

    public boolean hasDefaults() {
        return hasDefaults;
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

    private void addColumn(Column column) {
        registerValidColumn(column);

        if (column.isSubColumn()) handleNonRoot(column);
        else {
            rootColumnsList.add(column);
            rootColumnsMap.put(column.getName(), column);
        }
    }

    public Set<String> getMissingColumns(Collection<String> fieldNames) {
        Set<String> missing = new LinkedHashSet<>();
        for (String fieldName : fieldNames) {
            if (!rootColumnsMap.containsKey(fieldName)) {
                missing.add(fieldName);
            }
        }
        return missing;
    }

    private void handleNonRoot(Column column) {
        String parentName = column.getName().substring(0, column.getName().lastIndexOf("."));
        Column parent = allColumnsMap.getOrDefault(parentName, null);
        if (parent == null) {
            LOGGER.warn("Got non-root column, but its parent was not found to be updated. {}", column);
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
                final String childName = child.getName();
                if (SIZE_FIELD_MATCHER.test(childName) || childName.endsWith(".null"))
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
                        // nothing to do here. Only complex types require update of parent record with element types.
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
                // Log for troubleshooting what types were reached this point. Most of them should be ignored.
                if (child.getName().endsWith(".null")) {
                    LOGGER.debug("Ignoring complex column: {}", child);
                } else {
                    LOGGER.debug("Ignoring complex parent type: {} (parent name: '{}')",
                            parent.getType(), parent.getName());
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

    /**
     * A table ClickHouse listed but that has not been described: only its identity and the number of
     * columns the server reports for it are known. {@code extractTablesMapping} compares that count
     * against a described {@link Table}'s to decide whether a cached description is still current, so
     * both have to count the same thing: top-level columns, including aliases, materialized and
     * ephemeral ones.
     */
    public static class TableDesc {
        private final String database;
        private final String name;
        @Getter
        private final int numColumns;

        public TableDesc(String database, String name, int numColumns) {
            this.database = database;
            this.name = name;
            this.numColumns = numColumns;
        }

        public String getCleanName() {
            return name;
        }

        public String getFullName() {
            return Utils.escapeTableName(database, name);
        }
    }
}
