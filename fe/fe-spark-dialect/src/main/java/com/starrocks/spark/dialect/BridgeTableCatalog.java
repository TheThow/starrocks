// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.spark.dialect;

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Read-only Spark v2 catalog over a {@link SparkCatalogBridge}, for every StarRocks catalog other than the one
 * that plays {@code spark_catalog}. Views are not tables in Spark 3.5's v2 API; {@link ResolveCatalogViews}
 * expands them, so {@link #loadTable} reports them as missing.
 */
final class BridgeTableCatalog implements TableCatalog, SupportsNamespaces {
    private final String sparkName;
    private final SparkCatalogBridge bridge;

    BridgeTableCatalog(String sparkName, SparkCatalogBridge bridge) {
        this.sparkName = sparkName;
        this.bridge = bridge;
    }

    SparkCatalogBridge bridge() {
        return bridge;
    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
    }

    @Override
    public String name() {
        return sparkName;
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length != 1 || !bridge.databaseExists(namespace[0])) {
            throw new NoSuchNamespaceException(namespace);
        }
        return bridge.listTables(namespace[0]).stream()
                .map(table -> Identifier.of(namespace, table))
                .toArray(Identifier[]::new);
    }

    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        SparkTableInfo info = lookup(ident);
        if (info == null || info.isView()) {
            throw new NoSuchTableException(ident);
        }
        return new BridgeTable(info);
    }

    /** The table or view behind an identifier, or null. */
    SparkTableInfo lookup(Identifier ident) {
        if (ident.namespace().length != 1) {
            return null;
        }
        return bridge.getTable(ident.namespace()[0], ident.name());
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                             Map<String, String> properties) {
        throw new UnsupportedOperationException("The Spark dialect never executes DDL");
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) {
        throw new UnsupportedOperationException("The Spark dialect never executes DDL");
    }

    @Override
    public boolean dropTable(Identifier ident) {
        throw new UnsupportedOperationException("The Spark dialect never executes DDL");
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent) {
        throw new UnsupportedOperationException("The Spark dialect never executes DDL");
    }

    @Override
    public String[][] listNamespaces() {
        return bridge.listDatabases().stream().map(db -> new String[] {db}).toArray(String[][]::new);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length == 0) {
            return listNamespaces();
        }
        if (namespace.length == 1 && bridge.databaseExists(namespace[0])) {
            return new String[0][];
        }
        throw new NoSuchNamespaceException(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
        if (namespace.length != 1 || !bridge.databaseExists(namespace[0])) {
            throw new NoSuchNamespaceException(namespace);
        }
        return new HashMap<>();
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) {
        throw new UnsupportedOperationException("The Spark dialect never executes DDL");
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) {
        throw new UnsupportedOperationException("The Spark dialect never executes DDL");
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) {
        throw new UnsupportedOperationException("The Spark dialect never executes DDL");
    }

    /** Schema and identity partitioning of a StarRocks table; enough for analysis, never read or written. */
    static final class BridgeTable implements Table {
        private final SparkTableInfo info;
        private final StructType schema;

        BridgeTable(SparkTableInfo info) {
            this.info = info;
            List<StructField> fields = new ArrayList<>();
            for (SparkTableInfo.Column column : info.getColumns()) {
                DataType type = CatalystSqlParser$.MODULE$.parseDataType(column.sparkType());
                StructField field = new StructField(column.name(), type, true, Metadata.empty());
                if (column.comment() != null && !column.comment().isEmpty()) {
                    field = field.withComment(column.comment());
                }
                fields.add(field);
            }
            this.schema = new StructType(fields.toArray(new StructField[0]));
        }

        @Override
        public String name() {
            return info.getDatabase() + "." + info.getName();
        }

        @Override
        @SuppressWarnings("deprecation")
        public StructType schema() {
            return schema;
        }

        @Override
        public Transform[] partitioning() {
            // Only the columns matter here (the static INSERT OVERWRITE check), not the transforms.
            List<String> columns = info.getPartitionColumns().isEmpty() ? info.getPartitionSourceColumns()
                    : info.getPartitionColumns();
            return columns.stream().map(Expressions::identity).toArray(Transform[]::new);
        }

        @Override
        public Map<String, String> properties() {
            Map<String, String> properties = new HashMap<>(info.getProperties());
            if (info.getProvider() != null) {
                properties.put(TableCatalog.PROP_PROVIDER, info.getProvider());
            }
            return properties;
        }

        @Override
        public Set<TableCapability> capabilities() {
            return Set.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.TRUNCATE,
                    TableCapability.OVERWRITE_BY_FILTER, TableCapability.OVERWRITE_DYNAMIC);
        }
    }
}
