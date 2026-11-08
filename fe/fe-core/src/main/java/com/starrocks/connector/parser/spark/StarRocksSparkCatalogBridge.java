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

package com.starrocks.connector.parser.spark;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HiveView;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.IcebergView;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.spark.dialect.SparkCatalogBridge;
import com.starrocks.spark.dialect.SparkDialectException;
import com.starrocks.spark.dialect.SparkTableInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Exposes one StarRocks catalog to the Spark analyzer as Spark's session catalog.
 * All lookups go through {@link MetadataMgr}, so they use StarRocks' metadata caches and the session's identity;
 * privileges are still checked by StarRocks when it analyzes the translated SQL.
 */
public class StarRocksSparkCatalogBridge implements SparkCatalogBridge {
    // Spark keeps the provider of its data source tables in this Hive table property.
    private static final String SPARK_PROVIDER_PROPERTY = "spark.sql.sources.provider";

    private final ConnectContext context;
    private final String catalogName;
    private final String currentDatabase;
    private final MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

    public StarRocksSparkCatalogBridge(ConnectContext context, String catalogName, String currentDatabase) {
        this.context = context;
        this.catalogName = catalogName;
        this.currentDatabase = currentDatabase;
    }

    @Override
    public String catalogName() {
        return catalogName;
    }

    @Override
    public String catalogType() {
        return GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogType(catalogName);
    }

    @Override
    public String currentDatabase() {
        return currentDatabase;
    }

    @Override
    public boolean databaseExists(String db) {
        return metadataMgr.getDb(context, catalogName, db) != null;
    }

    @Override
    public List<String> listDatabases() {
        return metadataMgr.listDbNames(context, catalogName);
    }

    @Override
    public List<String> listTables(String db) {
        return metadataMgr.listTableNames(context, catalogName, db);
    }

    @Override
    public SparkTableInfo getTable(String db, String tableName) {
        Table table = metadataMgr.getTable(context, catalogName, db, tableName);
        if (table == null) {
            return null;
        }
        if (table instanceof HiveView hiveView) {
            return hiveView(db, hiveView);
        } else if (table instanceof IcebergView icebergView) {
            return icebergView(db, icebergView);
        } else if (table.isView()) {
            throw new SparkDialectException("View " + db + "." + tableName + " is not defined in Spark SQL "
                    + "and cannot be used from the Spark dialect");
        }

        SparkTableInfo info = SparkTableInfo.table(db, table.getName());
        addColumns(info, table.getFullSchema());
        info.setComment(table.getComment());
        if (table instanceof HiveTable hiveTable) {
            Map<String, String> properties = hiveTable.getProperties();
            info.setPartitionColumns(hiveTable.getPartitionColumnNames())
                    .setProvider(properties.getOrDefault(SPARK_PROVIDER_PROPERTY, "hive"))
                    .setLocation(hiveTable.getTableLocation())
                    .putProperties(properties);
        } else if (table instanceof IcebergTable) {
            info.setProvider("iceberg");
        }
        return info;
    }

    private SparkTableInfo hiveView(String db, HiveView view) {
        if (view.getViewType() == HiveView.Type.Trino) {
            throw new SparkDialectException("Trino view " + db + "." + view.getName()
                    + " cannot be used from the Spark dialect");
        }
        // Spark views carry view.sqlConfig.*, view.catalogAndNamespace.* and view.query.out.* in their table
        // properties; Spark reads Hive-created views (without those) the way Hive does.
        SparkTableInfo info = SparkTableInfo.view(db, view.getName(), view.getInlineViewDef());
        addColumns(info, view.getFullSchema());
        return info.putProperties(view.getMetastoreParameters()).setComment(view.getComment());
    }

    private SparkTableInfo icebergView(String db, IcebergView view) {
        if (!view.isSparkDialect()) {
            throw new SparkDialectException("Iceberg view " + db + "." + view.getName()
                    + " has no Spark SQL representation");
        }
        List<Column> columns = view.getFullSchema();
        // Iceberg's Spark runtime maps the view query's columns to the view schema by position.
        // Spark's own view handling maps them by name, so rename them positionally first.
        StringJoiner aliases = new StringJoiner(", ");
        for (Column column : columns) {
            aliases.add(quote(column.getName()));
        }
        String text = "SELECT * FROM (" + view.getInlineViewDef() + ") AS __iceberg_view(" + aliases + ")";

        Map<String, String> properties = new HashMap<>();
        String defaultDb = view.getDefaultDbName() != null ? view.getDefaultDbName() : db;
        properties.put("view.catalogAndNamespace.numParts", "2");
        properties.put("view.catalogAndNamespace.part.0", "spark_catalog");
        properties.put("view.catalogAndNamespace.part.1", defaultDb);
        properties.put("view.query.out.numCols", Integer.toString(columns.size()));
        for (int i = 0; i < columns.size(); i++) {
            properties.put("view.query.out.col." + i, columns.get(i).getName());
        }

        SparkTableInfo info = SparkTableInfo.view(db, view.getName(), text);
        addColumns(info, columns);
        return info.putProperties(properties).setComment(view.getComment());
    }

    private static void addColumns(SparkTableInfo info, List<Column> columns) {
        for (Column column : new ArrayList<>(columns)) {
            info.addColumn(column.getName(), SparkTypeConverter.toSparkType(column.getType()), column.getComment());
        }
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
