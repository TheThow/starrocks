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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Engine-neutral description of a table or view, as handed to the Spark analyzer.
 * Column types are Spark DDL type strings, e.g. {@code decimal(10,2)} or {@code array<struct<a:int>>}.
 */
public final class SparkTableInfo {

    public record Column(String name, String sparkType, String comment) {
        public Column {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(sparkType, "sparkType");
        }
    }

    private final String database;
    private final String name;
    private final boolean view;
    // Data columns followed by partition columns, the order Spark's CatalogTable expects.
    private final List<Column> columns = new ArrayList<>();
    private final List<String> partitionColumns = new ArrayList<>();
    // Spark provider, e.g. "hive", "parquet", "iceberg".
    private String provider;
    private String location;
    private String comment;
    // View SQL in Spark syntax.
    private String viewText;
    // Raw table properties. For views created by Spark these carry view.sqlConfig.*, view.catalogAndNamespace.*
    // and view.query.out.*, which Spark uses to analyze the view exactly as it was created.
    private final Map<String, String> properties = new HashMap<>();

    private SparkTableInfo(String database, String name, boolean view) {
        this.database = Objects.requireNonNull(database, "database");
        this.name = Objects.requireNonNull(name, "name");
        this.view = view;
    }

    public static SparkTableInfo table(String database, String name) {
        return new SparkTableInfo(database, name, false);
    }

    public static SparkTableInfo view(String database, String name, String viewText) {
        SparkTableInfo info = new SparkTableInfo(database, name, true);
        info.viewText = Objects.requireNonNull(viewText, "viewText");
        return info;
    }

    public SparkTableInfo addColumn(String columnName, String sparkType, String columnComment) {
        columns.add(new Column(columnName, sparkType, columnComment));
        return this;
    }

    public SparkTableInfo setPartitionColumns(List<String> names) {
        partitionColumns.clear();
        partitionColumns.addAll(names);
        return this;
    }

    public SparkTableInfo setProvider(String provider) {
        this.provider = provider;
        return this;
    }

    public SparkTableInfo setLocation(String location) {
        this.location = location;
        return this;
    }

    public SparkTableInfo setComment(String comment) {
        this.comment = comment;
        return this;
    }

    public SparkTableInfo putProperties(Map<String, String> props) {
        if (props != null) {
            properties.putAll(props);
        }
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public String getName() {
        return name;
    }

    public boolean isView() {
        return view;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public List<String> getPartitionColumns() {
        return partitionColumns;
    }

    public String getProvider() {
        return provider;
    }

    public String getLocation() {
        return location;
    }

    public String getComment() {
        return comment;
    }

    public String getViewText() {
        return viewText;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
