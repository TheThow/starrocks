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

import java.util.List;

/**
 * Metadata the Spark analyzer resolves names against. One bridge maps one StarRocks catalog onto Spark's
 * session catalog ({@code spark_catalog}): Spark's {@code db.tbl} names are looked up in that StarRocks catalog,
 * and generated StarRocks SQL qualifies every table with {@link #catalogName()}.
 */
public interface SparkCatalogBridge {

    /** StarRocks catalog used to qualify table names in the generated SQL. */
    String catalogName();

    /** Connector type of the catalog, e.g. "hive", "iceberg" or "internal". Drives DDL property mapping. */
    String catalogType();

    /** Current database of the StarRocks session, or null if none is selected. */
    String currentDatabase();

    boolean databaseExists(String db);

    List<String> listDatabases();

    List<String> listTables(String db);

    /** Returns the table or view, or null if it does not exist. */
    SparkTableInfo getTable(String db, String table);
}
