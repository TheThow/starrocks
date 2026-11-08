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

/**
 * The StarRocks catalogs a statement can see, under the names Spark SQL uses for them.
 *
 * <p>The names matter for semantics, not just lookup: Spark gives its built-in session catalog
 * ({@code spark_catalog}, normally the Hive metastore) different DDL behavior than every other catalog
 * (Hive storage defaults and partition columns moved last, versus v2 partition transforms and the query's column
 * order), and Spark views record the catalog names they were created in. Map them the way the Spark jobs that
 * share these catalogs are configured.
 */
public interface SparkCatalogs {
    String SESSION_CATALOG = "spark_catalog";

    /** The StarRocks catalog that plays Spark's {@code spark_catalog}. */
    SparkCatalogBridge sessionCatalog();

    /**
     * The StarRocks catalog Spark knows as {@code sparkName}, or null if there is none. Returning the session
     * catalog's bridge makes the name an alias of {@code spark_catalog}.
     */
    SparkCatalogBridge catalog(String sparkName);

    /** Spark name of the catalog unqualified names resolve in. */
    String currentCatalog();

    /** Database unqualified names resolve in, or null if none is selected. */
    String currentDatabase();
}
