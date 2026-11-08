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

import com.google.common.base.Strings;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.spark.dialect.SparkCatalogBridge;
import com.starrocks.spark.dialect.SparkCatalogs;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * StarRocks catalogs under their Spark names.
 *
 * <p>The {@code spark_catalog_mapping} session variable maps Spark catalog names to StarRocks catalogs, e.g.
 * {@code spark_catalog=hive_prod;iceberg_prod=iceberg}. Use the names the Spark jobs that share these catalogs
 * use: Spark views record them, and Spark's DDL behaves differently in spark_catalog. Without a mapping,
 * spark_catalog is the session's current StarRocks catalog and every other StarRocks catalog keeps its own name.
 */
public class StarRocksSparkCatalogs implements SparkCatalogs {
    private final ConnectContext context;
    private final Map<String, String> mapping;
    private final String sessionCatalogName;
    private final String currentCatalog;
    private final String currentDatabase;
    private final Map<String, SparkCatalogBridge> bridges = new HashMap<>();

    private StarRocksSparkCatalogs(ConnectContext context, Map<String, String> mapping, String sessionCatalogName,
                                   String currentStarRocksCatalog, String currentDatabase) {
        this.context = context;
        this.mapping = mapping;
        this.sessionCatalogName = sessionCatalogName;
        this.currentCatalog = currentStarRocksCatalog == null ? SESSION_CATALOG : sparkName(currentStarRocksCatalog);
        this.currentDatabase = currentDatabase;
    }

    /** Catalogs for a statement of the session: unqualified names resolve in its current catalog and database. */
    public static StarRocksSparkCatalogs forSession(ConnectContext context, SessionVariable sessionVariable) {
        Map<String, String> mapping = parseMapping(sessionVariable.getSparkCatalogMapping());
        String sessionCatalog = mapping.getOrDefault(SESSION_CATALOG, context.getCurrentCatalog());
        return new StarRocksSparkCatalogs(context, mapping, sessionCatalog, context.getCurrentCatalog(),
                Strings.emptyToNull(context.getDatabase()));
    }

    /**
     * Catalogs for expanding a view stored in {@code viewCatalog}. A Spark view stored in a Hive metastore was
     * created in Spark's spark_catalog, so for a Hive view that catalog is spark_catalog, whatever the mapping says.
     */
    public static StarRocksSparkCatalogs forView(ConnectContext context, SessionVariable sessionVariable,
                                                 String viewCatalog, boolean viewCatalogIsSessionCatalog) {
        Map<String, String> mapping = parseMapping(sessionVariable.getSparkCatalogMapping());
        String sessionCatalog = viewCatalogIsSessionCatalog ? viewCatalog
                : mapping.getOrDefault(SESSION_CATALOG, context.getCurrentCatalog());
        return new StarRocksSparkCatalogs(context, mapping, sessionCatalog, null, null);
    }

    /** Spark name of a StarRocks catalog. */
    public String sparkName(String starRocksCatalog) {
        if (starRocksCatalog.equals(sessionCatalogName)) {
            return SESSION_CATALOG;
        }
        for (Map.Entry<String, String> entry : mapping.entrySet()) {
            if (entry.getValue().equals(starRocksCatalog)) {
                return entry.getKey();
            }
        }
        return starRocksCatalog;
    }

    @Override
    public SparkCatalogBridge sessionCatalog() {
        return bridge(sessionCatalogName);
    }

    @Override
    public SparkCatalogBridge catalog(String sparkName) {
        String starRocksCatalog = mapping.get(sparkName.toLowerCase(Locale.ROOT));
        if (starRocksCatalog == null) {
            if (!GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists(sparkName)) {
                return null;
            }
            starRocksCatalog = sparkName;
        }
        return bridge(starRocksCatalog);
    }

    @Override
    public String currentCatalog() {
        return currentCatalog;
    }

    @Override
    public String currentDatabase() {
        return currentDatabase;
    }

    private SparkCatalogBridge bridge(String starRocksCatalog) {
        return bridges.computeIfAbsent(starRocksCatalog, name -> new StarRocksSparkCatalogBridge(context, name));
    }

    /** {@code spark_name=starrocks_catalog;...}, Spark names case-insensitive. */
    static Map<String, String> parseMapping(String value) {
        Map<String, String> mapping = new LinkedHashMap<>();
        if (Strings.isNullOrEmpty(value)) {
            return mapping;
        }
        for (String entry : value.split(";")) {
            int eq = entry.indexOf('=');
            if (eq > 0) {
                mapping.put(entry.substring(0, eq).trim().toLowerCase(Locale.ROOT), entry.substring(eq + 1).trim());
            }
        }
        return mapping;
    }
}
