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
import com.starrocks.spark.dialect.SparkSqlTranslator;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Entry points of the Spark SQL dialect: Spark SQL is analyzed by Spark (no SparkSession) against StarRocks
 * metadata and translated into StarRocks SQL that keeps Spark's semantics, which is then parsed as usual.
 */
public final class SparkParserUtils {
    private static final Logger LOG = LogManager.getLogger(SparkParserUtils.class);

    private SparkParserUtils() {
    }

    /** Translates one Spark statement, resolving names in the session's current catalog and database. */
    public static SparkSqlTranslator.Translation translate(String sparkSql, SessionVariable sessionVariable) {
        ConnectContext context = requireContext();
        SparkSqlTranslator translator = new SparkSqlTranslator(
                new StarRocksSparkCatalogBridge(context, context.getCurrentCatalog(), context.getDatabase()),
                sessionVariable.getTimeZone(), sparkConf(sessionVariable));
        SparkSqlTranslator.Translation translation = translator.translate(sparkSql);
        LOG.debug("Spark SQL [{}] translated to [{}]", sparkSql, translation.sql());
        return translation;
    }

    /** The StarRocks query for a view defined in Spark SQL, analyzed with the view's own Spark settings. */
    public static QueryStatement parseView(String catalogName, String dbName, String viewName,
                                           SessionVariable sessionVariable) {
        ConnectContext context = requireContext();
        SparkSqlTranslator translator = new SparkSqlTranslator(
                new StarRocksSparkCatalogBridge(context, catalogName, dbName),
                sessionVariable.getTimeZone(), sparkConf(sessionVariable));
        String sql = translator.translateView(dbName, viewName);
        LOG.debug("Spark view {}.{}.{} translated to [{}]", catalogName, dbName, viewName, sql);
        StatementBase statement = SqlParser.parseOneWithStarRocksDialect(sql, sessionVariable);
        if (!(statement instanceof QueryStatement queryStatement)) {
            throw new StarRocksPlannerException("Spark view " + viewName + " did not translate to a query",
                    ErrorType.INTERNAL_ERROR);
        }
        return queryStatement;
    }

    /** Spark settings from the {@code spark_sql_conf} session variable: {@code key=value;key=value}. */
    static Map<String, String> sparkConf(SessionVariable sessionVariable) {
        Map<String, String> conf = new HashMap<>();
        String value = sessionVariable.getSparkSqlConf();
        if (Strings.isNullOrEmpty(value)) {
            return conf;
        }
        for (String entry : value.split(";")) {
            int eq = entry.indexOf('=');
            if (eq > 0) {
                conf.put(entry.substring(0, eq).trim(), entry.substring(eq + 1).trim());
            }
        }
        return conf;
    }

    private static ConnectContext requireContext() {
        ConnectContext context = ConnectContext.get();
        if (context == null) {
            throw new StarRocksPlannerException("The Spark SQL dialect needs a session", ErrorType.INTERNAL_ERROR);
        }
        return context;
    }
}
