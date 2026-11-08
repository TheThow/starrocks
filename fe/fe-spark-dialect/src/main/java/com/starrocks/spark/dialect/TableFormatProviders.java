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

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

/**
 * Stand-ins for the Iceberg and Paimon Spark runtimes, registered under their USING names
 * (META-INF/services/org.apache.spark.sql.sources.DataSourceRegister).
 *
 * <p>Spark decides how {@code CREATE TABLE ... USING x} in spark_catalog is analyzed by looking the provider up
 * on the classpath: a v2 provider keeps the statement a v2 CREATE TABLE (query column order, partition
 * transforms), which is what Spark does with the real runtimes installed. Without a registered provider the
 * statement fails. These classes only answer that lookup; they never read or write data.
 */
public final class TableFormatProviders {
    private TableFormatProviders() {
    }

    abstract static class Provider implements TableProvider, DataSourceRegister {
        @Override
        public StructType inferSchema(CaseInsensitiveStringMap options) {
            throw new UnsupportedOperationException("The Spark dialect never reads data");
        }

        @Override
        public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
            throw new UnsupportedOperationException("The Spark dialect never reads data");
        }
    }

    public static final class Iceberg extends Provider {
        @Override
        public String shortName() {
            return "iceberg";
        }
    }

    public static final class Paimon extends Provider {
        @Override
        public String shortName() {
            return "paimon";
        }
    }
}
