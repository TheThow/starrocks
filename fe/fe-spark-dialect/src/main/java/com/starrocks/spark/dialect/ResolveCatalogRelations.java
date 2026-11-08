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

import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation;
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.command.DDLUtils;
import scala.Function1;
import scala.runtime.AbstractPartialFunction;

/**
 * Replaces Spark's FindDataSourceTable, which needs a SparkSession because it builds a file-backed relation.
 * The analyzer only needs the schema, so every catalog table becomes a {@code HiveTableRelation}: a resolved
 * Catalyst leaf that carries the table's data and partition columns and nothing else.
 */
final class ResolveCatalogRelations extends Rule<LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.resolveOperatorsUp(new AbstractPartialFunction<>() {
            @Override
            public boolean isDefinedAt(LogicalPlan node) {
                return node instanceof UnresolvedCatalogRelation
                        || (node instanceof InsertIntoStatement insert
                        && insert.table() instanceof UnresolvedCatalogRelation);
            }

            @Override
            public <A1 extends LogicalPlan, B1> B1 applyOrElse(A1 node, Function1<A1, B1> fallback) {
                LogicalPlan resolved;
                if (node instanceof UnresolvedCatalogRelation relation) {
                    resolved = DDLUtils.readHiveTable(relation.tableMeta());
                } else if (node instanceof InsertIntoStatement insert
                        && insert.table() instanceof UnresolvedCatalogRelation target) {
                    // The insert target is not a child of the statement, so it is never visited on its own.
                    resolved = insert.copy(DDLUtils.readHiveTable(target.tableMeta()), insert.partitionSpec(),
                            insert.userSpecifiedCols(), insert.query(), insert.overwrite(),
                            insert.ifPartitionNotExists(), insert.byName());
                } else {
                    return fallback.apply(node);
                }
                @SuppressWarnings("unchecked")
                B1 result = (B1) resolved;
                return result;
            }
        });
    }
}
