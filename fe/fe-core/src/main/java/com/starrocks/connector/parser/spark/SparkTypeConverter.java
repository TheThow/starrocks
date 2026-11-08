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

import com.starrocks.spark.dialect.SparkDialectException;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;

import java.util.StringJoiner;

/** StarRocks column types as Spark DDL type strings, for the Spark analyzer's view of StarRocks tables. */
public final class SparkTypeConverter {
    private SparkTypeConverter() {
    }

    public static String toSparkType(Type type) {
        if (type.isArrayType()) {
            return "array<" + toSparkType(((ArrayType) type).getItemType()) + ">";
        } else if (type.isMapType()) {
            MapType map = (MapType) type;
            return "map<" + toSparkType(map.getKeyType()) + "," + toSparkType(map.getValueType()) + ">";
        } else if (type.isStructType()) {
            StringJoiner fields = new StringJoiner(",", "struct<", ">");
            for (StructField field : ((StructType) type).getFields()) {
                fields.add("`" + field.getName().replace("`", "``") + "`:" + toSparkType(field.getType()));
            }
            return fields.toString();
        }
        if (!(type instanceof ScalarType scalar)) {
            throw new SparkDialectException("Type " + type + " has no Spark equivalent");
        }
        switch (scalar.getPrimitiveType()) {
            case BOOLEAN:
                return "boolean";
            case TINYINT:
                return "tinyint";
            case SMALLINT:
                return "smallint";
            case INT:
                return "int";
            case BIGINT:
                return "bigint";
            case LARGEINT:
                return "decimal(38,0)";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return "decimal(" + scalar.decimalPrecision() + "," + scalar.decimalScale() + ")";
            case DATE:
                return "date";
            case DATETIME:
                return "timestamp";
            case CHAR:
                return "char(" + scalar.getLength() + ")";
            case VARCHAR:
                // Hive STRING columns come back as the default-length VARCHAR.
                if (scalar.isWildcardVarchar() || scalar.getLength() >= StringType.DEFAULT_STRING_LENGTH) {
                    return "string";
                }
                return "varchar(" + scalar.getLength() + ")";
            case JSON:
                return "string";
            case BINARY:
            case VARBINARY:
                return "binary";
            default:
                throw new SparkDialectException("Type " + type + " has no Spark equivalent");
        }
    }
}
