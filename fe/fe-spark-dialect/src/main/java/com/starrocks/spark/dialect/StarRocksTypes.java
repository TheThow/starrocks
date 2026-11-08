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

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.VarcharType;

import java.util.StringJoiner;

/** Spark data types rendered as StarRocks types, for CAST targets and DDL. */
final class StarRocksTypes {
    private StarRocksTypes() {
    }

    static String of(DataType type) {
        if (type instanceof BooleanType) {
            return "BOOLEAN";
        } else if (type instanceof ByteType) {
            return "TINYINT";
        } else if (type instanceof ShortType) {
            return "SMALLINT";
        } else if (type instanceof IntegerType) {
            return "INT";
        } else if (type instanceof LongType) {
            return "BIGINT";
        } else if (type instanceof FloatType) {
            return "FLOAT";
        } else if (type instanceof DoubleType) {
            return "DOUBLE";
        } else if (type instanceof DecimalType decimal) {
            return "DECIMAL(" + decimal.precision() + "," + decimal.scale() + ")";
        } else if (type instanceof CharType charType) {
            return "CHAR(" + charType.length() + ")";
        } else if (type instanceof VarcharType varchar) {
            return "VARCHAR(" + varchar.length() + ")";
        } else if (type instanceof StringType) {
            return "STRING";
        } else if (type instanceof BinaryType) {
            return "VARBINARY";
        } else if (type instanceof DateType) {
            return "DATE";
        } else if (type instanceof TimestampType || type instanceof TimestampNTZType) {
            // Spark TIMESTAMP is an instant shown in spark.sql.session.timeZone; StarRocks DATETIME has no zone.
            // They agree as long as the StarRocks session time_zone equals the Spark session time zone,
            // which SparkSqlTranslator enforces by deriving one from the other.
            return "DATETIME";
        } else if (type instanceof ArrayType array) {
            return "ARRAY<" + of(array.elementType()) + ">";
        } else if (type instanceof MapType map) {
            return "MAP<" + of(map.keyType()) + "," + of(map.valueType()) + ">";
        } else if (type instanceof StructType struct) {
            StringJoiner fields = new StringJoiner(", ", "STRUCT<", ">");
            for (StructField field : struct.fields()) {
                fields.add(Sql.quote(field.name()) + " " + of(field.dataType()));
            }
            return fields.toString();
        } else if (type instanceof NullType) {
            return "BOOLEAN";
        }
        throw new SparkDialectException("Spark type " + type.simpleString() + " has no StarRocks equivalent");
    }
}
