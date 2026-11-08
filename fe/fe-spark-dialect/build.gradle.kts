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

// Spark SQL dialect support: parses and analyzes Spark SQL with Spark's own parser and analyzer
// (no SparkSession/SparkContext) and renders the resolved plan as StarRocks SQL.
// It depends on Spark only, not on fe-core, so it can also be embedded in an external service.

plugins {
    java
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

group = "com.starrocks"

val sparkExcludes: Action<ExternalModuleDependency> = Action {
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
    exclude(group = "org.apache.zookeeper", module = "zookeeper")
    exclude(group = "org.apache.logging.log4j", module = "log4j-slf4j2-impl")
    exclude(group = "org.apache.hadoop", module = "hadoop-client")
    exclude(group = "org.apache.hadoop", module = "hadoop-client-api")
    exclude(group = "org.apache.hadoop", module = "hadoop-client-runtime")
    exclude(group = "org.apache.ivy", module = "ivy")
    exclude(group = "log4j", module = "log4j")
    exclude(group = "com.clearspring.analytics", module = "stream")
    exclude(group = "org.apache.commons", module = "commons-compress")
    exclude(group = "com.google.protobuf", module = "protobuf-java")
    exclude(group = "org.eclipse.jetty")
    exclude(group = "org.lz4", module = "lz4-java")
}

dependencies {
    implementation("org.apache.spark:spark-core_2.12", sparkExcludes)
    implementation("org.apache.spark:spark-catalyst_2.12", sparkExcludes)
    implementation("org.apache.spark:spark-sql_2.12", sparkExcludes)
    implementation("org.scala-lang:scala-library")
    // Spark's Hadoop-typed APIs (SessionCatalog, InMemoryCatalog, the built-in file formats Spark looks up for
    // USING parquet/orc) need Hadoop on the classpath; fe-core already ships hadoop-client.
    compileOnly("org.apache.hadoop:hadoop-client:${project.ext["hadoop.version"]}")
    implementation("org.apache.logging.log4j:log4j-api")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

// Prints the runtime classpath so the module can be smoke-run on its own.
tasks.register("printRuntimeClasspath") {
    doLast {
        println((sourceSets["main"].runtimeClasspath + configurations["compileClasspath"]).asPath)
    }
}
