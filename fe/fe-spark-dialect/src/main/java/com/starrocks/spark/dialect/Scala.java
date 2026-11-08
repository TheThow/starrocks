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

import scala.Option;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction0;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** Small Java/Scala 2.12 interop helpers. */
final class Scala {
    private Scala() {
    }

    @SuppressWarnings("unchecked")
    static <T> List<T> list(Seq<? extends T> seq) {
        return (List<T>) JavaConverters.seqAsJavaList(seq);
    }

    static <T> Seq<T> seq(List<T> list) {
        return JavaConverters.asScalaBuffer(list).toSeq();
    }

    static <K, V> scala.collection.immutable.Map<K, V> map(Map<K, V> map) {
        return JavaConverters.mapAsScalaMap(map).toMap(Predef.<Tuple2<K, V>>$conforms());
    }

    static <K, V> Map<K, V> javaMap(scala.collection.Map<K, V> map) {
        return JavaConverters.mapAsJavaMap(map);
    }

    static <T> Option<T> option(T value) {
        return Option.apply(value);
    }

    static <T> T orNull(Option<T> option) {
        return option.isDefined() ? option.get() : null;
    }

    /**
     * Throws a checked exception without declaring it. Scala methods throw Spark's checked AnalysisException
     * subclasses without declaring them, so overrides of Scala methods must do the same.
     */
    @SuppressWarnings("unchecked")
    static <E extends Throwable> RuntimeException sneakyThrow(Throwable t) throws E {
        throw (E) t;
    }

    static <T> scala.Function0<T> fn0(Supplier<T> supplier) {
        return new AbstractFunction0<>() {
            @Override
            public T apply() {
                return supplier.get();
            }
        };
    }
}
