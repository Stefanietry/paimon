/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.read;

import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.VectorReadImpl;
import org.apache.paimon.table.source.VectorSearchSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SerializableFunction;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class SparkVectorReadImpl extends VectorReadImpl implements Serializable {

    private final JavaSparkContext jsc;

    public SparkVectorReadImpl(
            FileStoreTable table,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector) {
        super(table, filter, limit, vectorColumn, vector);
        jsc = new JavaSparkContext(SparkSession.builder().getOrCreate().sparkContext());
    }

    @Override
    protected Iterator<Optional<ScoredGlobalIndexResult>> cal(
            GlobalIndexer globalIndexer,
            IndexPathFactory indexPathFactory,
            List<VectorSearchSplit> splits,
            Integer threadNum,
            RoaringNavigableMap64 preFilter) {
        return map(
                        splits,
                        (SerializableFunction<VectorSearchSplit, Optional<ScoredGlobalIndexResult>>)
                                split ->
                                        eval(
                                                globalIndexer,
                                                indexPathFactory,
                                                split.rowRangeStart(),
                                                split.rowRangeEnd(),
                                                split.vectorIndexFiles(),
                                                preFilter),
                        splits.size())
                .iterator();
    }

    public <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) {
        return jsc.parallelize(data, parallelism).map(func::apply).collect();
    }

    public <I, O> List<O> flatMap(
            List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism) {
        return jsc.parallelize(data, parallelism).flatMap(x -> func.apply(x).iterator()).collect();
    }
}
