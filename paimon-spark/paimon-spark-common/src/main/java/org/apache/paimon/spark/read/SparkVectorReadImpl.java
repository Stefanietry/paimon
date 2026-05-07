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

import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.VectorReadImpl;
import org.apache.paimon.table.source.VectorSearchSplit;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.paimon.utils.SerializableFunction;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class SparkVectorReadImpl extends VectorReadImpl implements Serializable {

    public SparkVectorReadImpl(
            FileStoreTable table,
            Predicate filter,
            int limit,
            DataField vectorColumn,
            float[] vector) {
        super(table, filter, limit, vectorColumn, vector);
    }

    @Override
    protected Iterator<Optional<ScoredGlobalIndexResult>> cal(
            String indexType,
            List<VectorSearchSplit> splits,
            Integer threadNum,
            RoaringNavigableMap64 preFilter) {
        List<byte[]> splitBytes = new ArrayList<>();
        for (VectorSearchSplit split : splits) {
            try {
                splitBytes.add(InstantiationUtil.serializeObject(split));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        List<Optional<byte[]>> res =
                new SparkEngineContext()
                        .map(
                                splitBytes,
                                (SerializableFunction<byte[], Optional<byte[]>>)
                                        split -> {
                                            return eval(
                                                    indexType,
                                                    split,
                                                    preFilter,
                                                    table,
                                                    vectorColumn,
                                                    vector,
                                                    limit);
                                        },
                                splits.size());
        List<Optional<ScoredGlobalIndexResult>> results = new ArrayList<>();
        for (Optional<byte[]> result : res) {
            if (result.isPresent()) {
                try {
                    results.add(
                            Optional.of(
                                    new GlobalIndexResultSerializer().deserialize(result.get())));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                results.add(Optional.empty());
            }
        }
        return results.iterator();
    }
}
