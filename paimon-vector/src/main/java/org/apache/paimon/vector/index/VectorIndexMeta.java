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

package org.apache.paimon.vector.index;

import org.apache.paimon.globalindex.IvfShard;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** Metadata for a vector index file. */
public class VectorIndexMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE_REF =
            new TypeReference<LinkedHashMap<String, String>>() {};

    private static final String SHARD_MODE = "shardMode";
    private static final String CENTROID = "centroid";
    private static final String ROW_ID_ENCODING = "rowIdEncoding";

    private final IvfShard shardMode;
    private final Integer centroid;
    private final RowIdEncoding rowIdEncoding;

    VectorIndexMeta() {
        this(null, null, null);
    }

    private VectorIndexMeta(IvfShard shardMode, Integer centroid, RowIdEncoding rowIdEncoding) {
        this.shardMode = shardMode;
        this.centroid = centroid;
        this.rowIdEncoding = rowIdEncoding;
        validate();
    }

    public static VectorIndexMeta centroidShard(int centroid) {
        if (centroid < 0) {
            throw new IllegalArgumentException("centroid must not be negative: " + centroid);
        }
        return new VectorIndexMeta(
                IvfShard.CENTROID_BASED, centroid, RowIdEncoding.ABSOLUTE_ROW_ID);
    }

    public static VectorIndexMeta centroidRoutingModel() {
        return new VectorIndexMeta(IvfShard.CENTROID_BASED, null, null);
    }

    public byte[] serialize() throws IOException {
        Map<String, String> map = new LinkedHashMap<>();
        if (shardMode != null) {
            map.put(SHARD_MODE, shardMode.optionValue());
        }
        if (centroid != null) {
            map.put(CENTROID, String.valueOf(centroid));
        }
        if (rowIdEncoding != null) {
            map.put(ROW_ID_ENCODING, rowIdEncoding.value());
        }
        return OBJECT_MAPPER.writeValueAsBytes(map);
    }

    public static VectorIndexMeta deserialize(byte[] data) throws IOException {
        Map<String, String> map = OBJECT_MAPPER.readValue(data, MAP_TYPE_REF);
        String shardMode = map.get(SHARD_MODE);
        String centroid = map.get(CENTROID);
        String rowIdEncoding = map.get(ROW_ID_ENCODING);
        return new VectorIndexMeta(
                parseShardMode(shardMode),
                centroid == null ? null : Integer.valueOf(centroid),
                RowIdEncoding.fromValue(rowIdEncoding));
    }

    private void validate() {
        if (centroid != null && shardMode != IvfShard.CENTROID_BASED) {
            throw new IllegalArgumentException("Centroid requires centroid-based IVF shard mode.");
        }
        if (rowIdEncoding != null && centroid == null) {
            throw new IllegalArgumentException("Row id encoding requires a centroid shard.");
        }
    }

    private static IvfShard parseShardMode(String value) {
        if (value == null) {
            return null;
        }
        IvfShard shardMode = IvfShard.fromValue(value);
        if (shardMode == null) {
            throw new IllegalArgumentException("Unsupported vector index shard mode: " + value);
        }
        return shardMode;
    }

    public boolean isCentroidShard() {
        return shardMode == IvfShard.CENTROID_BASED && centroid != null;
    }

    public IvfShard shardMode() {
        return shardMode;
    }

    public Integer centroid() {
        return centroid;
    }

    public RowIdEncoding rowIdEncoding() {
        return rowIdEncoding;
    }
}
