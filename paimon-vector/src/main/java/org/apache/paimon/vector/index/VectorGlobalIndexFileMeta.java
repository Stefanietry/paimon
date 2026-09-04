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

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/** Partition-level envelope metadata stored in the vector global-index file. */
public class VectorGlobalIndexFileMeta implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final int VERSION = 1;
    private static final String VERSION_FIELD = "version";
    private static final String INDEX_TYPE = "indexType";
    private static final String SHARD_MODE = "shardMode";
    private static final String MODEL_OPTIONS = "modelOptions";

    private final String indexType;
    private final IvfShard shardMode;
    private final Map<String, String> modelOptions;

    private VectorGlobalIndexFileMeta(
            String indexType, IvfShard shardMode, Map<String, String> modelOptions) {
        this.indexType = indexType;
        this.shardMode = shardMode;
        this.modelOptions = new LinkedHashMap<>(modelOptions);
        validate();
    }

    public static VectorGlobalIndexFileMeta centroidSharded(
            String indexType, Map<String, String> modelOptions) {
        return new VectorGlobalIndexFileMeta(indexType, IvfShard.CENTROID_BASED, modelOptions);
    }

    public byte[] serialize() throws IOException {
        return OBJECT_MAPPER.writeValueAsBytes(toMap());
    }

    public void writeWithPayload(OutputStream out, VectorTrainingModel trainingModel)
            throws IOException {
        byte[] metadata = serialize();
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        dataOutputStream.writeInt(metadata.length);
        dataOutputStream.write(metadata);
        trainingModel.serializeTo(dataOutputStream);
        dataOutputStream.flush();
    }

    private Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(VERSION_FIELD, VERSION);
        map.put(INDEX_TYPE, indexType);
        map.put(SHARD_MODE, shardMode.optionValue());
        map.put(MODEL_OPTIONS, modelOptions);
        return map;
    }

    public static VectorGlobalIndexFileMeta deserialize(byte[] data) throws IOException {
        JsonNode root = OBJECT_MAPPER.readTree(readMetadataBytes(data));
        validateVersion(root);
        return new VectorGlobalIndexFileMeta(indexType(root), shardMode(root), modelOptions(root));
    }

    private static byte[] readMetadataBytes(byte[] data) throws IOException {
        if (data.length > 0 && data[0] == '{') {
            return data;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
            int metadataLength = in.readInt();
            if (metadataLength <= 0 || metadataLength > data.length - Integer.BYTES) {
                throw new IllegalArgumentException(
                        "Invalid vector global index metadata length: " + metadataLength);
            }
            byte[] metadata = new byte[metadataLength];
            in.readFully(metadata);
            return metadata;
        }
    }

    private void validate() {
        if (indexType == null || indexType.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Vector global index file metadata misses indexType.");
        }
        if (shardMode != IvfShard.CENTROID_BASED) {
            throw new IllegalArgumentException(
                    "Vector global index file requires shardMode="
                            + IvfShard.CENTROID_BASED.optionValue()
                            + ".");
        }
    }

    public IvfShard shardMode() {
        return shardMode;
    }

    public String indexType() {
        return indexType;
    }

    public Map<String, String> modelOptions() {
        return new LinkedHashMap<>(modelOptions);
    }

    private static void validateVersion(JsonNode root) {
        JsonNode node = root.get(VERSION_FIELD);
        if (node == null) {
            throw new IllegalArgumentException("Vector global index file metadata misses version.");
        }
        int version = node.asInt(-1);
        if (version != VERSION) {
            throw new IllegalArgumentException(
                    "Vector global index file has unsupported version: " + node.asText());
        }
    }

    private static IvfShard shardMode(JsonNode root) {
        JsonNode node = root.get(SHARD_MODE);
        if (node == null) {
            throw new IllegalArgumentException(
                    "Vector global index file metadata misses shardMode.");
        }
        IvfShard shardMode = IvfShard.fromValue(node.asText());
        if (shardMode == null) {
            throw new IllegalArgumentException(
                    "Vector global index file has unsupported shardMode: " + node.asText());
        }
        return shardMode;
    }

    private static String indexType(JsonNode root) {
        JsonNode node = root.get(INDEX_TYPE);
        if (node == null || node.asText().trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Vector global index file metadata misses indexType.");
        }
        return node.asText();
    }

    private static Map<String, String> modelOptions(JsonNode root) {
        Map<String, String> options = new LinkedHashMap<>();
        JsonNode node = root.get(MODEL_OPTIONS);
        if (node == null || node.isNull()) {
            return options;
        }
        if (!node.isObject()) {
            throw new IllegalArgumentException(
                    "Vector global index file metadata has invalid modelOptions.");
        }
        java.util.Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            options.put(field.getKey(), field.getValue().asText());
        }
        return options;
    }
}
