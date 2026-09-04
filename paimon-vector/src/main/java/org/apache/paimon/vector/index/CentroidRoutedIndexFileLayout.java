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

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.IndexFileKind;
import org.apache.paimon.globalindex.IvfShard;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** File layout of a centroid-routed vector index resolved from manifest metadata. */
final class CentroidRoutedIndexFileLayout {

    @Nullable private final GlobalIndexIOMeta routingModelFile;
    private final Map<Integer, GlobalIndexIOMeta> centroidDataShardFiles;

    private CentroidRoutedIndexFileLayout(
            @Nullable GlobalIndexIOMeta routingModelFile,
            Map<Integer, GlobalIndexIOMeta> centroidDataShardFiles) {
        this.routingModelFile = routingModelFile;
        this.centroidDataShardFiles = centroidDataShardFiles;
    }

    @Nullable
    GlobalIndexIOMeta routingModelFile() {
        return routingModelFile;
    }

    Map<Integer, GlobalIndexIOMeta> centroidDataShardFiles() {
        return centroidDataShardFiles;
    }

    static CentroidRoutedIndexFileLayout tryCreate(
            GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files) {
        try {
            if (files.isEmpty()) {
                return null;
            }

            GlobalIndexIOMeta routingModelFile = null;
            for (GlobalIndexIOMeta file : files) {
                if (file.fileKind() != IndexFileKind.ROUTING_MODEL) {
                    continue;
                }
                VectorIndexMeta fileMeta = parseVectorIndexMeta(file.metadata());
                if (fileMeta == null) {
                    throw new IllegalArgumentException(
                            "Routing model index file requires vector index metadata: "
                                    + file.filePath().getName());
                }
                if (fileMeta.shardMode() != IvfShard.CENTROID_BASED) {
                    throw new IllegalArgumentException(
                            "Centroid-routed vector index requires shardMode="
                                    + IvfShard.CENTROID_BASED.optionValue()
                                    + ".");
                }
                if (routingModelFile != null) {
                    throw new IllegalArgumentException(
                            "Centroid-routed vector index currently supports only one "
                                    + "global index file per partition.");
                }
                routingModelFile = file;
            }

            if (routingModelFile != null) {
                VectorGlobalIndexFileMeta routingModelMeta;
                try (org.apache.paimon.fs.SeekableInputStream in =
                        fileReader.getInputStream(routingModelFile)) {
                    routingModelMeta =
                            VectorGlobalIndexFileMeta.deserialize(IOUtils.readFully(in, false));
                }
                if (routingModelMeta.shardMode() != IvfShard.CENTROID_BASED) {
                    throw new IllegalArgumentException(
                            "Vector global index file contains shardMode="
                                    + routingModelMeta.shardMode().optionValue()
                                    + ".");
                }
            }

            Map<Integer, GlobalIndexIOMeta> centroidDataShardFiles =
                    tryCreateCentroidDataShardFiles(files);
            if (routingModelFile == null && centroidDataShardFiles.isEmpty()) {
                return null;
            }
            for (GlobalIndexIOMeta file : files) {
                String fileName = file.filePath().getName();
                if (routingModelFile != null
                        && (file == routingModelFile
                                || file.filePath().equals(routingModelFile.filePath()))) {
                    continue;
                }
                if (!containsFile(centroidDataShardFiles, file)) {
                    throw new IllegalArgumentException(
                            "Centroid-routed vector index currently supports only centroid "
                                    + "data shards in one partition. Unknown vector index file: "
                                    + fileName);
                }
            }
            return new CentroidRoutedIndexFileLayout(routingModelFile, centroidDataShardFiles);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse vector index metadata", e);
        }
    }

    private static Map<Integer, GlobalIndexIOMeta> tryCreateCentroidDataShardFiles(
            List<GlobalIndexIOMeta> files) throws IOException {
        Map<Integer, GlobalIndexIOMeta> centroidDataShardFiles = new LinkedHashMap<>();
        for (GlobalIndexIOMeta file : files) {
            if (file.fileKind() == IndexFileKind.ROUTING_MODEL) {
                continue;
            }
            VectorIndexMeta fileMeta = parseVectorIndexMeta(file.metadata());
            VectorIndexMeta shardMeta = parseCentroidShardMeta(fileMeta, file.filePath().getName());
            if (shardMeta == null) {
                continue;
            }
            GlobalIndexIOMeta previous = centroidDataShardFiles.put(shardMeta.centroid(), file);
            if (previous != null) {
                throw new IllegalArgumentException(
                        "Duplicate centroid data shard for centroid=" + shardMeta.centroid());
            }
        }
        return centroidDataShardFiles;
    }

    private static boolean containsFile(
            Map<Integer, GlobalIndexIOMeta> centroidDataShardFiles, GlobalIndexIOMeta file) {
        for (GlobalIndexIOMeta dataShard : centroidDataShardFiles.values()) {
            if (dataShard == file || dataShard.filePath().equals(file.filePath())) {
                return true;
            }
        }
        return false;
    }

    private static VectorIndexMeta parseCentroidShardMeta(VectorIndexMeta meta, String fileName) {
        if (meta == null || !meta.isCentroidShard()) {
            return null;
        }
        if (meta.centroid() == null) {
            throw new IllegalArgumentException(
                    "Centroid shard index file requires centroid: " + fileName);
        }
        if (meta.rowIdEncoding() != RowIdEncoding.ABSOLUTE_ROW_ID) {
            throw new IllegalArgumentException(
                    "Centroid shard index file requires ABSOLUTE_ROW_ID encoding: " + fileName);
        }
        return meta;
    }

    private static VectorIndexMeta parseVectorIndexMeta(byte[] indexMeta) throws IOException {
        if (indexMeta == null) {
            return null;
        }
        return VectorIndexMeta.deserialize(indexMeta);
    }
}
