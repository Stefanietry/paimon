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

import org.apache.paimon.fs.Path;
import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.IndexFileKind;
import org.apache.paimon.globalindex.IvfShard;
import org.apache.paimon.globalindex.RoutedVectorGlobalIndexer;
import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.Range;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/** Native vector global indexer backed by paimon-vector-index-java. */
public class NativeVectorGlobalIndexer implements RoutedVectorGlobalIndexer {

    private static final Logger LOG = LoggerFactory.getLogger(NativeVectorGlobalIndexer.class);

    static final String DEFAULT_METRIC = "inner_product";

    private final DataType fieldType;
    private final Map<String, String> options;
    private final String identifier;
    private final double trainSampleRatio;

    public NativeVectorGlobalIndexer(
            DataType fieldType, Map<String, String> options, String identifier) {
        this(
                fieldType,
                options,
                identifier,
                NativeVectorGlobalIndexerFactory.DEFAULT_TRAIN_SAMPLE_RATIO);
    }

    public NativeVectorGlobalIndexer(
            DataType fieldType,
            Map<String, String> options,
            String identifier,
            double trainSampleRatio) {
        this.fieldType = fieldType;
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.identifier = Objects.requireNonNull(identifier, "identifier must not be null");
        if (Double.isNaN(trainSampleRatio)
                || Double.isInfinite(trainSampleRatio)
                || trainSampleRatio <= 0
                || trainSampleRatio > 1) {
            throw new IllegalArgumentException(
                    "trainSampleRatio must be greater than 0 and less than or equal to 1: "
                            + trainSampleRatio);
        }
        this.trainSampleRatio = trainSampleRatio;
    }

    @Override
    public GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) {
        return new NativeVectorGlobalIndexWriter(
                fileWriter, fieldType, options, identifier, trainSampleRatio);
    }

    @Override
    public GlobalIndexReader createReader(
            GlobalIndexFileReader fileReader,
            List<GlobalIndexIOMeta> files,
            long totalRowCount,
            List<Range> rowRanges,
            ExecutorService executor) {
        CentroidRoutedIndexFileLayout centroidRoutedFileLayout =
                CentroidRoutedIndexFileLayout.tryCreate(fileReader, files);
        if (centroidRoutedFileLayout != null) {
            LOG.info(
                    "Centroid-based IVF reader enabled for '{}': routingModelFile={}, "
                            + "centroidShardFiles={}, totalRowCount={}",
                    identifier,
                    centroidRoutedFileLayout.routingModelFile(),
                    centroidRoutedFileLayout.centroidDataShardFiles(),
                    totalRowCount);
            return createCentroidRoutedReader(fileReader, centroidRoutedFileLayout, executor);
        }
        return new NativeVectorGlobalIndexReader(fileReader, files, fieldType, executor);
    }

    private GlobalIndexReader createCentroidRoutedReader(
            GlobalIndexFileReader fileReader,
            CentroidRoutedIndexFileLayout files,
            ExecutorService executor) {
        try {
            NativeVectorCentroidRouter centroidRouter =
                    files.routingModelFile() == null
                            ? null
                            : new NativeVectorCentroidRouter(fileReader, files.routingModelFile());
            return new NativeCentroidRoutedIndexReader(
                    fileReader,
                    centroidRouter,
                    files.centroidDataShardFiles(),
                    fieldType,
                    executor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create centroid-routed vector index reader", e);
        }
    }

    @Override
    public String metric() {
        return options.getOrDefault("metric", DEFAULT_METRIC);
    }

    @Override
    public boolean isRoutingGlobalIndexFile(@Nullable IndexFileKind fileKind, byte[] indexMeta) {
        if (fileKind != IndexFileKind.ROUTING_MODEL || indexMeta == null) {
            return false;
        }
        try {
            VectorIndexMeta meta = VectorIndexMeta.deserialize(indexMeta);
            return meta.shardMode() == IvfShard.CENTROID_BASED;
        } catch (IOException | IllegalArgumentException e) {
            return false;
        }
    }

    @Override
    public Set<Integer> routeCentroids(
            GlobalIndexFileReader fileReader,
            GlobalIndexIOMeta globalIndexFile,
            float[][] queryVectors,
            int limit,
            Map<String, String> options) {
        List<Set<Integer>> routedCentroidsByVector =
                routeCentroidsByVector(
                        fileReader, globalIndexFile, queryVectors, limit, options, null);
        Set<Integer> routed = new LinkedHashSet<>();
        for (Set<Integer> routedCentroids : routedCentroidsByVector) {
            routed.addAll(routedCentroids);
        }
        return routed;
    }

    @Override
    public List<Set<Integer>> routeCentroidsByVector(
            GlobalIndexFileReader fileReader,
            GlobalIndexIOMeta globalIndexFile,
            float[][] queryVectors,
            int limit,
            Map<String, String> options,
            Map<Path, AutoCloseable> routingModelCache) {
        IvfTrainingModel model = null;
        boolean closeModel = routingModelCache == null;
        try {
            model = routingModel(fileReader, globalIndexFile, routingModelCache);
            List<Set<Integer>> routedCentroidsByVector = new ArrayList<>(queryVectors.length);
            IvfRoutingModel routingModel = model.routingModel();
            int nprobe = nprobe(options, limit);
            for (float[] queryVector : queryVectors) {
                Set<Integer> routed = new LinkedHashSet<>();
                for (int centroid : routingModel.nearestCentroids(queryVector, nprobe)) {
                    routed.add(centroid);
                }
                routedCentroidsByVector.add(routed);
            }
            return routedCentroidsByVector;
        } catch (IOException e) {
            throw new RuntimeException("Failed to route vector centroid ids.", e);
        } finally {
            if (closeModel && model != null) {
                IOUtils.closeQuietly(model);
            }
        }
    }

    private IvfTrainingModel routingModel(
            GlobalIndexFileReader fileReader,
            GlobalIndexIOMeta globalIndexFile,
            Map<Path, AutoCloseable> routingModelCache)
            throws IOException {
        Path modelFilePath = globalIndexFile.filePath();
        if (routingModelCache == null) {
            LOG.info(
                    "Loading IVF routing model without cache: modelFile={}, file={}",
                    modelFilePath.getName(),
                    modelFilePath);
            return loadRoutingModel(fileReader, globalIndexFile, modelFilePath, false, 0);
        }

        synchronized (routingModelCache) {
            AutoCloseable cached = routingModelCache.get(modelFilePath);
            if (cached != null) {
                if (!(cached instanceof IvfTrainingModel)) {
                    throw new IllegalStateException(
                            "Unexpected cached routing model type: " + cached.getClass().getName());
                }
                return (IvfTrainingModel) cached;
            }
            LOG.info(
                    "IVF routing model cache miss: modelFile={}, file={}, cacheSize={}",
                    modelFilePath.getName(),
                    modelFilePath,
                    routingModelCache.size());
            IvfTrainingModel model =
                    loadRoutingModel(
                            fileReader,
                            globalIndexFile,
                            modelFilePath,
                            true,
                            routingModelCache.size() + 1);
            routingModelCache.put(modelFilePath, model);
            return model;
        }
    }

    private IvfTrainingModel loadRoutingModel(
            GlobalIndexFileReader fileReader,
            GlobalIndexIOMeta globalIndexFile,
            Path modelFilePath,
            boolean cached,
            int cacheSize)
            throws IOException {
        byte[] fileBytes;
        try (org.apache.paimon.fs.SeekableInputStream in =
                fileReader.getInputStream(globalIndexFile)) {
            fileBytes = IOUtils.readFully(in, false);
        }
        VectorGlobalIndexFileMeta.deserialize(fileBytes);
        IvfTrainingModel model =
                asIvfTrainingModel(NativeVectorTrainingModelLoader.load(fileBytes));
        LOG.info(
                "Loaded IVF routing model: modelFile={}, file={}, bytes={}, cached={}, "
                        + "cacheSize={}",
                modelFilePath.getName(),
                modelFilePath,
                fileBytes.length,
                cached,
                cached ? cacheSize : 0);
        return model;
    }

    @Override
    public boolean acceptsRoutedIndexFile(byte[] indexMeta, Set<Integer> routedCentroids) {
        if (indexMeta == null || routedCentroids.isEmpty()) {
            return false;
        }
        try {
            VectorIndexMeta meta = VectorIndexMeta.deserialize(indexMeta);
            return meta.isCentroidShard()
                    && meta.rowIdEncoding() == RowIdEncoding.ABSOLUTE_ROW_ID
                    && meta.centroid() != null
                    && routedCentroids.contains(meta.centroid());
        } catch (IOException | IllegalArgumentException e) {
            return false;
        }
    }

    private static int nprobe(Map<String, String> options, int limit) {
        String value = options.get("ivf.nprobe");
        if (value == null) {
            return limit;
        }
        int nprobe = Integer.parseInt(value);
        if (nprobe <= 0) {
            throw new IllegalArgumentException("Invalid value for 'ivf.nprobe': " + value);
        }
        return nprobe;
    }

    private static IvfTrainingModel asIvfTrainingModel(VectorTrainingModel model) {
        if (!(model instanceof IvfTrainingModel)) {
            throw new IllegalStateException(
                    "Centroid-routed IVF index requires IvfTrainingModel, but got: "
                            + model.getClass().getName());
        }
        return (IvfTrainingModel) model;
    }
}
