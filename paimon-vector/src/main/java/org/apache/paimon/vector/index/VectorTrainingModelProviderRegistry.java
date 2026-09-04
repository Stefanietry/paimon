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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/** Registry for vector training model providers. */
public class VectorTrainingModelProviderRegistry {

    private static final Map<String, VectorTrainingModelProvider> PROVIDERS = new HashMap<>();

    static {
        ServiceLoader<VectorTrainingModelProvider> serviceLoader =
                ServiceLoader.load(VectorTrainingModelProvider.class);
        for (VectorTrainingModelProvider provider : serviceLoader) {
            for (String indexType : provider.supportedIndexTypes()) {
                VectorTrainingModelProvider previous = PROVIDERS.put(indexType, provider);
                if (previous != null) {
                    throw new IllegalStateException(
                            "Duplicate vector training model provider for index type: "
                                    + indexType);
                }
            }
        }
    }

    private VectorTrainingModelProviderRegistry() {}

    public static VectorGlobalModelTrainer createTrainer(
            String indexType, Map<String, String> options) {
        return provider(indexType).createTrainer(indexType, options);
    }

    public static VectorTrainingModel loadModel(
            String indexType, Map<String, String> options, byte[] payload) throws IOException {
        return provider(indexType).loadModel(indexType, options, payload);
    }

    private static VectorTrainingModelProvider provider(String indexType) {
        VectorTrainingModelProvider provider = PROVIDERS.get(indexType);
        if (provider == null) {
            throw new IllegalArgumentException(
                    "Unsupported vector index type for training model provider: " + indexType);
        }
        return provider;
    }
}
