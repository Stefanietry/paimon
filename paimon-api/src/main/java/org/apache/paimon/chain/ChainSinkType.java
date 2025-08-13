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

package org.apache.paimon.chain;

import org.apache.paimon.options.description.InlineElement;

import java.util.Map;

import static org.apache.paimon.CoreOptions.BRANCH;
import static org.apache.paimon.CoreOptions.CHAIN_TABLE_DELTA_BRANCH;
import static org.apache.paimon.CoreOptions.CHAIN_TABLE_SNAPSHOT_BRANCH;
import static org.apache.paimon.options.description.TextElement.text;

/** Type of the sink for chain table. */
public enum ChainSinkType {
    DEFAULT("", "main branch."),
    DELTA("delta", "delta branch."),
    SNAPSHOT("snapshot", "snapshot branch.");

    private final String value;
    private final String description;

    ChainSinkType(String value, String description) {
        this.value = value;
        this.description = description;
    }

    public String getValue() {
        return value;
    }

    public InlineElement getDescription() {
        return text(description);
    }

    public static String getSinkBranchName(String sinkType, Map<String, String> tblOptions) {
        if (DELTA.value.equalsIgnoreCase(sinkType)) {
            return tblOptions.get(CHAIN_TABLE_DELTA_BRANCH.key());
        } else if (SNAPSHOT.value.equalsIgnoreCase(sinkType)) {
            return tblOptions.get(CHAIN_TABLE_SNAPSHOT_BRANCH.key());
        } else {
            return tblOptions.getOrDefault(BRANCH.key(), BRANCH.defaultValue());
        }
    }
}
