/*
 * Copyright 2019 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfs.nodes;

import org.apache.commons.lang.StringUtils;

public enum WriteConsistency {

    ANY("any"),
    QUORUM("quorum");

    private final String value;

    WriteConsistency(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static WriteConsistency fromValueIfExists(String value) {
        for (WriteConsistency writeConsistency : values()) {
            if (StringUtils.equalsIgnoreCase(writeConsistency.value, value)) {
                return writeConsistency;
            }
        }
        return null;
    }
}
