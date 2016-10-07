/*
 * Copyright 2016 The Simple File Server Authors
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

package org.sfs.vo;

public class ContainerStats {

    private final long objectCount;
    private final double bytesUsed;
    private final PersistentContainer persistentContainer;

    public ContainerStats(double bytesUsed, long objectCount, PersistentContainer persistentContainer) {
        this.bytesUsed = bytesUsed;
        this.objectCount = objectCount;
        this.persistentContainer = persistentContainer;
    }

    public double getBytesUsed() {
        return bytesUsed;
    }

    public long getObjectCount() {
        return objectCount;
    }

    public PersistentContainer getPersistentContainer() {
        return persistentContainer;
    }
}
