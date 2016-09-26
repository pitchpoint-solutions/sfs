/*
 *
 * Copyright (C) 2009 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfs.vo;

import java.util.Collection;

public class ContainerList {

    private long containerCount = 0;
    private long objectCount = 0;
    private double bytesUsed = 0;
    private final PersistentAccount account;
    private Collection<SparseContainer> containers;

    public ContainerList(PersistentAccount account, Collection<SparseContainer> containers) {
        this.account = account;
        this.containers = containers;
        this.containerCount = containers.size();
        for (SparseContainer sparseContainer : containers) {
            objectCount += sparseContainer.getObjectCount();
            bytesUsed += sparseContainer.getByteCount();
        }
    }

    public long getContainerCount() {
        return containers.size();
    }

    public long getObjectCount() {
        return objectCount;
    }

    public double getBytesUsed() {
        return bytesUsed;
    }

    public PersistentAccount getAccount() {
        return account;
    }

    public Iterable<SparseContainer> getContainers() {
        return containers;
    }

    public static class SparseContainer {

        private final String containerName;
        private long objectCount;
        private double byteCount;

        public SparseContainer(String containerName, long objectCount, double byteCount) {
            this.containerName = containerName;
            this.objectCount = objectCount;
            this.byteCount = byteCount;
        }

        public String getContainerName() {
            return containerName;
        }

        public long getObjectCount() {
            return objectCount;
        }

        public double getByteCount() {
            return byteCount;
        }

        public void setObjectCount(long objectCount) {
            this.objectCount = objectCount;
        }

        public void setByteCount(double byteCount) {
            this.byteCount = byteCount;
        }
    }
}
