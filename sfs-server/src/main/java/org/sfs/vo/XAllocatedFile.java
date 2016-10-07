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

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Optional.fromNullable;

public abstract class XAllocatedFile<T extends XAllocatedFile> {

    private String file;
    private Long fileSizeBytes;
    private Integer freeRangeCount;
    private Long bytesFree;
    private Integer lockCount;
    private Long writeQueueBytesPending;
    private Long writeQueueBytesFull;
    private Long writeQueueBytesDrained;

    public String getFile() {
        return file;
    }

    public T setFile(String file) {
        this.file = file;
        return (T) this;
    }

    public Optional<Long> getFileSizeBytes() {
        return fromNullable(fileSizeBytes);
    }

    public T setFileSizeBytes(Long fileSizeBytes) {
        this.fileSizeBytes = fileSizeBytes;
        return (T) this;
    }

    public Optional<Long> getBytesFree() {
        return fromNullable(bytesFree);
    }

    public T setBytesFree(Long bytesFree) {
        this.bytesFree = bytesFree;
        return (T) this;
    }

    public Optional<Integer> getFreeRangeCount() {
        return fromNullable(freeRangeCount);
    }

    public T setFreeRangeCount(Integer freeRangeCount) {
        this.freeRangeCount = freeRangeCount;
        return (T) this;
    }

    public Optional<Integer> getLockCount() {
        return fromNullable(lockCount);
    }

    public T setLockCount(Integer lockCount) {
        this.lockCount = lockCount;
        return (T) this;
    }

    public Optional<Long> getWriteQueueBytesPending() {
        return fromNullable(writeQueueBytesPending);
    }

    public T setWriteQueueBytesPending(Long writeQueueBytesPending) {
        this.writeQueueBytesPending = writeQueueBytesPending;
        return (T) this;
    }

    public Optional<Long> getWriteQueueBytesFull() {
        return fromNullable(writeQueueBytesFull);
    }

    public T setWriteQueueBytesFull(Long writeQueueBytesFull) {
        this.writeQueueBytesFull = writeQueueBytesFull;
        return (T) this;
    }

    public Optional<Long> getWriteQueueBytesDrained() {
        return fromNullable(writeQueueBytesDrained);
    }

    public T setWriteQueueBytesDrained(Long writeQueueBytesDrained) {
        this.writeQueueBytesDrained = writeQueueBytesDrained;
        return (T) this;
    }

    public abstract T copy();

    protected T copyInternal(XAllocatedFile t) {
        setBytesFree(t.bytesFree);
        setFreeRangeCount(t.freeRangeCount);
        setFile(t.file);
        setFileSizeBytes(t.fileSizeBytes);
        setLockCount(t.lockCount);
        setWriteQueueBytesPending(t.writeQueueBytesPending);
        setWriteQueueBytesFull(t.writeQueueBytesFull);
        setWriteQueueBytesDrained(t.writeQueueBytesDrained);
        return (T) this;
    }

    public T merge(XAllocatedFile<? extends XAllocatedFile> other) {
        this.freeRangeCount = other.freeRangeCount;
        this.bytesFree = other.bytesFree;
        this.file = other.file;
        this.fileSizeBytes = other.fileSizeBytes;
        this.lockCount = other.lockCount;
        this.writeQueueBytesPending = other.writeQueueBytesPending;
        this.writeQueueBytesFull = other.writeQueueBytesFull;
        this.writeQueueBytesDrained = other.writeQueueBytesDrained;
        return (T) this;
    }

    public T merge(JsonObject jsonObject) {
        this.freeRangeCount = jsonObject.getInteger("free_range_count");
        this.bytesFree = jsonObject.getLong("bytes_free");
        this.file = jsonObject.getString("file");
        this.fileSizeBytes = jsonObject.getLong("file_size_bytes");
        this.lockCount = jsonObject.getInteger("lock_count");
        this.writeQueueBytesPending = jsonObject.getLong("write_queue_pending_bytes");
        this.writeQueueBytesFull = jsonObject.getLong("write_queue_full_bytes");
        this.writeQueueBytesDrained = jsonObject.getLong("write_queue_drained_bytes");
        return (T) this;
    }

    public JsonObject toJsonObject() {
        return new JsonObject()
                .put("free_range_count", freeRangeCount)
                .put("bytes_free", bytesFree)
                .put("file", file)
                .put("file_size_bytes", fileSizeBytes)
                .put("lock_count", lockCount)
                .put("write_queue_pending_bytes", writeQueueBytesPending)
                .put("write_queue_full_bytes", writeQueueBytesFull)
                .put("write_queue_drained_bytes", writeQueueBytesDrained);
    }
}
