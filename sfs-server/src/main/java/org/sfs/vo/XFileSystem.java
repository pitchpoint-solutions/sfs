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

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Optional.fromNullable;

public abstract class XFileSystem<T extends XFileSystem> {

    private String partition;
    private String device;
    private String path;
    private Long totalSpace;
    private Long unallocatedSpace;
    private Long usableSpace;
    private String type;

    public Optional<String> getPath() {
        return fromNullable(path);
    }

    public T setPath(String path) {
        this.path = path;
        return (T) this;
    }

    public Optional<String> getDevice() {
        return fromNullable(device);
    }

    public T setDevice(String device) {
        this.device = device;
        return (T) this;
    }

    public Optional<Long> getTotalSpace() {
        return fromNullable(totalSpace);
    }

    public T setTotalSpace(Long totalSpace) {
        this.totalSpace = totalSpace;
        return (T) this;
    }

    public Optional<Long> getUnallocatedSpace() {
        return fromNullable(unallocatedSpace);
    }

    public T setUnallocatedSpace(Long unallocatedSpace) {
        this.unallocatedSpace = unallocatedSpace;
        return (T) this;
    }

    public Optional<String> getType() {
        return fromNullable(type);
    }

    public T setType(String type) {
        this.type = type;
        return (T) this;
    }

    public Optional<Long> getUsableSpace() {
        return fromNullable(usableSpace);
    }

    public T setUsableSpace(Long usableSpace) {
        this.usableSpace = usableSpace;
        return (T) this;
    }

    public Optional<String> getPartition() {
        return fromNullable(partition);
    }

    public T setPartition(String partition) {
        this.partition = partition;
        return (T) this;
    }

    public abstract T copy();

    protected T copyInternal(XFileSystem t) {
        setPath(t.path);
        setTotalSpace(t.totalSpace);
        setUnallocatedSpace(t.unallocatedSpace);
        setUsableSpace(t.usableSpace);
        setType(t.type);
        setDevice(t.device);
        setPartition(t.partition);
        return (T) this;
    }

    public T merge(XFileSystem<? extends XFileSystem> other) {
        this.path = other.path;
        this.totalSpace = other.totalSpace;
        this.unallocatedSpace = other.unallocatedSpace;
        this.usableSpace = other.usableSpace;
        this.type = other.type;
        this.device = other.device;
        this.partition = other.partition;
        return (T) this;
    }

    public T merge(JsonObject jsonObject) {
        this.path = jsonObject.getString("path");
        this.totalSpace = jsonObject.getLong("totalSpace");
        this.unallocatedSpace = jsonObject.getLong("unallocated_space");
        this.usableSpace = jsonObject.getLong("usable_space");
        this.type = jsonObject.getString("type");
        this.device = jsonObject.getString("device");
        this.partition = jsonObject.getString("partition");
        return (T) this;
    }

    public JsonObject toJsonObject() {
        return new JsonObject()
                .put("path", path)
                .put("total_space", totalSpace)
                .put("unallocated_space", unallocatedSpace)
                .put("usable_space", usableSpace)
                .put("type", type)
                .put("device", device)
                .put("partition", partition);
    }
}
