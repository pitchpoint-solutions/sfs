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
import static org.sfs.filesystem.volume.Volume.Status;
import static org.sfs.filesystem.volume.Volume.Status.fromNameIfExists;

public abstract class XVolume<T extends XVolume> {

    private String id;
    private XFileSystem<? extends XFileSystem> fileSystem;
    private XAllocatedFile<? extends XAllocatedFile> indexFile;
    private XAllocatedFile<? extends XAllocatedFile> dataFile;
    private Long usableSpace;
    private Status status;

    public abstract T copy();

    protected T copyInternal(XVolume t) {
        setId(t.id);
        setStatus(t.status);
        setUsableSpace(t.usableSpace);
        setFileSystem(t.fileSystem != null ? t.fileSystem.copy() : null);
        setIndexFile(t.indexFile != null ? t.indexFile.copy() : null);
        setDataFile(t.dataFile != null ? t.dataFile.copy() : null);
        return (T) this;
    }

    public Optional<String> getId() {
        return fromNullable(id);
    }

    public T setId(String id) {
        this.id = id;
        return (T) this;
    }

    public Optional<XAllocatedFile<? extends XAllocatedFile>> getDataFile() {
        return fromNullable(dataFile);
    }

    public T setDataFile(XAllocatedFile<? extends XAllocatedFile> dataFile) {
        this.dataFile = dataFile;
        return (T) this;
    }

    public Optional<XAllocatedFile<? extends XAllocatedFile>> getIndexFile() {
        return fromNullable(indexFile);
    }

    public T setIndexFile(XAllocatedFile<? extends XAllocatedFile> indexFile) {
        this.indexFile = indexFile;
        return (T) this;
    }

    public Optional<XFileSystem<? extends XFileSystem>> getFileSystem() {
        return fromNullable(fileSystem);
    }

    public T setFileSystem(XFileSystem<? extends XFileSystem> fileSystem) {
        this.fileSystem = fileSystem;
        return (T) this;
    }

    public Optional<Status> getStatus() {
        return fromNullable(status);
    }

    public T setStatus(Status status) {
        this.status = status;
        return (T) this;
    }

    public Optional<Long> getUsableSpace() {
        return fromNullable(usableSpace);
    }

    public T setUsableSpace(Long usableSpace) {
        this.usableSpace = usableSpace;
        return (T) this;
    }

    public T merge(XVolume<? extends XVolume> other) {
        this.id = other.id;
        this.fileSystem = other.fileSystem;
        this.status = other.status;
        this.indexFile = other.indexFile;
        this.dataFile = other.dataFile;
        this.usableSpace = other.usableSpace;
        return (T) this;
    }

    public T merge(JsonObject jsonObject) {
        this.id = jsonObject.getString("id");
        JsonObject jsonFileSystem = jsonObject.getJsonObject("file_system");
        if (jsonFileSystem != null) {
            this.fileSystem =
                    new TransientXFileSystem()
                            .merge(jsonFileSystem);
        } else {
            this.fileSystem = null;
        }
        JsonObject jsonIndexFile = jsonObject.getJsonObject("index_file");
        if (jsonIndexFile != null) {
            this.indexFile =
                    new TransientXAllocatedFile()
                            .merge(jsonIndexFile);
        } else {
            this.indexFile = null;
        }
        JsonObject jsonDataFile = jsonObject.getJsonObject("data_file");
        if (jsonDataFile != null) {
            this.dataFile =
                    new TransientXAllocatedFile()
                            .merge(jsonDataFile);
        }
        this.status = fromNameIfExists(jsonObject.getString("status"));
        this.usableSpace = jsonObject.getLong("usable_space");
        return (T) this;
    }

    public JsonObject toJsonObject() {
        JsonObject jsonObject =
                new JsonObject()
                        .put("id", id)
                        .put("status", status != null ? status.name() : null)
                        .put("usable_space", usableSpace);

        if (fileSystem != null) {
            JsonObject jsonFileSystem = fileSystem.toJsonObject();
            jsonObject = jsonObject.put("file_system", jsonFileSystem);
        }
        if (indexFile != null) {
            JsonObject jsonIndexFile = indexFile.toJsonObject();
            jsonObject = jsonObject.put("index_file", jsonIndexFile);
        }
        if (dataFile != null) {
            JsonObject jsonDataFile = dataFile.toJsonObject();
            jsonObject = jsonObject.put("data_file", jsonDataFile);
        }
        return jsonObject;
    }
}
