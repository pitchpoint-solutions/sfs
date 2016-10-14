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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.addAll;
import static java.util.Calendar.getInstance;
import static java.util.Collections.unmodifiableList;
import static org.sfs.util.DateFormatter.fromDateTimeString;
import static org.sfs.util.DateFormatter.toDateTimeString;

public abstract class ServiceDef<T extends ServiceDef> {

    private String id;
    private XFileSystem<? extends XFileSystem> fileSystem;
    private Calendar lastUpdate;
    private Boolean master;
    private Boolean dataNode;
    private Long documentCount;
    private Integer availableProcessors;
    private Long freeMemory;
    private Long totalMemory;
    private Long maxMemory;
    private Integer backgroundPoolQueueSize;
    private Integer ioPoolQueueSize;
    private List<HostAndPort> publishAddresses = new ArrayList<>();
    private List<XVolume<? extends XVolume>> volumes = new ArrayList<>();

    public ServiceDef(String id) {
        this.id = id;
    }

    public ServiceDef() {
    }

    public String getId() {
        return id;
    }

    public Calendar getLastUpdate() {
        if (lastUpdate == null) {
            lastUpdate = getInstance();
        }
        return lastUpdate;
    }

    public T setLastUpdate(Calendar lastUpdate) {
        this.lastUpdate = lastUpdate;
        return (T) this;
    }

    public Optional<Boolean> getMaster() {
        return fromNullable(master);
    }

    public T setMaster(Boolean master) {
        this.master = master;
        return (T) this;
    }

    public Optional<Integer> getBackgroundPoolQueueSize() {
        return fromNullable(backgroundPoolQueueSize);
    }

    public T setBackgroundPoolQueueSize(Integer backgroundPoolQueueSize) {
        this.backgroundPoolQueueSize = backgroundPoolQueueSize;
        return (T) this;
    }

    public Optional<Integer> getIoPoolQueueSize() {
        return fromNullable(ioPoolQueueSize);
    }

    public T setIoPoolQueueSize(Integer ioPoolQueueSize) {
        this.ioPoolQueueSize = ioPoolQueueSize;
        return (T) this;
    }

    public Optional<Boolean> getDataNode() {
        return fromNullable(dataNode);
    }

    public T setDataNode(Boolean dataNode) {
        this.dataNode = dataNode;
        return (T) this;
    }

    public T setDocumentCount(Long documentCount) {
        this.documentCount = documentCount;
        return (T) this;
    }

    public Optional<Long> getDocumentCount() {
        return fromNullable(documentCount);
    }

    public List<HostAndPort> getPublishAddresses() {
        return unmodifiableList(publishAddresses);
    }

    public T setPublishAddresses(Iterable<HostAndPort> publishAddresses) {
        this.publishAddresses.clear();
        addAll(this.publishAddresses, publishAddresses);
        return (T) this;
    }

    public List<XVolume<? extends XVolume>> getVolumes() {
        return volumes;
    }

    public T setVolumes(Iterable<? extends XVolume<? extends XVolume>> volumes) {
        this.volumes.clear();
        addAll(this.volumes, volumes);
        return (T) this;
    }

    public Optional<Integer> getAvailableProcessors() {
        return fromNullable(availableProcessors);
    }

    public T setAvailableProcessors(Integer availableProcessors) {
        this.availableProcessors = availableProcessors;
        return (T) this;
    }

    public Optional<Long> getFreeMemory() {
        return fromNullable(freeMemory);
    }

    public T setFreeMemory(Long freeMemory) {
        this.freeMemory = freeMemory;
        return (T) this;
    }

    public Optional<Long> getMaxMemory() {
        return fromNullable(maxMemory);
    }

    public T setMaxMemory(Long maxMemory) {
        this.maxMemory = maxMemory;
        return (T) this;
    }

    public Optional<Long> getTotalMemory() {
        return fromNullable(totalMemory);
    }

    public T setTotalMemory(Long totalMemory) {
        this.totalMemory = totalMemory;
        return (T) this;
    }

    public Optional<XFileSystem<? extends XFileSystem>> getFileSystem() {
        return fromNullable(fileSystem);
    }

    public T setFileSystem(XFileSystem<? extends XFileSystem> fileSystem) {
        this.fileSystem = fileSystem;
        return (T) this;
    }

    public abstract T copy();

    protected T copyInternal(ServiceDef t) {
        setDataNode(t.dataNode);
        setDocumentCount(t.documentCount);
        if (t.lastUpdate != null) {
            Calendar cal = getInstance();
            cal.setTimeInMillis(t.lastUpdate.getTimeInMillis());
            setLastUpdate(cal);
        }
        setMaster(t.master);
        setDataNode(t.dataNode);
        setAvailableProcessors(t.availableProcessors);
        setFreeMemory(t.freeMemory);
        setMaxMemory(t.maxMemory);
        setTotalMemory(t.totalMemory);
        setIoPoolQueueSize(t.ioPoolQueueSize);
        setBackgroundPoolQueueSize(t.backgroundPoolQueueSize);
        setFileSystem(t.fileSystem != null ? t.fileSystem.copy() : null);
        if (t.volumes != null) {
            setVolumes(
                    from(t.volumes)
                            .transform(new Function<XVolume<? extends XVolume>, XVolume<? extends XVolume>>() {
                                @Override
                                public XVolume<? extends XVolume> apply(XVolume<? extends XVolume> input) {
                                    return input.copy();
                                }
                            }));
        }
        if (t.publishAddresses != null) {
            setPublishAddresses(t.publishAddresses);
        }
        return (T) this;
    }

    public T merge(ServiceDef<? extends ServiceDef> other) {
        this.id = other.id;
        this.lastUpdate = other.lastUpdate != null ? (Calendar) other.lastUpdate.clone() : null;
        this.master = other.master;
        this.dataNode = other.dataNode;
        this.documentCount = other.documentCount;
        this.availableProcessors = other.availableProcessors;
        this.freeMemory = other.freeMemory;
        this.maxMemory = other.maxMemory;
        this.totalMemory = other.totalMemory;
        this.fileSystem = other.fileSystem;
        this.ioPoolQueueSize = other.ioPoolQueueSize;
        this.backgroundPoolQueueSize = other.backgroundPoolQueueSize;
        this.publishAddresses.clear();
        addAll(this.publishAddresses, other.publishAddresses);
        this.volumes.clear();
        addAll(this.volumes, other.volumes);
        return (T) this;
    }

    public T merge(JsonObject jsonObject) {
        this.id = jsonObject.getString("id");
        this.lastUpdate = fromDateTimeString(jsonObject.getString("update_ts"));
        this.master = jsonObject.getBoolean("master_node");
        this.dataNode = jsonObject.getBoolean("data_node");
        this.documentCount = jsonObject.getLong("document_count");
        this.availableProcessors = jsonObject.getInteger("available_processors");
        this.freeMemory = jsonObject.getLong("free_memory");
        this.maxMemory = jsonObject.getLong("max_memory");
        this.totalMemory = jsonObject.getLong("total_memory");
        this.backgroundPoolQueueSize = jsonObject.getInteger("threadpool_background_queue_size");
        this.ioPoolQueueSize = jsonObject.getInteger("threadpool_io_queue_size");

        JsonObject jsonFileSystem = jsonObject.getJsonObject("file_system");
        if (jsonFileSystem != null) {
            this.fileSystem =
                    new TransientXFileSystem()
                            .merge(jsonFileSystem);
        } else {
            this.fileSystem = null;
        }

        JsonArray jsonListeners = jsonObject.getJsonArray("publish_addresses");
        this.publishAddresses.clear();
        if (jsonListeners != null) {
            for (Object o : jsonListeners) {
                String jsonListener = (String) o;
                this.publishAddresses.add(HostAndPort.fromString(jsonListener));
            }
        }

        JsonArray jsonVolumes = jsonObject.getJsonArray("volumes");
        this.volumes.clear();
        if (jsonVolumes != null) {
            for (Object o : jsonVolumes) {
                JsonObject jsonVolume = (JsonObject) o;
                TransientXVolume transientXVolume =
                        new TransientXVolume()
                                .merge(jsonVolume);
                this.volumes.add(transientXVolume);
            }
        }
        return (T) this;
    }

    public JsonObject toJsonObject() {
        JsonObject jsonObject = new JsonObject()
                .put("id", id)
                .put("update_ts", toDateTimeString(getLastUpdate()))
                .put("master_node", master)
                .put("data_node", dataNode)
                .put("document_count", documentCount)
                .put("available_processors", availableProcessors)
                .put("free_memory", freeMemory)
                .put("max_memory", maxMemory)
                .put("total_memory", totalMemory)
                .put("threadpool_background_queue_size", backgroundPoolQueueSize)
                .put("threadpool_io_queue_size", ioPoolQueueSize);

        if (fileSystem != null) {
            JsonObject jsonFileSystem = fileSystem.toJsonObject();
            jsonObject = jsonObject.put("file_system", jsonFileSystem);
        }

        JsonArray jsonListeners = new JsonArray();
        for (HostAndPort publishAddress : publishAddresses) {
            jsonListeners.add(publishAddress.toString());
        }
        jsonObject.put("publish_addresses", jsonListeners);

        JsonArray jsonVolumes = new JsonArray();
        for (XVolume<? extends XVolume> xVolume : volumes) {
            jsonVolumes.add(xVolume.toJsonObject());
        }
        jsonObject.put("volumes", jsonVolumes);
        return jsonObject;
    }

}
