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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.sfs.util.IdentityComparator;

import java.util.Calendar;
import java.util.NavigableSet;
import java.util.TreeSet;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Optional.of;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Ordering.from;
import static com.google.common.math.LongMath.checkedAdd;
import static java.util.Calendar.getInstance;
import static org.sfs.util.DateFormatter.fromDateTimeString;
import static org.sfs.util.DateFormatter.toDateTimeString;

public abstract class XObject<T extends XObject> {

    private final PersistentContainer parent;
    private final String id;
    private Calendar createTs;
    private Calendar updateTs;
    private String ownerGuid;
    // id of data node that scans and manages this object. This value
    // allows the work of managing objects to be split over the nodes
    // without using a consistent hash ring for object assignment
    private String nodeId;
    // largest id numbers should be at the beginning of the collection
    private NavigableSet<TransientVersion> versions = new TreeSet<>(from(new IdentityComparator()));

    public XObject(PersistentContainer parent, String id) {
        this.parent = parent;
        this.id = id;
    }

    public Calendar getCreateTs() {
        if (createTs == null) createTs = getInstance();
        return createTs;
    }

    public T setCreateTs(Calendar createTs) {
        this.createTs = createTs;
        return (T) this;
    }

    public Calendar getUpdateTs() {
        if (updateTs == null) updateTs = getInstance();
        return updateTs;
    }

    public T setUpdateTs(Calendar updateTs) {
        this.updateTs = updateTs;
        return (T) this;
    }

    public Optional<String> getOwnerGuid() {
        return fromNullable(ownerGuid);
    }

    public T setOwnerGuid(String ownerGuid) {
        this.ownerGuid = ownerGuid;
        return (T) this;
    }

    public Optional<String> getNodeId() {
        return fromNullable(nodeId);
    }


    public T setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return (T) this;
    }

    public PersistentContainer getParent() {
        return parent;
    }

    public String getId() {
        return id;
    }

    public Optional<TransientVersion> getNewestVersion() {
        if (!versions.isEmpty()) {
            return of(versions.last());
        } else {
            return absent();
        }
    }

    public Optional<TransientVersion> getOldestVersion() {
        if (!versions.isEmpty()) {
            return of(versions.first());
        } else {
            return absent();
        }
    }

    public Optional<TransientVersion> getVersion(long id) {
        for (TransientVersion version : versions) {
            if (id == version.getId()) {
                return of(version);
            }
        }
        return absent();
    }

    public NavigableSet<TransientVersion> getVersions() {
        return versions;
    }

    public T setVersions(Iterable<TransientVersion> versions) {
        this.versions.clear();
        addAll(this.versions, versions);
        return (T) this;
    }

    public TransientVersion newVersion() {
        if (this.versions.isEmpty()) {
            TransientVersion newVersion = new TransientVersion(this, 0L);
            this.versions.add(newVersion);
            return newVersion;
        } else {
            TransientVersion existingNewest = getNewestVersion().get();
            TransientVersion newVersion =
                    new TransientVersion(
                            this,
                            checkedAdd(existingNewest.getId(), 1));
            newVersion = newVersion.setCreateTs(existingNewest.getCreateTs());
            this.versions.add(newVersion);
            return newVersion;
        }
    }

    public T merge(JsonObject document) {
        this.versions.clear();
        this.ownerGuid = document.getString("owner_guid");
        this.nodeId = document.getString("node_id");
        JsonArray jsonVerionArray = document.getJsonArray("versions");
        if (jsonVerionArray != null) {
            for (Object o : jsonVerionArray) {
                JsonObject versionDocument = (JsonObject) o;
                Long id = versionDocument.getLong("id");
                if (id == null) {
                    checkNotNull(id, "id is null for %s", document.encodePrettily());
                }
                TransientVersion transientVersion =
                        new TransientVersion(this, id)
                                .merge(versionDocument);
                versions.add(transientVersion);
            }
        }

        String createTimestamp = document.getString("create_ts");
        String updateTimestamp = document.getString("update_ts");

        if (createTimestamp != null) {
            setCreateTs(fromDateTimeString(createTimestamp));
        }
        if (updateTimestamp != null) {
            setUpdateTs(fromDateTimeString(updateTimestamp));
        }
        return (T) this;
    }

    public JsonObject toJsonObject() {
        JsonObject document = new JsonObject();

        checkState(parent != null, "container cannot be null");
        checkState(parent.getParent() != null, "account cannot be null");

        document.put("account_id", parent.getParent().getId());
        document.put("container_id", parent.getId());
        document.put("node_id", nodeId);
        document.put("owner_guid", ownerGuid);

        JsonArray versionsJsonArray = new JsonArray();
        for (TransientVersion transientVersion : versions) {
            versionsJsonArray.add(transientVersion.toJsonObject());
        }
        document.put("versions", versionsJsonArray);
        document.put("version_count", versions.size());
        Optional<TransientVersion> oOldestVersion = getOldestVersion();
        if (oOldestVersion.isPresent()) {
            document.put("oldest_version_ts", toDateTimeString(oOldestVersion.get().getCreateTs()));
        } else {
            document.put("oldest_version_ts", (String) null);
        }

        document.put("create_ts", toDateTimeString(getCreateTs()));
        document.put("update_ts", toDateTimeString(getUpdateTs()));

        return document;
    }
}
