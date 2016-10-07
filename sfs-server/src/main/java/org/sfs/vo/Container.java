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
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.sfs.SfsRequest;
import org.sfs.metadata.Metadata;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.SortedSet;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.primitives.Ints.tryParse;
import static java.lang.Integer.parseInt;
import static java.util.Calendar.getInstance;
import static org.sfs.metadata.Metadata.container;
import static org.sfs.util.DateFormatter.fromDateTimeString;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static org.sfs.util.KnownMetadataKeys.X_MAX_OBJECT_REVISIONS;
import static org.sfs.util.KnownMetadataKeys.X_SERVER_SIDE_ENCRYPTION;
import static org.sfs.util.KnownMetadataKeys.X_TEMP_URL_KEY;
import static org.sfs.util.KnownMetadataKeys.X_TEMP_URL_KEY_2;
import static org.sfs.util.Limits.MAX_OBJECT_REVISIONS;
import static org.sfs.util.Limits.NOT_SET;
import static org.sfs.util.NullSafeAscii.equalsIgnoreCase;
import static org.sfs.util.SfsHttpHeaders.X_SFS_OBJECT_REPLICAS;
import static org.sfs.vo.ObjectPath.fromPaths;

public abstract class Container<T extends Container> {

    private final PersistentAccount parent;
    private final String id;
    private final String name;
    private String nodeId;
    private Metadata metadata = container();
    private String ownerGuid;
    private Calendar createTs;
    private Calendar updateTs;
    private Integer objectReplicas;

    public Container(PersistentAccount parent, String id) {
        this.parent = parent;
        this.id = id;
        if (this.id != null) {
            this.name = fromPaths(id).containerName().get();
        } else {
            this.name = null;
        }
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public Optional<String> getNodeId() {
        return fromNullable(nodeId);
    }

    public T setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return (T) this;
    }

    public PersistentAccount getParent() {
        return parent;
    }

    public T setOwnerGuid(String ownerGuid) {
        this.ownerGuid = ownerGuid;
        return (T) this;
    }

    public List<String> getTempUrlKeys() {
        List<String> keys = new ArrayList<>(2);
        SortedSet<String> values = metadata.get(X_TEMP_URL_KEY_2);
        if (!values.isEmpty()) {
            keys.add(values.first());
        }
        values = metadata.get(X_TEMP_URL_KEY);
        if (!values.isEmpty()) {
            keys.add(values.first());
        }
        if (!keys.isEmpty()) {
            return keys;
        } else {
            return getParent().getTempUrlKeys();
        }
    }

    public int getMaxObjectRevisions() {
        SortedSet<String> value = metadata.get(X_MAX_OBJECT_REVISIONS);
        if (!value.isEmpty()) {
            Integer maxRevisions = tryParse(value.first());
            if (maxRevisions != null) {
                return maxRevisions;
            }
        }
        return MAX_OBJECT_REVISIONS;
    }

    public Optional<String> getOwnerGuid() {
        return fromNullable(ownerGuid);
    }

    public Metadata getMetadata() {
        return metadata;
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

    public boolean getServerSideEncryption() {
        SortedSet<String> value = metadata.get(X_SERVER_SIDE_ENCRYPTION);
        if (!value.isEmpty()) {
            return equalsIgnoreCase("true", value.first());
        }
        return false;
    }

    public int getObjectReplicas() {
        return objectReplicas != null ? NOT_SET : objectReplicas;
    }

    public T setObjectReplicas(int objectReplicas) {
        this.objectReplicas = objectReplicas;
        return (T) this;
    }

    public T merge(JsonObject document) {

        checkState(getParent().getId().equals(document.getString("account_id")));

        setOwnerGuid(document.getString("owner_guid"));

        JsonArray metadataJsonObject = document.getJsonArray("metadata", new JsonArray());
        getMetadata().withJsonObject(metadataJsonObject);

        setNodeId(document.getString("node_id"));

        String createTimestamp = document.getString("create_ts");
        String updateTimestamp = document.getString("update_ts");

        if (createTimestamp != null) {
            setCreateTs(fromDateTimeString(createTimestamp));
        }
        if (updateTimestamp != null) {
            setUpdateTs(fromDateTimeString(updateTimestamp));
        }

        setObjectReplicas(document.containsKey("object_replicas") ? document.getInteger("object_replicas") : NOT_SET);

        return (T) this;
    }


    public T merge(SfsRequest httpServerRequest) {
        MultiMap headers = httpServerRequest.headers();

        if (headers.contains(X_SFS_OBJECT_REPLICAS)) {
            setObjectReplicas(parseInt(headers.get(X_SFS_OBJECT_REPLICAS)));
        } else {
            setObjectReplicas(NOT_SET);
        }

        getMetadata().withHttpHeaders(headers);

        return (T) this;
    }

    public JsonObject toJsonObject() {
        JsonObject document = new JsonObject();

        document.put("owner_guid", ownerGuid);

        document.put("account_id", parent.getId());

        document.put("metadata", metadata.toJsonObject());

        document.put("node_id", nodeId);

        document.put("create_ts", toDateTimeString(getCreateTs()));

        document.put("update_ts", toDateTimeString(getUpdateTs()));

        document.put("object_replicas", objectReplicas);

        return document;
    }
}