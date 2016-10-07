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
import static java.util.Calendar.getInstance;
import static org.sfs.metadata.Metadata.account;
import static org.sfs.util.DateFormatter.fromDateTimeString;
import static org.sfs.util.DateFormatter.toDateTimeString;
import static org.sfs.util.KnownMetadataKeys.X_TEMP_URL_KEY;
import static org.sfs.util.KnownMetadataKeys.X_TEMP_URL_KEY_2;

public abstract class Account<T extends Account> {

    private final String id;
    private String nodeId;
    private Metadata metadata = account();
    private Calendar createTs;
    private Calendar updateTs;

    public Account(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @SuppressWarnings("unused")
    public Optional<String> getNodeId() {
        return fromNullable(nodeId);
    }

    public T setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return (T) this;
    }

    public Metadata getMetadata() {
        return metadata;
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
        return keys;
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

    public T merge(JsonObject document) {

        JsonArray metadataJsonObject = document.getJsonArray("metadata", new JsonArray());
        metadata.withJsonObject(metadataJsonObject);

        setNodeId(document.getString("node_id"));

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


    public T merge(SfsRequest httpServerRequest) {
        MultiMap headers = httpServerRequest.headers();

        metadata.withHttpHeaders(headers);

        return (T) this;
    }

    public JsonObject toJsonObject() {
        JsonObject document = new JsonObject();

        document.put("metadata", metadata.toJsonObject());

        document.put("node_id", nodeId);

        document.put("create_ts", toDateTimeString(getCreateTs()));

        document.put("update_ts", toDateTimeString(getUpdateTs()));

        return document;
    }
}
