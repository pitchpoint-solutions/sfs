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

import io.vertx.core.json.JsonObject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.search.SearchHit;

public class PersistentObject extends XObject<PersistentObject> {

    private final long persistentVersion;

    public static PersistentObject fromIndexResponse(PersistentContainer persistentContainer, IndexResponse indexResponse, JsonObject source) {
        return
                new PersistentObject(persistentContainer, indexResponse.getId(), indexResponse.getVersion())
                        .merge(source);

    }

    public static PersistentObject fromGetResponse(PersistentContainer container, GetResponse getResponse) {

        String id = getResponse.getId();
        long persistentVersion = getResponse.getVersion();
        JsonObject document = new JsonObject(getResponse.getSourceAsString());

        return new PersistentObject(container, id, persistentVersion)
                .merge(document);

    }

    public static PersistentObject fromSearchHit(PersistentContainer container, SearchHit searchHit) {
        JsonObject document = new JsonObject(searchHit.getSourceAsString());
        return new PersistentObject(container, searchHit.getId(), searchHit.getVersion())
                .merge(document);
    }

    public static PersistentObject fromSearchHit(PersistentContainer container, JsonObject searchHit) {
        JsonObject document = searchHit.getJsonObject("_source");
        return new PersistentObject(container, searchHit.getString("_id"), searchHit.getLong("_version"))
                .merge(document);
    }

    public PersistentObject(PersistentContainer persistentContainer, String id, long persistentVersion) {
        super(persistentContainer, id);
        this.persistentVersion = persistentVersion;
    }

    public long getPersistentVersion() {
        return persistentVersion;
    }

}
