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

public class PersistentContainer extends Container<PersistentContainer> {

    private final long persistentVersion;

    public static PersistentContainer fromIndexResponse(PersistentAccount persistentAccount, IndexResponse indexResponse, JsonObject source) {
        return
                new PersistentContainer(persistentAccount, indexResponse.getId(), indexResponse.getVersion())
                        .merge(source);

    }

    public static PersistentContainer fromGetResponse(PersistentAccount persistentAccount, GetResponse getResponse) {
        JsonObject document = new JsonObject(getResponse.getSourceAsString());

        return
                new PersistentContainer(persistentAccount, getResponse.getId(), getResponse.getVersion())
                        .merge(document);

    }

    public static PersistentContainer fromSearchHit(PersistentAccount persistentAccount, SearchHit searchHit) {
        JsonObject document = new JsonObject(searchHit.getSourceAsString());

        return
                new PersistentContainer(persistentAccount, searchHit.getId(), searchHit.getVersion())
                        .merge(document);

    }

    public PersistentContainer(PersistentAccount persistentAccount, String id, long persistentVersion) {
        super(persistentAccount, id);
        this.persistentVersion = persistentVersion;
    }

    public long getPersistentVersion() {
        return persistentVersion;
    }
}
