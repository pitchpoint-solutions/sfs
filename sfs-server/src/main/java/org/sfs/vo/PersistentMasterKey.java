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

import io.vertx.core.json.JsonObject;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.search.SearchHit;

public class PersistentMasterKey extends MasterKey<PersistentMasterKey> {

    private final long persistentVersion;

    public PersistentMasterKey(String id, long persistentVersion) {
        super(id);
        this.persistentVersion = persistentVersion;
    }

    public static PersistentMasterKey fromSearchHit(JsonObject searchHit) {
        return
                new PersistentMasterKey(searchHit.getString("_id"), searchHit.getLong("_version"))
                        .merge(searchHit.getJsonObject("_source"));
    }

    public static PersistentMasterKey fromSearchHit(SearchHit searchHit) {
        return
                new PersistentMasterKey(searchHit.getId(), searchHit.getVersion())
                        .merge(new JsonObject(searchHit.getSourceAsString()));
    }

    public static PersistentMasterKey fromIndexResponse(IndexResponse indexResponse, JsonObject source) {
        return
                new PersistentMasterKey(indexResponse.getId(), indexResponse.getVersion())
                        .merge(source);
    }

    public static PersistentMasterKey fromGetResponse(GetResponse getResponse) {
        return
                new PersistentMasterKey(getResponse.getId(), getResponse.getVersion())
                        .merge(new JsonObject(getResponse.getSourceAsString()));
    }

    public long getPersistentVersion() {
        return persistentVersion;
    }
}
