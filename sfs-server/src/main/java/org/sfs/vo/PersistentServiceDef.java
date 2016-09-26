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

public class PersistentServiceDef extends ServiceDef<PersistentServiceDef> {

    private final long version;

    public PersistentServiceDef(String id, long version) {
        super(id);
        this.version = version;
    }

    @Override
    public PersistentServiceDef copy() {
        return new PersistentServiceDef(getId(), version)
                .copyInternal(this);
    }

    public long getVersion() {
        return version;
    }

    public static PersistentServiceDef fromIndexResponse(IndexResponse indexResponse, JsonObject source) {
        return new PersistentServiceDef(indexResponse.getId(), indexResponse.getVersion())
                .merge(source);
    }

    public static PersistentServiceDef fromGetResponse(GetResponse getResponse) {
        long persistentVersion = getResponse.getVersion();
        JsonObject document = new JsonObject(getResponse.getSourceAsString());

        return new PersistentServiceDef(getResponse.getId(), persistentVersion)
                .merge(document);
    }

    public static PersistentServiceDef fromSearchHit(SearchHit searchHit) {
        long persistentVersion = searchHit.getVersion();
        JsonObject document = new JsonObject(searchHit.getSourceAsString());

        return new PersistentServiceDef(searchHit.getId(), persistentVersion)
                .merge(document);
    }
}
