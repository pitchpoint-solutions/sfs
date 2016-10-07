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

import static org.sfs.vo.ObjectPath.fromPaths;

public class PersistentContainerKey extends ContainerKey<PersistentContainerKey> {

    private final long persistentVersion;

    public static PersistentContainerKey fromSearchHit(PersistentContainer persistentContainer, SearchHit searchHit) {
        return
                new PersistentContainerKey(persistentContainer, searchHit.getId(), searchHit.getVersion())
                        .merge(new JsonObject(searchHit.getSourceAsString()));
    }

    public static PersistentContainerKey fromIndexResponse(PersistentContainer persistentContainer, IndexResponse indexResponse, JsonObject source) {
        return
                new PersistentContainerKey(persistentContainer, indexResponse.getId(), indexResponse.getVersion())
                        .merge(source);
    }

    public static PersistentContainerKey fromGetResponse(PersistentContainer container, GetResponse getResponse) {
        return
                new PersistentContainerKey(container, getResponse.getId(), getResponse.getVersion())
                        .merge(new JsonObject(getResponse.getSourceAsString()));
    }

    public PersistentContainerKey(PersistentContainer persistentContainer, String id, long persistentVersion) {
        super(persistentContainer, fromPaths(id));
        this.persistentVersion = persistentVersion;
    }

    public PersistentContainerKey(String id, long persistentVersion) {
        super(id);
        this.persistentVersion = persistentVersion;
    }

    public long getPersistentVersion() {
        return persistentVersion;
    }
}
