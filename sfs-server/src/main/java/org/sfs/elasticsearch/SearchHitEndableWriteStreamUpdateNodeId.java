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

package org.sfs.elasticsearch;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.Holder1;
import rx.Observable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static org.sfs.rx.Defer.aVoid;

public class SearchHitEndableWriteStreamUpdateNodeId extends AbstractBulkUpdateEndableWriteStream {

    private static final Logger LOGGER = getLogger(SearchHitEndableWriteStreamUpdateNodeId.class);
    private final Map<String, Holder1<Long>> documentCountByNode;
    private long count = -1;
    private Map.Entry<String, Holder1<Long>> smallestEntry;
    private boolean isDebugEnabled = LOGGER.isDebugEnabled();

    public SearchHitEndableWriteStreamUpdateNodeId(VertxContext<Server> vertxContext, Map<String, Long> documentCountByNode) {
        super(vertxContext);
        this.documentCountByNode = new HashMap<>(documentCountByNode.size());
        for (Map.Entry<String, Long> entry : documentCountByNode.entrySet()) {
            this.documentCountByNode.put(entry.getKey(), new Holder1<>(entry.getValue()));
        }
    }

    @Override
    protected Observable<com.google.common.base.Optional<JsonObject>> transform(JsonObject data, String id, long version) {
        return aVoid()
                .map(aVoid -> {
                    Map.Entry<String, Holder1<Long>> entry = selectSmallest();
                    incrementCount(entry);
                    data.put("node_id", entry.getKey());
                    if (isDebugEnabled) {
                        LOGGER.debug("Assigning {" + id + "} to node " + entry.getKey());
                    }
                    return of(data);
                });
    }

    protected Map.Entry<String, Holder1<Long>> selectSmallest() {
        if (count < 0 || count > 1000) {
            Optional<Map.Entry<String, Holder1<Long>>> min =
                    documentCountByNode.entrySet()
                            .stream()
                            .min((o1, o2) -> o1.getValue().value().compareTo(o2.getValue().value()));
            smallestEntry = min.get();
            count = 0;
        } else {
            count++;
        }
        return smallestEntry;
    }

    protected void incrementCount(Map.Entry<String, Holder1<Long>> entry) {
        entry.getValue().value++;
    }
}
