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

package org.sfs.elasticsearch.masterkey;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentMasterKey;
import org.sfs.vo.TransientMasterKey;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.sfs.vo.PersistentMasterKey.fromIndexResponse;

public class PersistMasterKey implements Func1<TransientMasterKey, Observable<Optional<PersistentMasterKey>>> {

    private static final Logger LOGGER = getLogger(PersistMasterKey.class);
    private final VertxContext<Server> vertxContext;

    public PersistMasterKey(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Optional<PersistentMasterKey>> call(TransientMasterKey transientMasterKey) {
        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        final JsonObject document = transientMasterKey.toJsonObject();

        String encoded = document.encode();


        IndexRequestBuilder request = elasticSearch.get()
                .prepareIndex(
                        elasticSearch.masterKeyTypeIndex(),
                        elasticSearch.defaultType(),
                        transientMasterKey.getId())
                .setCreate(true) // put only if absent
                .setTimeout(timeValueMillis(elasticSearch.getDefaultIndexTimeout() - 10))
                .setSource(encoded);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Index Request {%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.masterKeyTypeIndex(), Jsonify.toString(request)));
        }

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultIndexTimeout())
                .map(indexResponse -> {
                    if (indexResponse.isPresent()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.masterKeyTypeIndex(), transientMasterKey.getId(), Jsonify.toString(indexResponse.get())));
                        }
                        return of(fromIndexResponse(indexResponse.get(), document));
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.masterKeyTypeIndex(), transientMasterKey.getId(), "null"));
                        }
                        return absent();
                    }
                });
    }
}
