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

package org.sfs.elasticsearch.nodes;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentServiceDef;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.fromNullable;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.sfs.vo.PersistentServiceDef.fromIndexResponse;

public class UpdateServiceDef implements Func1<PersistentServiceDef, Observable<Optional<PersistentServiceDef>>> {

    private static final Logger LOGGER = getLogger(UpdateServiceDef.class);
    private final VertxContext<Server> vertxContext;
    private final long ttl;

    public UpdateServiceDef(VertxContext<Server> vertxContext, long ttl) {
        this.vertxContext = vertxContext;
        this.ttl = ttl;
    }

    @Override
    public Observable<Optional<PersistentServiceDef>> call(final PersistentServiceDef persistentServiceDef) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        final String id = persistentServiceDef.getId();

        final JsonObject source = persistentServiceDef.toJsonObject();

        String encoded;

        if (LOGGER.isDebugEnabled()) {
            encoded = source.encodePrettily();
            LOGGER.debug(format("Request {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.serviceDefTypeIndex(), id, encoded));
        } else {
            encoded = source.encode();
        }


        IndexRequestBuilder request =
                elasticSearch.get()
                        .prepareIndex(
                                elasticSearch.serviceDefTypeIndex(),
                                elasticSearch.defaultType(),
                                id)
                        .setSource(encoded)
                        .setTTL(ttl)
                        .setVersion(persistentServiceDef.getVersion())
                        .setCreate(false)
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultIndexTimeout() - 10));

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultIndexTimeout())
                .map(new Func1<Optional<IndexResponse>, Optional<PersistentServiceDef>>() {
                    @Override
                    public Optional<PersistentServiceDef> call(Optional<IndexResponse> oIndexResponse) {
                        if (oIndexResponse.isPresent()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Response {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.serviceDefTypeIndex(), id, Jsonify.toString(oIndexResponse.get())));
                            }
                            return fromNullable(fromIndexResponse(oIndexResponse.get(), source));
                        } else {
                            LOGGER.debug(format("Response {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.serviceDefTypeIndex(), id, "null"));
                            return absent();
                        }
                    }
                });
    }
}