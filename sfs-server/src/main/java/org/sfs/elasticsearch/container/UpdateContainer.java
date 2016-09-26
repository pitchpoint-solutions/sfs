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

package org.sfs.elasticsearch.container;

import com.google.common.base.Optional;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentContainer;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.sfs.vo.PersistentContainer.fromIndexResponse;

public class UpdateContainer implements Func1<PersistentContainer, Observable<Optional<PersistentContainer>>> {

    private static final Logger LOGGER = getLogger(UpdateContainer.class);
    private final VertxContext<Server> vertxContext;

    public UpdateContainer(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Optional<PersistentContainer>> call(final PersistentContainer persistentContainer) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        final JsonObject source = persistentContainer.toJsonObject();

        String encoded;

        if (LOGGER.isDebugEnabled()) {
            encoded = source.encodePrettily();
            LOGGER.debug(format("Index Request {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), persistentContainer.getId(), persistentContainer.getPersistentVersion(), encoded));
        } else {
            encoded = source.encode();
        }

        IndexRequestBuilder request =
                elasticSearch.get()
                        .prepareIndex(
                                elasticSearch.containerIndex(),
                                elasticSearch.defaultType(),
                                persistentContainer.getId())
                        .setVersion(persistentContainer.getPersistentVersion())
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultIndexTimeout() - 10))
                        .setSource(encoded);

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultIndexTimeout())
                .map(indexResponse -> {
                    if (indexResponse.isPresent()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), persistentContainer.getId(), persistentContainer.getPersistentVersion(), Jsonify.toString(indexResponse.get())));
                        }
                        return of(fromIndexResponse(persistentContainer.getParent(), indexResponse.get(), source));
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), persistentContainer.getId(), persistentContainer.getPersistentVersion(), "null"));
                        }
                        return absent();
                    }
                });
    }
}