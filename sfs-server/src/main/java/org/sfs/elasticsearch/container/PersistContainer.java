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
import org.sfs.vo.TransientContainer;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.sfs.vo.PersistentContainer.fromIndexResponse;

public class PersistContainer implements Func1<TransientContainer, Observable<Optional<PersistentContainer>>> {

    private static final Logger LOGGER = getLogger(PersistContainer.class);
    private final VertxContext<Server> vertxContext;

    public PersistContainer(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Optional<PersistentContainer>> call(final TransientContainer transientContainer) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        final JsonObject source = transientContainer.toJsonObject();

        String encoded;

        if (LOGGER.isDebugEnabled()) {
            encoded = source.encodePrettily();
            LOGGER.debug(format("Index Request {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), transientContainer.getId(), encoded));
        } else {
            encoded = source.encode();
        }


        IndexRequestBuilder request =
                elasticSearch.get()
                        .prepareIndex(
                                elasticSearch.containerIndex(),
                                elasticSearch.defaultType(),
                                transientContainer.getId())
                        .setCreate(true)
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultIndexTimeout() - 10))
                        .setSource(encoded);

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultIndexTimeout())
                .map(indexResponse -> {
                    if (indexResponse.isPresent()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), transientContainer.getId(), Jsonify.toString(indexResponse.get())));
                        }
                        return of(fromIndexResponse(transientContainer.getParent(), indexResponse.get(), source));
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), transientContainer.getId(), "null"));
                        }
                        return absent();
                    }
                });
    }
}