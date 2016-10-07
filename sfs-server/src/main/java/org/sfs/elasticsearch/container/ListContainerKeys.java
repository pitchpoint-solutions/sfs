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

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentContainerKey;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.sfs.vo.PersistentContainerKey.fromSearchHit;
import static rx.Observable.empty;
import static rx.Observable.from;

public class ListContainerKeys implements Func1<PersistentContainer, Observable<PersistentContainerKey>> {

    private static final Logger LOGGER = getLogger(UpdateContainer.class);
    private final VertxContext<Server> vertxContext;

    public ListContainerKeys(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<PersistentContainerKey> call(PersistentContainer persistentContainer) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        final JsonObject source = persistentContainer.toJsonObject();

        String encoded;

        if (LOGGER.isDebugEnabled()) {
            encoded = source.encodePrettily();
            LOGGER.debug(format("Search Request {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), persistentContainer.getId(), persistentContainer.getPersistentVersion(), encoded));
        } else {
            encoded = source.encode();
        }

        SearchRequestBuilder request =
                elasticSearch.get()
                        .prepareSearch(elasticSearch.containerKeyIndex())
                        .setTypes(elasticSearch.defaultType())
                        .setVersion(true)
                        .setQuery(termQuery("container_id", persistentContainer.getId()))
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultIndexTimeout() - 10))
                        .setSource(encoded);

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultIndexTimeout())
                .flatMap(oSearchResponse -> {
                    if (oSearchResponse.isPresent()) {
                        SearchResponse searchResponse = oSearchResponse.get();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Search Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), persistentContainer.getId(), persistentContainer.getPersistentVersion(), Jsonify.toString(searchResponse)));
                        }
                        return from(searchResponse.getHits());
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Index Response {%s,%s,%s,%d} = %s", elasticSearch.defaultType(), elasticSearch.containerIndex(), persistentContainer.getId(), persistentContainer.getPersistentVersion(), "null"));
                        }
                        return empty();
                    }
                })
                .map(searchHitFields -> fromSearchHit(persistentContainer, searchHitFields));
    }
}