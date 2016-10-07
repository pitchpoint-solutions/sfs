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

package org.sfs.elasticsearch.object;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentObject;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Preconditions.checkState;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.sfs.vo.PersistentObject.fromSearchHit;
import static rx.Observable.from;

public class PrefixSearch implements Func1<String, Observable<PersistentObject>> {

    private static final Logger LOGGER = getLogger(PrefixSearch.class);
    private final VertxContext<Server> vertxContext;
    private final PersistentContainer persistentContainer;

    public PrefixSearch(VertxContext<Server> vertxContext, PersistentContainer persistentContainer) {
        this.vertxContext = vertxContext;
        this.persistentContainer = persistentContainer;
    }

    @Override
    public Observable<PersistentObject> call(String prefix) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        PrefixQueryBuilder query = prefixQuery("_id", prefix);

        String objectIndex = elasticSearch.objectIndex(persistentContainer.getName());


        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request {%s,%s} = %s", elasticSearch.defaultType(), objectIndex, Jsonify.toString(query)));
        }

        final int MAX_RESULTS = 100;

        SearchRequestBuilder request =
                elasticSearch.get()
                        .prepareSearch(objectIndex)
                        .setTypes(elasticSearch.defaultType())
                        .setSize(MAX_RESULTS + 1)
                        .setQuery(query)
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultSearchTimeout() - 10));

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultSearchTimeout())
                .flatMap(new Func1<Optional<SearchResponse>, Observable<PersistentObject>>() {
                    @Override
                    public Observable<PersistentObject> call(Optional<SearchResponse> oSearchResponse) {
                        SearchResponse searchResponse = oSearchResponse.get();
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(format("Search Response {%s,%s} = %s", elasticSearch.defaultType(), objectIndex, Jsonify.toString(searchResponse)));
                        }
                        checkState(searchResponse.getHits().totalHits() <= MAX_RESULTS, "Max search results is %s", MAX_RESULTS);
                        return from(searchResponse.getHits())
                                .map(searchHit -> searchHit)
                                .map(searchHit -> fromSearchHit(persistentContainer, searchHit));

                    }
                });
    }
}