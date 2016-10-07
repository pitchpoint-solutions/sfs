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

package org.sfs.elasticsearch.containerkey;

import com.google.common.base.Optional;
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentContainer;
import org.sfs.vo.PersistentContainerKey;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.of;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.sort.SortOrder.DESC;
import static org.sfs.vo.PersistentContainerKey.fromSearchHit;

public class GetNewestContainerKey implements Func1<PersistentContainer, Observable<Optional<PersistentContainerKey>>> {

    private static final Logger LOGGER = getLogger(GetNewestContainerKey.class);
    private final VertxContext<Server> vertxContext;

    public GetNewestContainerKey(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Optional<PersistentContainerKey>> call(final PersistentContainer persistentContainer) {

        String containerName = persistentContainer.getId();

        TermQueryBuilder query = termQuery("container_id", containerName);

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request {%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), Jsonify.toString(query)));
        }

        SearchRequestBuilder request =
                elasticSearch.get()
                        .prepareSearch(elasticSearch.containerKeyIndex())
                        .setTypes(elasticSearch.defaultType())
                        .addSort("create_ts", DESC)
                        .setQuery(query)
                        .setVersion(true)
                        .setSize(1)
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultSearchTimeout() - 10));

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultSearchTimeout())
                .map(oSearchResponse -> {
                    SearchResponse searchResponse = oSearchResponse.get();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Search Response {%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.containerKeyIndex(), Jsonify.toString(searchResponse)));
                    }
                    for (SearchHit searchHit : searchResponse.getHits()) {
                        if (!searchHit.isSourceEmpty()) {
                            return of(fromSearchHit(persistentContainer, searchHit));
                        }
                    }
                    return Optional.<PersistentContainerKey>absent();
                });

    }

}
