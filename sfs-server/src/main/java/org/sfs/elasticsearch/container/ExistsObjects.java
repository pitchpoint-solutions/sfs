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

import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.vo.PersistentContainer;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class ExistsObjects implements Func1<PersistentContainer, Observable<Boolean>> {

    private static final Logger LOGGER = getLogger(ExistsObjects.class);
    private final VertxContext<Server> vertxContext;

    public ExistsObjects(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Boolean> call(PersistentContainer persistentContainer) {
        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();

        String objectIndex = elasticSearch.objectIndex(persistentContainer.getName());

        SearchRequestBuilder request = elasticSearch.get()
                .prepareSearch(objectIndex)
                .setTypes(elasticSearch.defaultType())
                .setQuery(termQuery("container_id", persistentContainer.getId()))
                .setSize(0)
                .setTerminateAfter(1);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request {%s,%s,%s}", elasticSearch.defaultType(), objectIndex, persistentContainer.getId()));
        }


        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultGetTimeout())
                .map(oSearchResponse -> {
                    SearchResponse searchResponse = oSearchResponse.get();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Search Response {%s,%s,%s} = %s", elasticSearch.defaultType(), objectIndex, persistentContainer.getId(), Jsonify.toString(searchResponse)));
                    }
                    return searchResponse.getHits().getTotalHits() > 0;
                });
    }

}
