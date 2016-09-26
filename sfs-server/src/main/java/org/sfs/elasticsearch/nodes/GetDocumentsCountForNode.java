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
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.elasticsearch.ListSfsStorageIndexes;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.sfs.rx.Defer.empty;

public class GetDocumentsCountForNode implements Func1<Void, Observable<Long>> {

    private static final Logger LOGGER = getLogger(GetDocumentsCountForNode.class);
    private final VertxContext<Server> vertxContext;
    private final String nodeId;

    public GetDocumentsCountForNode(VertxContext<Server> vertxContext, String nodeId) {
        this.nodeId = nodeId;
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Long> call(Void aVoid) {
        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();


        return empty()
                .flatMap(new ListSfsStorageIndexes(vertxContext))
                .flatMap(index -> {
                    SearchRequestBuilder request = elasticSearch.get()
                            .prepareSearch(index)
                            .setTypes(elasticSearch.defaultType())
                            .setSize(0)
                            .setQuery(termQuery("node_id", nodeId));

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Request {%s,%s}", elasticSearch.defaultType(), Jsonify.toString(request)));
                    }

                    return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultSearchTimeout())
                            .map(Optional::get)
                            .map(countResponse -> {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug(format("Response {%s} = %s", elasticSearch.defaultType(), Jsonify.toString(countResponse)));
                                }
                                return countResponse;
                            })
                            .map(searchResponse -> searchResponse.getHits().getTotalHits());
                })
                .reduce(0L, (r, newHits) -> r + newHits);


    }
}
