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
import io.vertx.core.logging.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.elasticsearch.Elasticsearch;
import org.sfs.elasticsearch.Jsonify;
import org.sfs.encryption.AlgorithmDef;
import org.sfs.vo.PersistentMasterKey;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.base.Optional.absent;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.sort.SortOrder.DESC;
import static rx.Observable.from;

public class GetNewestMasterKey implements Func1<Void, Observable<Optional<PersistentMasterKey>>> {

    private static final Logger LOGGER = getLogger(ListReEncryptableMasterKeys.class);
    private final VertxContext<Server> vertxContext;
    private final AlgorithmDef algorithmDef;

    public GetNewestMasterKey(VertxContext<Server> vertxContext, AlgorithmDef algorithmDef) {
        this.vertxContext = vertxContext;
        this.algorithmDef = algorithmDef;
    }

    @Override
    public Observable<Optional<PersistentMasterKey>> call(Void aVoid) {

        final Elasticsearch elasticSearch = vertxContext.verticle().elasticsearch();


        TermQueryBuilder query = termQuery("algorithm_name", algorithmDef.getAlgorithmName());

        SearchRequestBuilder request =
                elasticSearch.get()
                        .prepareSearch(
                                elasticSearch.masterKeyTypeIndex())
                        .setVersion(true)
                        .setTypes(elasticSearch.defaultType())
                        .addSort("create_ts", DESC)
                        .setQuery(query)
                        .setSize(1)
                        .setTimeout(timeValueMillis(elasticSearch.getDefaultSearchTimeout() - 10));


        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Search Request {%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.masterKeyTypeIndex(), Jsonify.toString(request)));
        }

        return elasticSearch.execute(vertxContext, request, elasticSearch.getDefaultSearchTimeout())
                .flatMap(oSearchResponse -> {
                    SearchResponse searchResponse = oSearchResponse.get();
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(format("Search Response {%s,%s} = %s", elasticSearch.defaultType(), elasticSearch.masterKeyTypeIndex(), Jsonify.toString(searchResponse)));
                    }
                    if (oSearchResponse.isPresent()) {
                        return from(oSearchResponse.get().getHits());
                    } else {
                        return from(emptyList());
                    }
                })
                .map(PersistentMasterKey::fromSearchHit)
                .map(Optional::of)
                .singleOrDefault(absent());
    }
}

